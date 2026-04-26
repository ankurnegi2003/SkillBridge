import io
import json
import os
import random
import re
import sqlite3
import threading
import time
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone

import google.generativeai as genai
from fastapi import BackgroundTasks, FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from config import (
    ALLOWED_ORIGINS,
    GEMINI_API_KEY,
    GEMINI_MODEL,
    MAX_SKILLS_FOR_QUESTION_GENERATION,
    PRACTICE_TEST_GEMINI_API_KEY,
    QUESTION_GEMINI_API_KEY,
)
from database import get_connection, init_db

try:
    from pypdf import PdfReader
except ImportError:  # Optional until dependency is installed
    PdfReader = None


MAX_JOB_DESCRIPTION_CHARS = 20000
MAX_RESUME_TEXT_CHARS = 30000
MAX_FINAL_RESUME_CHARS = 40000


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    print("✅ Database initialized")
    yield


app = FastAPI(title="Skill Assessment Agent API", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)


GENAI_CONFIG_LOCK = threading.Lock()


def _generate_gemini_content(model_name: str, prompt: str, timeout_seconds: int, api_key: str | None = None):
    key_to_use = (api_key or GEMINI_API_KEY).strip()
    if not key_to_use:
        raise RuntimeError("Gemini API key is not configured.")

    default_key = GEMINI_API_KEY.strip()
    with GENAI_CONFIG_LOCK:
        genai.configure(api_key=key_to_use)
        try:
            model = genai.GenerativeModel(model_name)
            return model.generate_content(prompt, request_options={"timeout": timeout_seconds})
        finally:
            if default_key and key_to_use != default_key:
                genai.configure(api_key=default_key)


def _extract_text_from_pdf_bytes(file_bytes: bytes) -> str:
    if PdfReader is None:
        raise HTTPException(
            status_code=500,
            detail="PDF parser dependency missing. Install pypdf in backend requirements.",
        )

    try:
        reader = PdfReader(io.BytesIO(file_bytes))
        text_parts = []
        for page in reader.pages:
            text_parts.append(page.extract_text() or "")
        return "\n".join(text_parts).strip()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not read PDF: {exc}") from exc


IGNORED_SKILL_VALUES = {
    "",
    "-",
    "--",
    ",",
    ".",
    "'",
    '"',
    "''",
    '""',
    "n/a",
    "none",
    "null",
    "na",
}


def _clean_skill_value(value: str) -> str:
    # Normalize whitespace first.
    cleaned = " ".join(str(value).strip().split())
    if not cleaned:
        return ""

    # Remove leading/trailing punctuation often introduced by LLM formatting.
    cleaned = cleaned.strip(" \t\r\n,;:|()[]{}<>`\"'")

    # Keep common technical symbols in skills while removing noisy punctuation.
    cleaned = re.sub(r"[^A-Za-z0-9+.#/&\-\s]", "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    if cleaned.lower() in IGNORED_SKILL_VALUES:
        return ""
    return cleaned


def _normalized_unique(items: list[str]) -> list[str]:
    deduped = []
    seen = set()
    for item in items:
        value = _clean_skill_value(str(item))
        if not value:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(value)
    return deduped


SKILL_ALIASES = {
    "Web Development": [
        "web development",
        "web dev",
        "website development",
        "frontend",
        "front-end",
        "backend",
        "back-end",
        "full stack",
        "fullstack",
    ],
    "Frontend Development": [
        "frontend development",
        "front-end development",
        "frontend",
        "front-end",
    ],
    "Backend Development": [
        "backend development",
        "back-end development",
        "backend",
        "back-end",
        "api development",
        "server-side",
    ],
    "Full Stack Development": [
        "full stack",
        "fullstack",
        "full-stack",
        "frontend and backend",
        "mern",
        "mean",
    ],
    "Mobile Development": [
        "mobile development",
        "android",
        "ios",
        "flutter",
        "react native",
        "swift",
        "kotlin",
        "xamarin",
    ],
    "Software Engineering": [
        "software engineering",
        "software development",
        "software design",
        "clean code",
        "design patterns",
    ],
    "Python": ["python", "py"],
    "Java": ["java"],
    "C": ["c language", "ansi c"],
    "C++": ["c++", "cpp"],
    "C#": ["c#", "csharp", "dotnet", ".net"],
    "Go": ["go", "golang"],
    "Rust": ["rust"],
    "PHP": ["php"],
    "Ruby": ["ruby"],
    "R": ["r language", "r programming"],
    "MATLAB": ["matlab"],
    "FastAPI": ["fastapi"],
    "Django": ["django"],
    "Flask": ["flask"],
    "Spring Boot": ["spring boot", "spring"],
    "Express.js": ["express", "express.js"],
    "Laravel": ["laravel"],
    "Ruby on Rails": ["ruby on rails", "rails"],
    "SQL": ["sql", "postgresql", "mysql", "sql server", "mariadb", "oracle"],
    "NoSQL": ["nosql", "mongodb", "cassandra", "dynamodb", "redis", "couchdb"],
    "SQLite": ["sqlite"],
    "PostgreSQL": ["postgresql", "postgres", "psql"],
    "MySQL": ["mysql"],
    "MongoDB": ["mongodb", "mongo"],
    "React": ["react", "react.js", "reactjs"],
    "Angular": ["angular", "angularjs"],
    "Vue.js": ["vue", "vue.js", "vuejs"],
    "Next.js": ["next", "next.js"],
    "JavaScript": ["javascript", "js"],
    "TypeScript": ["typescript", "ts"],
    "Node.js": ["node", "nodejs", "node.js"],
    "HTML": ["html", "html5"],
    "CSS": ["css", "css3", "sass", "scss"],
    "Git": ["git", "github", "gitlab", "bitbucket", "version control"],
    "REST API": ["rest api", "restful api", "rest"],
    "GraphQL": ["graphql"],
    "gRPC": ["grpc", "protocol buffers", "protobuf"],
    "API Integration": ["api integration", "api integrations", "third-party api", "integrations"],
    "Microservices": ["microservices", "microservice architecture", "service-oriented architecture"],
    "Cloud Computing": ["cloud", "cloud computing", "iaas", "paas", "saas", "serverless"],
    "AWS": ["aws", "amazon web services", "ec2", "s3", "lambda", "rds", "cloudwatch"],
    "GCP": ["gcp", "google cloud", "google cloud platform", "bigquery", "gke"],
    "Azure": ["azure", "microsoft azure", "aks", "azure functions"],
    "Serverless": ["serverless", "faas", "lambda", "cloud functions"],
    "DevOps": [
        "devops",
        "site reliability",
        "sre",
        "ci/cd",
        "continuous integration",
        "continuous delivery",
        "jenkins",
        "github actions",
        "gitlab ci",
    ],
    "Docker": ["docker", "dockerfile", "containers", "containerization"],
    "Kubernetes": ["kubernetes", "k8s", "helm"],
    "Infrastructure as Code": ["infrastructure as code", "iac", "terraform", "cloudformation", "pulumi", "ansible"],
    "CI/CD": ["ci/cd", "continuous integration", "continuous delivery", "jenkins", "github actions", "gitlab ci"],
    "Monitoring & Observability": ["monitoring", "observability", "prometheus", "grafana", "elk", "datadog"],
    "Linux": ["linux", "bash", "shell scripting", "ubuntu", "debian", "centos"],
    "Automation": [
        "automation",
        "workflow automation",
        "process automation",
        "automation engineering",
        "n8n",
        "make",
        "integromat",
        "zapier",
        "power automate",
    ],
    "Scripting": ["scripting", "bash", "shell", "powershell", "python scripting"],
    "Data Science": ["data science", "data analysis", "analytics", "statistical analysis"],
    "Data Engineering": ["data engineering", "etl", "elt", "data pipelines", "airflow", "dbt", "spark", "kafka"],
    "Big Data": ["big data", "hadoop", "spark", "hive", "data lake"],
    "Machine Learning": ["machine learning", "ml", "scikit-learn", "xgboost", "lightgbm"],
    "Deep Learning": ["deep learning", "dl", "tensorflow", "keras", "pytorch"],
    "NLP": ["nlp", "natural language processing", "transformers", "bert", "llm"],
    "Computer Vision": ["computer vision", "opencv", "image processing", "object detection"],
    "Generative AI": ["generative ai", "genai", "llm", "prompt engineering", "rag", "langchain", "vector database"],
    "Pandas": ["pandas"],
    "NumPy": ["numpy"],
    "Cybersecurity": ["cybersecurity", "security", "application security", "network security", "owasp", "penetration testing"],
    "Networking": ["networking", "tcp/ip", "dns", "http", "https", "load balancing"],
    "System Design": ["system design", "architecture", "scalability", "distributed systems"],
    "Distributed Systems": ["distributed systems", "event-driven", "message queue", "pub/sub", "rabbitmq", "kafka"],
    "Message Queues": ["message queue", "queues", "rabbitmq", "kafka", "sqs", "pub/sub"],
    "Testing": ["testing", "unit testing", "integration testing", "e2e testing", "pytest", "jest", "selenium"],
    "QA Automation": ["qa automation", "test automation", "cypress", "playwright", "selenium"],
    "Data Structures": ["data structures", "dsa"],
    "Algorithms": ["algorithms", "algorithm design"],
    "Object-Oriented Programming": ["oop", "object oriented", "object-oriented programming"],
    "Operating Systems": ["operating systems", "os", "concurrency", "multithreading"],
}


# Domain coverage is directional:
# - JD must explicitly mention domain terms (from SKILL_ALIASES)
# - Resume can satisfy that domain through these concrete subskills/tools
DOMAIN_EVIDENCE = {
    "Web Development": [
        "html",
        "css",
        "javascript",
        "typescript",
        "react",
        "angular",
        "vue",
        "next.js",
        "node.js",
        "express",
        "fastapi",
        "django",
        "flask",
    ],
    "Frontend Development": [
        "html",
        "css",
        "javascript",
        "typescript",
        "react",
        "angular",
        "vue",
        "next.js",
        "tailwind",
        "bootstrap",
    ],
    "Backend Development": [
        "fastapi",
        "django",
        "flask",
        "node.js",
        "express",
        "spring boot",
        "laravel",
        "ruby on rails",
        "sql",
    ],
    "Full Stack Development": [
        "frontend development",
        "backend development",
        "react",
        "angular",
        "vue",
        "node.js",
        "express",
        "fastapi",
        "django",
    ],
}

DOMAIN_EVIDENCE_MIN_MATCH = {
    "Web Development": 2,
    "Frontend Development": 2,
    "Backend Development": 1,
    "Full Stack Development": 2,
}


def _compile_alias_pattern(alias: str) -> re.Pattern:
    escaped = re.escape(alias.lower())
    return re.compile(rf"(?<![a-z0-9]){escaped}(?![a-z0-9])")


def _skill_present_in_text(text: str, skill: str) -> bool:
    text_l = text.lower()
    normalized_skill = _clean_skill_value(skill).lower()
    if not normalized_skill:
        return False

    # First, try canonical alias mapping.
    for canonical, aliases in SKILL_ALIASES.items():
        all_aliases = [canonical.lower(), *[a.lower() for a in aliases]]
        if normalized_skill in all_aliases:
            for alias in all_aliases:
                if _compile_alias_pattern(alias).search(text_l):
                    return True
            return False

    # Fallback for unknown skill phrases from AI.
    return bool(_compile_alias_pattern(normalized_skill).search(text_l))


def _canonical_skill_label(skill: str) -> str:
    normalized_skill = _clean_skill_value(skill).lower()
    if not normalized_skill:
        return ""
    for canonical, aliases in SKILL_ALIASES.items():
        all_aliases = [canonical.lower(), *[a.lower() for a in aliases]]
        if normalized_skill in all_aliases:
            return canonical
    return _clean_skill_value(skill)


def _resume_matches_canonical_skill(resume_text: str, canonical_skill: str) -> bool:
    # Direct canonical/alias hit is always accepted.
    if _skill_present_in_text(resume_text, canonical_skill):
        return True

    evidence = DOMAIN_EVIDENCE.get(canonical_skill, [])
    if not evidence:
        return False

    min_match = DOMAIN_EVIDENCE_MIN_MATCH.get(canonical_skill, 1)
    hit_count = 0
    for token in evidence:
        if _skill_present_in_text(resume_text, token):
            hit_count += 1
            if hit_count >= min_match:
                return True
    return False


def _reclassify_skills(
    candidate_skills: list[str],
    resume_text: str,
    job_description: str,
) -> tuple[list[str], list[str]]:
    present = []
    lacking = []

    for skill in _normalized_unique(candidate_skills):
        canonical_skill = _canonical_skill_label(skill)
        in_jd = _skill_present_in_text(job_description, canonical_skill)
        if not in_jd:
            continue

        if _resume_matches_canonical_skill(resume_text, canonical_skill):
            present.append(canonical_skill)
        else:
            lacking.append(canonical_skill)

    return _normalized_unique(present), _normalized_unique(lacking)


def _merge_present_lacking(
    base_present: list[str],
    base_lacking: list[str],
    add_present: list[str],
    add_lacking: list[str],
) -> tuple[list[str], list[str]]:
    final_present = _normalized_unique([*base_present, *add_present])
    present_lower = {s.lower() for s in final_present}
    final_lacking = _normalized_unique([*base_lacking, *add_lacking])
    final_lacking = [s for s in final_lacking if s.lower() not in present_lower]
    return final_present, final_lacking


def _get_model_candidates() -> list[str]:
    # Skip genai.list_models() - it is an uncapped network call that can hang.
    # A static preferred list is enough and keeps extraction fast.
    preferred = [
        GEMINI_MODEL,
        "gemini-1.5-flash-latest",
        "gemini-2.0-flash-lite",
        "gemini-2.0-flash",
    ]

    ordered_unique = []
    seen = set()
    for name in preferred:
        key = name.strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        ordered_unique.append(name.strip())
    return ordered_unique


def _fallback_extract_skills(resume_text: str, job_description: str) -> tuple[list[str], list[str]]:
    jd_candidates = []
    for canonical, aliases in SKILL_ALIASES.items():
        if _skill_present_in_text(job_description, canonical):
            jd_candidates.append(canonical)
            continue
        for alias in aliases:
            if _skill_present_in_text(job_description, alias):
                jd_candidates.append(canonical)
                break

    return _reclassify_skills(jd_candidates, resume_text, job_description)


def _extract_skills_with_gemini(
    resume_text: str,
    job_description: str,
) -> tuple[list[str], list[str], str]:
    baseline_present, baseline_lacking = _fallback_extract_skills(resume_text, job_description)

    if not GEMINI_API_KEY:
        return baseline_present, baseline_lacking, "fallback:no_api_key"

    prompt = f"""
You are an expert technical recruiter.

Task:
1) Extract all relevant required skills from the job description.
2) Extract all relevant skills from the candidate resume.
3) Compare both.
4) Use the baseline comparison below as already-verified skills.
5) Add only missing skills that baseline may have missed.

Important mapping rule:
- Map tool names to broader domains when appropriate.
- Example: n8n, Make, Zapier imply Automation.

Output:
- additional_present_skills: extra present skills to add beyond baseline.
- additional_lacking_skills: extra lacking skills to add beyond baseline.

Rules:
- Output strict JSON only.
- Keep each skill short (1-4 words).
- Do not include duplicates.
- Focus on technical and role-relevant professional skills.
- Do not repeat skills already in baseline lists.

Resume:
{resume_text}

Job Description:
{job_description}

Baseline Present Skills (already confirmed):
{json.dumps(baseline_present)}

Baseline Lacking Skills (already confirmed):
{json.dumps(baseline_lacking)}

Return format:
{{
    "additional_present_skills": ["Python", "FastAPI"],
    "additional_lacking_skills": ["Docker", "AWS"]
}}
"""

    model_candidates = _get_model_candidates()
    budget_seconds = 14
    deadline = time.monotonic() + budget_seconds

    for model_name in model_candidates:
        if time.monotonic() >= deadline:
            break
        try:
            # Keep extraction responsive; if the model is slow, fall back to deterministic matching.
            response = _generate_gemini_content(model_name, prompt, timeout_seconds=10)
            raw_text = (response.text or "").strip()
            cleaned = raw_text.replace("```json", "").replace("```", "").strip()
            parsed = json.loads(cleaned)

            ai_add_present_raw = parsed.get("additional_present_skills", parsed.get("present_skills", []))
            ai_add_lacking_raw = parsed.get("additional_lacking_skills", parsed.get("lacking_skills", []))

            ai_candidate_pool = [*ai_add_present_raw, *ai_add_lacking_raw]
            ai_present, ai_lacking = _reclassify_skills(
                ai_candidate_pool,
                resume_text,
                job_description,
            )

            merged_present, merged_lacking = _merge_present_lacking(
                baseline_present,
                baseline_lacking,
                ai_present,
                ai_lacking,
            )

            if merged_present or merged_lacking:
                return merged_present, merged_lacking, f"hybrid:baseline+ai:{model_name}"
        except Exception:
            continue

    return baseline_present, baseline_lacking, "fallback:model_unavailable"


def _days_until(interview_date: str) -> int:
    parsed_date = date.fromisoformat(interview_date)
    return (parsed_date - date.today()).days


def _get_session_present_skills(session_id: int):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id, skill_name, category, proficiency_score, priority_weight
        FROM skills
        WHERE session_id = ? AND category = 'present'
        ORDER BY id ASC
        """,
        (session_id,),
    )
    rows = cursor.fetchall()
    conn.close()
    return rows


def _get_session_by_id(session_id: int):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM sessions WHERE id = ?", (session_id,))
    session = cursor.fetchone()
    conn.close()
    return session


def _skill_has_generated_questions(cursor, session_id: int, skill_id: int) -> bool:
    cursor.execute(
        "SELECT COUNT(*) AS count FROM questions WHERE session_id = ? AND skill_id = ?",
        (session_id, skill_id),
    )
    return cursor.fetchone()["count"] > 0


def _normalize_mcq_option(value) -> str:
    return " ".join(str(value).strip().split())


def _looks_like_placeholder_option(option: str) -> bool:
    normalized = option.strip().lower().rstrip(".")
    return normalized in {
        "1",
        "2",
        "3",
        "4",
        "a",
        "b",
        "c",
        "d",
        "option 1",
        "option 2",
        "option 3",
        "option 4",
        "choice 1",
        "choice 2",
        "choice 3",
        "choice 4",
    }


def _question_expects_code_options(question_text: str) -> bool:
    lower_question = question_text.lower()
    code_signals = [
        "write a",
        "write an",
        "implement",
        "code",
        "function",
        "snippet",
        "output of",
        "what will this print",
    ]
    return any(signal in lower_question for signal in code_signals)


def _option_quality_is_valid(option: str) -> bool:
    normalized = option.strip()
    if not normalized or _looks_like_placeholder_option(normalized):
        return False
    if len(normalized) < 8:
        return False
    return True


def _skill_question_family(skill_name: str) -> str:
    canonical = _canonical_skill_label(skill_name).lower()

    automation_tokens = {
        "automation",
        "workflow automation",
        "process automation",
        "automation engineering",
        "zapier",
        "make",
        "n8n",
        "power automate",
    }
    coding_tokens = {
        "python",
        "java",
        "c",
        "c++",
        "c#",
        "go",
        "rust",
        "php",
        "ruby",
        "r",
        "matlab",
        "javascript",
        "typescript",
        "node.js",
        "sql",
    }
    platform_tokens = {
        "devops",
        "docker",
        "kubernetes",
        "ci/cd",
        "infrastructure as code",
        "linux",
        "cloud computing",
        "aws",
        "gcp",
        "azure",
        "serverless",
        "monitoring & observability",
        "networking",
        "microservices",
        "distributed systems",
        "message queues",
    }
    data_tokens = {
        "data science",
        "data engineering",
        "big data",
        "machine learning",
        "deep learning",
        "nlp",
        "computer vision",
        "generative ai",
        "pandas",
        "numpy",
    }
    testing_tokens = {
        "testing",
        "qa automation",
    }

    if canonical in automation_tokens:
        return "automation"
    if canonical in coding_tokens:
        return "coding"
    if canonical in platform_tokens:
        return "platform"
    if canonical in data_tokens:
        return "data"
    if canonical in testing_tokens:
        return "testing"
    return "general"


def _question_text_is_sensible(question_text: str, skill_name: str) -> bool:
    text = question_text.lower().strip()
    family = _skill_question_family(skill_name)

    # For most non-coding skills, avoid prompts that ask the candidate to build or implement a full system.
    if family != "coding":
        blocked_patterns = [
            r"\bbuild( a| an| the)?\b",
            r"\bcreate( a| an| the)?\b",
            r"\bimplement( a| an| the)?\b",
            r"\bwrite code\b",
            r"\bdevelop( a| an| the)?\b",
        ]
        if any(re.search(pattern, text) for pattern in blocked_patterns):
            return False

    if family == "automation":
        return any(
            token in text
            for token in [
                "workflow",
                "trigger",
                "action",
                "retry",
                "error",
                "logging",
                "integration",
                "mapping",
                "approval",
                "rate limit",
                "idempot",
            ]
        ) or "automation" in text

    if family in {"platform", "data", "testing", "general"}:
        return len(text) >= 12 and text.endswith("?")

    return len(text) >= 12 and text.endswith("?")


def _build_topic_question(skill_name: str, question_index: int, difficulty_level: int, question_text: str, options: list[str], ideal_answer: str) -> dict:
    correct_option = options[0]
    shuffled_options = options[:]
    random.shuffle(shuffled_options)

    return {
        "question_index": question_index,
        "difficulty_level": difficulty_level,
        "question_text": question_text,
        "options": shuffled_options,
        "correct_option_index": shuffled_options.index(correct_option) + 1,
        "ideal_answer": ideal_answer,
    }


def _fallback_questions_for_skill(skill_name: str) -> list[dict]:
    family = _skill_question_family(skill_name)

    if family == "automation":
        return [
            _build_topic_question(
                skill_name,
                1,
                1,
                f"What is the main purpose of {skill_name} in a workflow?",
                [
                    "To automate repetitive steps and reduce manual effort.",
                    "To redesign the user interface of a product.",
                    "To replace the need for any business rules.",
                    "To store database backups in a spreadsheet.",
                ],
                f"{skill_name} is used to automate repetitive steps and reduce manual effort.",
            ),
            _build_topic_question(
                skill_name,
                2,
                2,
                f"When designing {skill_name}, what should be defined first?",
                [
                    "The trigger, the input data, and the expected action.",
                    "The logo color and page margins.",
                    "The final report font size before the flow exists.",
                    "The last step only, without checking the inputs.",
                ],
                "Start with the trigger, input data, and expected action.",
            ),
            _build_topic_question(
                skill_name,
                3,
                3,
                f"A step in {skill_name} fails intermittently. What is the best next improvement?",
                [
                    "Add retries, logging, and a failure alert.",
                    "Hide the error and continue without checks.",
                    "Remove all validation to speed it up.",
                    "Duplicate the same step several times.",
                ],
                "Reliable automations usually need retries, logging, and alerts.",
            ),
            _build_topic_question(
                skill_name,
                4,
                4,
                f"What makes a {skill_name} workflow easier to maintain over time?",
                [
                    "Clear naming, modular steps, and versioned changes.",
                    "One huge step with no documentation.",
                    "Random manual edits in production.",
                    "Removing the input checks to save time.",
                ],
                "Clear naming, modular steps, and versioned changes improve maintainability.",
            ),
            _build_topic_question(
                skill_name,
                5,
                5,
                f"For a larger {skill_name} setup across multiple tools, which practice matters most?",
                [
                    "Idempotency, monitoring, and rate-limit handling.",
                    "Using the most colorful dashboard theme.",
                    "Skipping error handling to reduce code size.",
                    "Keeping every step manual for control.",
                ],
                "Large automations need idempotency, monitoring, and rate-limit handling.",
            ),
        ]

    if family == "coding":
        return [
            _build_topic_question(
                skill_name,
                1,
                1,
                f"What is the role of {skill_name} in software development?",
                [
                    f"It is a programming skill used to write software and solve technical problems.",
                    "It is only used for graphic design.",
                    "It is a social media management tool.",
                    "It is a hardware replacement strategy.",
                ],
                f"{skill_name} is used to write software and solve technical problems.",
            ),
            _build_topic_question(
                skill_name,
                2,
                2,
                f"Which idea is most important when using {skill_name} in a project?",
                [
                    "Readable structure, correct syntax, and clear control flow.",
                    "Only choosing the brightest color scheme.",
                    "Ignoring testing until the end.",
                    "Avoiding functions and keeping everything in one block.",
                ],
                "Readable structure, correct syntax, and clear control flow matter most.",
            ),
            _build_topic_question(
                skill_name,
                3,
                3,
                f"How do you usually approach debugging in {skill_name}?",
                [
                    "Reproduce the issue, inspect the error, and narrow the cause step by step.",
                    "Guess the fix and change multiple files at once.",
                    "Delete the test data without checking anything.",
                    "Ignore the error until it disappears.",
                ],
                "Debugging works best by reproducing the issue and narrowing the cause step by step.",
            ),
            _build_topic_question(
                skill_name,
                4,
                4,
                f"What is a strong practice when scaling a project that uses {skill_name}?",
                [
                    "Refactor for modularity, test important paths, and track dependencies.",
                    "Keep every feature in a single file forever.",
                    "Avoid code reviews to move faster.",
                    "Remove comments and tests to keep things minimal.",
                ],
                "Modularity, testing, and dependency awareness help projects scale.",
            ),
            _build_topic_question(
                skill_name,
                5,
                5,
                f"What best describes an advanced use of {skill_name} in a production system?",
                [
                    "A solution that balances correctness, maintainability, and performance trade-offs.",
                    "A temporary script that cannot be reused or tested.",
                    "A design with no error handling or edge-case thinking.",
                    "A one-time demo that cannot be adapted.",
                ],
                "Advanced use balances correctness, maintainability, and performance trade-offs.",
            ),
        ]

    if family == "platform":
        return [
            _build_topic_question(
                skill_name,
                1,
                1,
                f"What is the primary goal of {skill_name}?",
                [
                    f"To run, deploy, or operate systems reliably at scale.",
                    "To replace all software with spreadsheets.",
                    "To create marketing copy faster.",
                    "To avoid monitoring any services.",
                ],
                "Platform skills focus on reliable operation, deployment, and scaling.",
            ),
            _build_topic_question(
                skill_name,
                2,
                2,
                f"Which factor is most important when configuring {skill_name}?",
                [
                    "Security, repeatability, and clear operational boundaries.",
                    "Only the visual theme of the console.",
                    "The number of emojis in the config file.",
                    "Removing all permissions for simplicity.",
                ],
                "Security, repeatability, and clear operational boundaries matter most.",
            ),
            _build_topic_question(
                skill_name,
                3,
                3,
                f"What is the best reaction when a {skill_name} pipeline or deployment fails?",
                [
                    "Check logs, isolate the failed stage, and roll back or retry safely.",
                    "Ignore it and deploy again immediately.",
                    "Delete logs before reviewing them.",
                    "Increase the number of manual steps.",
                ],
                "Check logs, isolate the failed stage, and handle rollback or retry safely.",
            ),
            _build_topic_question(
                skill_name,
                4,
                4,
                f"What helps a growing {skill_name} setup stay manageable?",
                [
                    "Automation, observability, and version-controlled configuration.",
                    "Changing settings manually in production every time.",
                    "Avoiding documentation completely.",
                    "Using a different naming style for every service.",
                ],
                "Automation, observability, and version-controlled config keep systems manageable.",
            ),
            _build_topic_question(
                skill_name,
                5,
                5,
                f"For an advanced {skill_name} role, what trade-off is most important?",
                [
                    "Balancing reliability, speed of delivery, and operational cost.",
                    "Choosing the busiest dashboard over the fastest one.",
                    "Removing alerts to reduce noise.",
                    "Skipping access control for convenience.",
                ],
                "Advanced platform work balances reliability, speed, and operational cost.",
            ),
        ]

    if family == "data":
        return [
            _build_topic_question(
                skill_name,
                1,
                1,
                f"What is the main purpose of {skill_name}?",
                [
                    "To extract insights, model data, or solve analytical problems.",
                    "To design the color palette for reports only.",
                    "To replace data with random text.",
                    "To avoid using metrics completely.",
                ],
                "Data skills focus on insight, modeling, and analytical problem-solving.",
            ),
            _build_topic_question(
                skill_name,
                2,
                2,
                f"Which step usually comes first in a {skill_name} project?",
                [
                    "Understand the problem, inspect the data, and check data quality.",
                    "Choose a model before seeing the data.",
                    "Present the final chart before analysis.",
                    "Skip cleaning to move faster.",
                ],
                "Start by understanding the problem and checking the quality of the data.",
            ),
            _build_topic_question(
                skill_name,
                3,
                3,
                f"How should you handle messy input data in {skill_name}?",
                [
                    "Clean, transform, and validate before analysis or training.",
                    "Use it unchanged so the results stay simple.",
                    "Drop all records with no review.",
                    "Ignore missing values entirely.",
                ],
                "Cleaning, transforming, and validating data comes before analysis.",
            ),
            _build_topic_question(
                skill_name,
                4,
                4,
                f"What is a common trade-off in {skill_name} work?",
                [
                    "Accuracy versus interpretability or speed versus complexity.",
                    "Choosing between blue and green dashboards.",
                    "Whether to use fewer keyboard shortcuts.",
                    "How many icons appear in a notebook.",
                ],
                "Data work often balances accuracy, interpretability, and complexity.",
            ),
            _build_topic_question(
                skill_name,
                5,
                5,
                f"In an advanced {skill_name} workflow, what is most important before sharing results?",
                [
                    "Validate assumptions, test robustness, and explain limitations clearly.",
                    "Share the first output without review.",
                    "Hide the source data from everyone.",
                    "Skip reproducibility to save time.",
                ],
                "Advanced data work validates assumptions and explains limitations clearly.",
            ),
        ]

    return [
        _build_topic_question(
            skill_name,
            1,
            1,
            f"What best describes {skill_name}?",
            [
                f"A practical skill used in technical work and problem solving.",
                "A purely decorative concept with no real use.",
                "A marketing-only topic.",
                "An unrelated hobby with no technical context.",
            ],
            f"{skill_name} is a practical skill used in technical work and problem solving.",
        ),
        _build_topic_question(
            skill_name,
            2,
            2,
            f"What should you understand first when learning {skill_name}?",
            [
                "The core concepts, typical workflow, and main use cases.",
                "Only the final advanced implementation details.",
                "A random unrelated framework.",
                "The tool logo and branding rules.",
            ],
            "Start with the core concepts, workflow, and common use cases.",
        ),
        _build_topic_question(
            skill_name,
            3,
            3,
            f"How would you apply {skill_name} in a real project?",
            [
                "By choosing a reasonable approach, testing it, and adjusting for constraints.",
                "By ignoring the project requirements.",
                "By using a completely unrelated method.",
                "By copying a result without checking it.",
            ],
            "Apply the skill with a reasonable approach, testing, and adjustment to constraints.",
        ),
        _build_topic_question(
            skill_name,
            4,
            4,
            f"What is a common challenge when using {skill_name}?",
            [
                "Knowing the trade-offs, limitations, and edge cases.",
                "Choosing the wrong color for the button.",
                "Using too many unrelated terms.",
                "Avoiding all documentation.",
            ],
            "A strong answer mentions trade-offs, limitations, and edge cases.",
        ),
        _build_topic_question(
            skill_name,
            5,
            5,
            f"What distinguishes an advanced answer about {skill_name}?",
            [
                "It explains decisions, trade-offs, and practical impact clearly.",
                "It repeats the definition without context.",
                "It avoids any mention of real-world usage.",
                "It gives a vague answer with no structure.",
            ],
            "Advanced answers explain decisions, trade-offs, and practical impact clearly.",
        ),
    ]


def _question_options_are_valid(question_text: str, options: list[str]) -> bool:
    if len(options) != 4:
        return False

    if any(not _option_quality_is_valid(option) for option in options):
        return False

    if _question_expects_code_options(question_text):
        code_like_options = 0
        for option in options:
            if any(token in option for token in ["def ", "return ", "print(", "for ", "if ", "else:", "while ", "import "]):
                code_like_options += 1
        if code_like_options < 4:
            return False

    return True


def _stored_question_is_valid(row) -> bool:
    try:
        question_text = _normalize_mcq_option(row["question_text"] or "")
        options_raw = row["options"] or "[]"
        options = json.loads(options_raw) if isinstance(options_raw, str) else options_raw
        normalized_options = [_normalize_mcq_option(option) for option in options]
        normalized_options = [option for option in normalized_options if option]
        correct_option_index = int(row["correct_option_index"] or 0)
    except Exception:
        return False

    if not question_text or len(normalized_options) != 4:
        return False
    if correct_option_index < 1 or correct_option_index > 4:
        return False
    return _question_options_are_valid(question_text, normalized_options)


def _normalize_mcq_question(item: dict, fallback_index: int, skill_name: str) -> dict | None:
    question_text = _normalize_mcq_option(item.get("question_text", ""))
    options = item.get("options", [])
    normalized_options = [_normalize_mcq_option(option) for option in options]
    normalized_options = [option for option in normalized_options if option]

    if not question_text or len(normalized_options) != 4:
        return None

    if not _question_text_is_sensible(question_text, skill_name):
        return None

    deduped = []
    seen = set()
    for option in normalized_options:
        key = option.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(option)

    if len(deduped) != 4 or not _question_options_are_valid(question_text, deduped):
        return None

    correct_option_index = int(item.get("correct_option_index", fallback_index))
    if correct_option_index < 1 or correct_option_index > 4:
        correct_option_index = fallback_index
    if correct_option_index < 1 or correct_option_index > 4:
        correct_option_index = 1

    ideal_answer = _normalize_mcq_option(item.get("ideal_answer", ""))

    correct_option = deduped[correct_option_index - 1]
    shuffled_options = deduped[:]
    random.shuffle(shuffled_options)

    return {
        "question_index": int(item.get("question_index", fallback_index)),
        "difficulty_level": int(item.get("difficulty_level", fallback_index)),
        "question_text": question_text,
        "options": shuffled_options,
        "correct_option_index": shuffled_options.index(correct_option) + 1,
        "ideal_answer": ideal_answer or f"Correct answer for {skill_name}.",
    }


def _generate_questions_for_skill(skill_name: str) -> list[dict]:
    question_api_key = (QUESTION_GEMINI_API_KEY or GEMINI_API_KEY).strip()
    if not question_api_key:
        return _fallback_questions_for_skill(skill_name)

    family = _skill_question_family(skill_name)

    prompt = f"""
You are an expert interview coach.

Create 5 interview questions for the skill: {skill_name}

Skill family: {family}

Requirements:
- Questions must increase in difficulty from 1 to 5.
- Ask practical, skill-specific questions that a real interviewer could ask.
- Each question must have exactly four options relevant to the question asked.
- Exactly one option must be correct.
- Every option must be a complete answer choice, never placeholder labels like 1, 2, 3, 4 or A, B, C, D.
- Keep all distractors plausible and in the same topic as the question.
- Do not mix unrelated topics into the wrong answers.
- Do not ask the candidate to build an entire system or automation from scratch unless the skill itself is a coding language and the question is specifically about code.
- For non-coding skills, avoid verbs like build/create/implement/develop/write code.
- For automation and workflow tools, ask about triggers, actions, routing, retries, mappings, validation, monitoring, and troubleshooting instead of asking the user to build a full automation.
- For platform/DevOps/cloud skills, ask about deployment, reliability, observability, security, and rollback.
- For data skills, ask about data quality, analysis steps, interpretation, and trade-offs.
- For coding skills, code-based questions are allowed only when all four options are actual code snippets or code-based answers.
- Include an ideal answer for grading.
- Keep each question concise but useful.
- Return strict JSON only.

Question style guidance:
- Easy questions should test the core purpose or meaning of the skill.
- Middle questions should test the workflow, common components, or practical usage.
- Hard questions should test trade-offs, troubleshooting, or best practices.
- Every option must stay on the same topic as the question.
- The correct answer should be clearly better, not just slightly longer.

Preferred structure by difficulty:
1. Core concept or purpose
2. Main component / workflow / configuration
3. Practical scenario or troubleshooting
4. Trade-off / edge case / reliability
5. Advanced judgment or optimization choice

Return format:
{{
  "questions": [
                {{"question_index": 1, "difficulty_level": 1, "question_text": "...", "options": ["...", "...", "...", "..."], "correct_option_index": 3, "ideal_answer": "..."}},
                {{"question_index": 2, "difficulty_level": 2, "question_text": "...", "options": ["...", "...", "...", "..."], "correct_option_index": 1, "ideal_answer": "..."}},
                {{"question_index": 3, "difficulty_level": 3, "question_text": "...", "options": ["...", "...", "...", "..."], "correct_option_index": 4, "ideal_answer": "..."}},
                {{"question_index": 4, "difficulty_level": 4, "question_text": "...", "options": ["...", "...", "...", "..."], "correct_option_index": 2, "ideal_answer": "..."}},
                {{"question_index": 5, "difficulty_level": 5, "question_text": "...", "options": ["...", "...", "...", "..."], "correct_option_index": 1, "ideal_answer": "..."}}
  ]
}}
"""

    model_candidates = _get_model_candidates()
    for model_name in model_candidates:
        try:
            # Keep assessment start responsive; fallback questions are used if model is slow.
            response = _generate_gemini_content(
                model_name,
                prompt,
                timeout_seconds=12,
                api_key=question_api_key,
            )
            raw_text = (response.text or "").strip()
            cleaned = raw_text.replace("```json", "").replace("```", "").strip()
            parsed = json.loads(cleaned)
            questions = parsed.get("questions", [])
            normalized = []
            for idx, item in enumerate(questions, start=1):
                normalized_item = _normalize_mcq_question(item, idx, skill_name)
                if normalized_item is None:
                    continue
                normalized.append(normalized_item)
            if len(normalized) == 5:
                return normalized
        except Exception:
            continue

    return _fallback_questions_for_skill(skill_name)


def _generate_missing_questions_for_session(session_id: int) -> int:
    session = _get_session_by_id(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="Session not found.")

    skills = _get_session_present_skills(session_id)[:MAX_SKILLS_FOR_QUESTION_GENERATION]
    if not skills:
        return 0

    conn = get_connection()
    cursor = conn.cursor()
    inserted = 0

    for skill in skills:
        cursor.execute(
            """
            SELECT id, question_text, options, correct_option_index
            FROM questions
            WHERE session_id = ? AND skill_id = ?
            ORDER BY question_index ASC, id ASC
            """,
            (session_id, skill["id"]),
        )
        existing_questions = cursor.fetchall()

        if existing_questions:
            all_valid = len(existing_questions) == 5 and all(
                _stored_question_is_valid(question) for question in existing_questions
            )
            if all_valid:
                continue

            cursor.execute(
                "DELETE FROM questions WHERE session_id = ? AND skill_id = ?",
                (session_id, skill["id"]),
            )

        questions = _generate_questions_for_skill(skill["skill_name"])
        for question in questions:
            cursor.execute(
                """
                INSERT INTO questions (
                    session_id,
                    skill_id,
                    question_index,
                    difficulty_level,
                    question_text,
                    options,
                    correct_option_index,
                    ideal_answer,
                    asked_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    session_id,
                    skill["id"],
                    question["question_index"],
                    question["difficulty_level"],
                    question["question_text"],
                    json.dumps(question["options"]),
                    question["correct_option_index"],
                    question.get("ideal_answer", ""),
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            inserted += 1

    if inserted > 0:
        cursor.execute(
            "UPDATE sessions SET status = ? WHERE id = ?",
            ("assessment_started", session_id),
        )
        conn.commit()

    conn.close()
    return inserted


def _prepare_assessment_questions_for_session(session_id: int) -> int:
    """Generate or refresh questions for a session as soon as the skills are known."""
    return _generate_missing_questions_for_session(session_id)


def _retry_sqlite_write(operation, attempts: int = 5, delay_seconds: float = 0.4):
    last_exc = None
    for attempt in range(attempts):
        try:
            return operation()
        except sqlite3.OperationalError as exc:
            last_exc = exc
            if "database is locked" not in str(exc).lower() or attempt == attempts - 1:
                raise
            time.sleep(delay_seconds * (attempt + 1))
    if last_exc:
        raise last_exc


def _set_session_status(session_id: int, status: str):
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE sessions SET status = ? WHERE id = ?",
            (status, session_id),
        )
        conn.commit()
    finally:
        conn.close()


def _queue_question_generation_for_session(session_id: int):
    """Generate assessment questions in background after skill extraction."""
    try:
        _set_session_status(session_id, "question_generation_in_progress")
        _retry_sqlite_write(lambda: _prepare_assessment_questions_for_session(session_id))

        # If question generation did not create rows (for example no present skills),
        # keep session status coherent and avoid leaving it in an in-progress state.
        next_question = _fetch_next_unanswered_question(session_id)
        if next_question is None:
            session = _get_session_by_id(session_id)
            if session and session["status"] == "question_generation_in_progress":
                _set_session_status(session_id, "skills_extracted")
    except Exception:
        _set_session_status(session_id, "question_generation_failed")


def _fetch_next_unanswered_question(session_id: int):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT q.*, s.skill_name
        FROM questions q
        JOIN skills s ON s.id = q.skill_id
        WHERE q.session_id = ? AND q.answer_text IS NULL
        ORDER BY s.id ASC, q.question_index ASC, q.id ASC
        LIMIT 1
        """,
        (session_id,),
    )
    row = cursor.fetchone()
    conn.close()
    if row is None:
        return None

    payload = dict(row)
    payload["options"] = json.loads(payload["options"]) if payload.get("options") else []
    return payload


def _score_mcq_answer(
    skill_name: str,
    question_text: str,
    ideal_answer: str,
    user_answer: str,
    options: list[str],
    correct_option_index: int,
    selected_option_index: int,
):
    is_correct = selected_option_index == correct_option_index
    score = 1.0 if is_correct else 0.0

    if is_correct:
        feedback = f"Correct. {ideal_answer or f'That matches the expected answer for {skill_name}.'}"
    else:
        correct_answer = ""
        if 1 <= correct_option_index <= len(options):
            correct_answer = options[correct_option_index - 1]
        feedback = (
            f"Incorrect. The best answer was: {correct_answer}. "
            f"{ideal_answer or f'Review the core concept for {skill_name}.'}"
        ).strip()

    return score, feedback


def _recalculate_skill_metrics_with_cursor(cursor, session_id: int, skill_id: int):
    cursor.execute(
        """
        SELECT AVG(score) AS avg_score
        FROM questions
        WHERE session_id = ? AND skill_id = ? AND score IS NOT NULL
        """,
        (session_id, skill_id),
    )
    avg_score = cursor.fetchone()["avg_score"]
    if avg_score is None:
        return None, None

    avg_score = float(avg_score)
    priority_weight = round(1.0 + (1.0 - avg_score) * 4.0, 2)
    cursor.execute(
        """
        UPDATE skills
        SET proficiency_score = ?, priority_weight = ?
        WHERE id = ?
        """,
        (round(avg_score, 2), priority_weight, skill_id),
    )
    return round(avg_score, 2), priority_weight


def _recalculate_skill_metrics(session_id: int, skill_id: int):
    conn = get_connection()
    cursor = conn.cursor()
    avg_score, priority_weight = _recalculate_skill_metrics_with_cursor(cursor, session_id, skill_id)
    conn.commit()
    conn.close()
    return avg_score, priority_weight


def _session_assessment_complete(session_id: int) -> bool:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT COUNT(*) AS count FROM questions WHERE session_id = ? AND answer_text IS NULL",
        (session_id,),
    )
    count = cursor.fetchone()["count"]
    conn.close()
    return count == 0


def _session_assessment_complete_with_cursor(cursor, session_id: int) -> bool:
    cursor.execute(
        "SELECT COUNT(*) AS count FROM questions WHERE session_id = ? AND answer_text IS NULL",
        (session_id,),
    )
    count = cursor.fetchone()["count"]
    return count == 0


def _assessment_summary(session_id: int):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id, skill_name, category, proficiency_score, priority_weight
        FROM skills
        WHERE session_id = ?
        ORDER BY category ASC, priority_weight DESC, skill_name ASC
        """,
        (session_id,),
    )
    skills = [dict(row) for row in cursor.fetchall()]

    cursor.execute(
        "SELECT COUNT(*) AS total_questions FROM questions WHERE session_id = ?",
        (session_id,),
    )
    total_questions = cursor.fetchone()["total_questions"]

    cursor.execute(
        "SELECT COUNT(*) AS answered_questions FROM questions WHERE session_id = ? AND answer_text IS NOT NULL",
        (session_id,),
    )
    answered_questions = cursor.fetchone()["answered_questions"]
    conn.close()

    progress = 0 if total_questions == 0 else round((answered_questions / total_questions) * 100, 2)
    return {
        "session_id": session_id,
        "total_questions": total_questions,
        "answered_questions": answered_questions,
        "progress_percent": progress,
        "skills": skills,
    }


def _question_payload(row):
    if row is None:
        return None
    payload = dict(row)
    options_value = payload.get("options")
    if isinstance(options_value, str):
        payload["options"] = json.loads(options_value) if options_value else []
    elif isinstance(options_value, list):
        payload["options"] = options_value
    else:
        payload["options"] = []
    return payload

@app.get("/")
async def root():
    return {"status": "ok", "message": "Skill Assessment Agent API is running"}

@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.post("/api/module2/extract-skills")
async def extract_skills_module2(
    background_tasks: BackgroundTasks,
    job_description: str = Form(...),
    interview_date: str = Form(...),
    resume_text: str = Form(""),
    resume_file: UploadFile | None = File(default=None),
):
    if len(job_description) > MAX_JOB_DESCRIPTION_CHARS:
        raise HTTPException(
            status_code=413,
            detail=f"Job description is too long (max {MAX_JOB_DESCRIPTION_CHARS} chars).",
        )
    if len(resume_text) > MAX_RESUME_TEXT_CHARS:
        raise HTTPException(
            status_code=413,
            detail=f"Resume text is too long (max {MAX_RESUME_TEXT_CHARS} chars).",
        )

    parsed_resume_text = ""

    if resume_file is not None:
        if not resume_file.filename.lower().endswith(".pdf"):
            raise HTTPException(status_code=400, detail="Resume file must be a PDF.")
        file_bytes = await resume_file.read()
        if not file_bytes:
            raise HTTPException(status_code=400, detail="Uploaded resume PDF is empty.")
        parsed_resume_text = _extract_text_from_pdf_bytes(file_bytes)

    final_resume_text = "\n".join([resume_text.strip(), parsed_resume_text.strip()]).strip()
    if not final_resume_text:
        raise HTTPException(
            status_code=400,
            detail="Provide either resume text or a resume PDF.",
        )
    if len(final_resume_text) > MAX_FINAL_RESUME_CHARS:
        raise HTTPException(
            status_code=413,
            detail=f"Combined resume content is too long (max {MAX_FINAL_RESUME_CHARS} chars).",
        )

    try:
        days_remaining = _days_until(interview_date)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Interview date must be YYYY-MM-DD.") from exc

    present_skills, lacking_skills, matching_engine = _extract_skills_with_gemini(
        final_resume_text,
        job_description,
    )

    def _insert_session_and_skills() -> int:
        conn = get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO sessions (resume_text, job_description, interview_date, status)
                VALUES (?, ?, ?, ?)
                """,
                (final_resume_text, job_description, interview_date, "skills_extracted"),
            )
            session_id = cursor.lastrowid

            for skill in present_skills:
                cursor.execute(
                    """
                    INSERT INTO skills (session_id, skill_name, category)
                    VALUES (?, ?, 'present')
                    """,
                    (session_id, skill),
                )

            for skill in lacking_skills:
                cursor.execute(
                    """
                    INSERT INTO skills (session_id, skill_name, category)
                    VALUES (?, ?, 'lacking')
                    """,
                    (session_id, skill),
                )

            conn.commit()
            return session_id
        finally:
            conn.close()

    session_id = _retry_sqlite_write(_insert_session_and_skills)

    # Queue question generation in background so skill extraction response returns quickly.
    question_generation_status = "not_required"
    if present_skills:
        question_generation_status = "queued"
        background_tasks.add_task(_queue_question_generation_for_session, session_id)

    return {
        "session_id": session_id,
        "interview_date": interview_date,
        "days_remaining": days_remaining,
        "matching_engine": matching_engine,
        "question_generation_status": question_generation_status,
        "present_skills": present_skills,
        "lacking_skills": lacking_skills,
    }


@app.get("/api/sessions/{session_id}/skills")
async def get_session_skills(session_id: int):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM sessions WHERE id = ?", (session_id,))
    session = cursor.fetchone()
    if session is None:
        conn.close()
        raise HTTPException(status_code=404, detail="Session not found.")

    cursor.execute(
        """
        SELECT skill_name, category
        FROM skills
        WHERE session_id = ?
        ORDER BY category ASC, skill_name ASC
        """,
        (session_id,),
    )
    rows = cursor.fetchall()
    conn.close()

    present_skills = [row["skill_name"] for row in rows if row["category"] == "present"]
    lacking_skills = [row["skill_name"] for row in rows if row["category"] == "lacking"]

    return {
        "session_id": session_id,
        "interview_date": session["interview_date"],
        "days_remaining": _days_until(session["interview_date"]),
        "present_skills": present_skills,
        "lacking_skills": lacking_skills,
    }


@app.post("/api/module3/sessions/{session_id}/start-assessment")
async def start_module3_assessment(session_id: int, background_tasks: BackgroundTasks):
    session = _get_session_by_id(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="Session not found.")

    next_question = _fetch_next_unanswered_question(session_id)
    inserted_count = 0
    if next_question is None:
        # If questions are not yet generated for a session with present skills,
        # lazily queue generation so assessment can open immediately and poll.
        if session["status"] in {"skills_extracted", "question_generation_failed"}:
            background_tasks.add_task(_queue_question_generation_for_session, session_id)

        return {
            "session_id": session_id,
            "status": _get_session_by_id(session_id)["status"],
            "questions_created": inserted_count,
            "total_questions": 0,
            "next_question": None,
            "is_generating": True,
        }

    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT COUNT(*) AS total_questions FROM questions WHERE session_id = ?",
        (session_id,),
    )
    total_questions = cursor.fetchone()["total_questions"]
    conn.close()

    return {
        "session_id": session_id,
        "status": session["status"],
        "questions_created": inserted_count,
        "total_questions": total_questions,
        "next_question": _question_payload(next_question),
    }


@app.get("/api/module3/sessions/{session_id}/next-question")
async def get_next_module3_question(session_id: int):
    session = _get_session_by_id(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="Session not found.")

    next_question = _fetch_next_unanswered_question(session_id)
    if next_question is None:
        conn = get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) AS total_present_skills FROM skills WHERE session_id = ? AND category = 'present'",
                (session_id,),
            )
            total_present_skills = cursor.fetchone()["total_present_skills"]

            cursor.execute(
                "SELECT COUNT(*) AS total_questions FROM questions WHERE session_id = ?",
                (session_id,),
            )
            total_questions = cursor.fetchone()["total_questions"]
        finally:
            conn.close()

        # If this session has present skills but no question rows yet, generation is still pending.
        if total_present_skills > 0 and total_questions == 0:
            return {
                "session_id": session_id,
                "is_complete": False,
                "is_generating": True,
                "next_question": None,
                "summary": None,
            }

        return {
            "session_id": session_id,
            "is_complete": True,
            "is_generating": False,
            "next_question": None,
            "summary": _assessment_summary(session_id),
        }

    return {
        "session_id": session_id,
        "is_complete": False,
        "is_generating": False,
        "next_question": _question_payload(next_question),
    }


@app.post("/api/module3/questions/{question_id}/answer")
async def answer_module3_question(question_id: int, selected_option_index: int = Form(...)):
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT q.*, s.skill_name
            FROM questions q
            JOIN skills s ON s.id = q.skill_id
            WHERE q.id = ?
            """,
            (question_id,),
        )
        question = cursor.fetchone()
        if question is None:
            raise HTTPException(status_code=404, detail="Question not found.")

        if question["answer_text"] is not None:
            # Idempotent retry path: return previous result if the same option was already submitted.
            if question["selected_option_index"] == selected_option_index:
                completed = _session_assessment_complete_with_cursor(cursor, question["session_id"])
                next_question = _fetch_next_unanswered_question(question["session_id"])
                return {
                    "question_id": question_id,
                    "session_id": question["session_id"],
                    "skill_id": question["skill_id"],
                    "skill_name": question["skill_name"],
                    "score": question["score"],
                    "feedback": question["feedback"],
                    "avg_skill_score": None,
                    "priority_weight": None,
                    "is_complete": completed,
                    "next_question": _question_payload(next_question),
                    "summary": _assessment_summary(question["session_id"]) if completed else None,
                }
            raise HTTPException(status_code=400, detail="Question already answered.")

        options = json.loads(question["options"]) if question["options"] else []
        if selected_option_index < 1 or selected_option_index > len(options):
            raise HTTPException(status_code=400, detail="Selected option is invalid.")

        selected_option_text = options[selected_option_index - 1]

        score, feedback = _score_mcq_answer(
            question["skill_name"],
            question["question_text"],
            question["ideal_answer"] or "",
            selected_option_text,
            options,
            int(question["correct_option_index"] or 1),
            selected_option_index,
        )

        cursor.execute(
            """
            UPDATE questions
            SET selected_option_index = ?, answer_text = ?, score = ?, feedback = ?, answered_at = ?
            WHERE id = ?
            """,
            (selected_option_index, selected_option_text, score, feedback, datetime.now(timezone.utc).isoformat(), question_id),
        )

        avg_score, priority_weight = _recalculate_skill_metrics_with_cursor(
            cursor,
            question["session_id"],
            question["skill_id"],
        )

        completed = _session_assessment_complete_with_cursor(cursor, question["session_id"])
        if completed:
            cursor.execute(
                "UPDATE sessions SET status = ? WHERE id = ?",
                ("assessment_completed", question["session_id"]),
            )

        conn.commit()
    except sqlite3.OperationalError as exc:
        message = str(exc).lower()
        if "database is locked" in message:
            raise HTTPException(status_code=503, detail="Database busy. Please retry your answer.") from exc
        raise
    finally:
        conn.close()

    next_question = _fetch_next_unanswered_question(question["session_id"])

    return {
        "question_id": question_id,
        "session_id": question["session_id"],
        "skill_id": question["skill_id"],
        "skill_name": question["skill_name"],
        "score": score,
        "feedback": feedback,
        "avg_skill_score": avg_score,
        "priority_weight": priority_weight,
        "is_complete": completed,
        "next_question": _question_payload(next_question),
        "summary": _assessment_summary(question["session_id"]) if completed else None,
    }


@app.get("/api/module3/sessions/{session_id}/summary")
async def get_module3_summary(session_id: int):
    session = _get_session_by_id(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="Session not found.")
    return _assessment_summary(session_id)


class TopicCompletionUpdate(BaseModel):
    is_completed: bool
    plan_id: int | None = None
    skill_id: int | None = None
    topic_name: str | None = None
    day_number: int | None = None


class PracticeTestAnswer(BaseModel):
    question_id: int
    selected_option_index: int


class PracticeTestSubmission(BaseModel):
    answers: list[PracticeTestAnswer]


MAX_STUDY_PLAN_ENTRIES = 7


def _free_resources_for_skill(skill_name: str) -> list[dict]:
    canonical = _canonical_skill_label(skill_name)
    query = canonical.replace(" ", "+")

    base_resources = [
        {
            "title": f"Roadmap.sh - {canonical}",
            "url": f"https://roadmap.sh/{query.lower()}",
            "type": "roadmap",
        },
        {
            "title": f"freeCodeCamp search: {canonical}",
            "url": f"https://www.freecodecamp.org/news/search/?query={query}",
            "type": "article",
        },
        {
            "title": f"YouTube: {canonical} tutorial", 
            "url": f"https://www.youtube.com/results?search_query={query}+tutorial",
            "type": "video",
        },
    ]

    overrides = {
        "Python": [
            {"title": "Python docs", "url": "https://docs.python.org/3/tutorial/", "type": "docs"},
            {"title": "Automate the Boring Stuff", "url": "https://automatetheboringstuff.com/", "type": "book"},
            {"title": "Corey Schafer Python Playlist", "url": "https://www.youtube.com/playlist?list=PL-osiE80TeTsqhIuOqKhwlXsIBIdSeYtc", "type": "video"},
        ],
        "FastAPI": [
            {"title": "FastAPI docs", "url": "https://fastapi.tiangolo.com/tutorial/", "type": "docs"},
            {"title": "FastAPI SQLModel tutorial", "url": "https://fastapi.tiangolo.com/tutorial/sql-databases/", "type": "docs"},
            {"title": "FastAPI crash course", "url": "https://www.youtube.com/results?search_query=fastapi+crash+course", "type": "video"},
        ],
        "SQL": [
            {"title": "SQLBolt", "url": "https://sqlbolt.com/", "type": "interactive"},
            {"title": "PostgreSQL tutorial", "url": "https://www.postgresqltutorial.com/", "type": "docs"},
            {"title": "Mode SQL tutorial", "url": "https://mode.com/sql-tutorial/", "type": "interactive"},
        ],
        "React": [
            {"title": "React docs", "url": "https://react.dev/learn", "type": "docs"},
            {"title": "Scrimba React course", "url": "https://scrimba.com/learn/learnreact", "type": "course"},
            {"title": "React tutorial videos", "url": "https://www.youtube.com/results?search_query=react+tutorial", "type": "video"},
        ],
        "Docker": [
            {"title": "Docker docs", "url": "https://docs.docker.com/get-started/", "type": "docs"},
            {"title": "Play with Docker", "url": "https://labs.play-with-docker.com/", "type": "interactive"},
            {"title": "Docker tutorial videos", "url": "https://www.youtube.com/results?search_query=docker+tutorial", "type": "video"},
        ],
        "AWS": [
            {"title": "AWS Skill Builder", "url": "https://explore.skillbuilder.aws/learn", "type": "course"},
            {"title": "AWS docs", "url": "https://docs.aws.amazon.com/", "type": "docs"},
            {"title": "AWS workshops", "url": "https://workshops.aws/", "type": "workshop"},
        ],
    }

    return overrides.get(canonical, base_resources)


VIDEO_SUGGESTIONS_BY_SKILL = {
    "Python": [
        {
            "title": "Corey Schafer Python Tutorials",
            "url": "https://www.youtube.com/playlist?list=PL-osiE80TeTt2d9bfVyTiXJA-UTHn6WwU",
            "type": "video",
        }
    ],
    "SQL": [
        {
            "title": "freeCodeCamp SQL Tutorial - Full Database Course for Beginners",
            "url": "https://www.youtube.com/watch?v=HXV3zeQKqGY",
            "type": "video",
        }
    ],
    "JavaScript": [
        {
            "title": "JavaScript Full Course for Beginners",
            "url": "https://www.youtube.com/watch?v=PkZNo7MFNFg",
            "type": "video",
        }
    ],
    "React": [
        {
            "title": "React Course - Beginner's Tutorial for React JavaScript Library",
            "url": "https://www.youtube.com/watch?v=bMknfKXIFA8",
            "type": "video",
        }
    ],
}


VIDEO_RESOURCES_BY_SKILL = {
    "Data Structures": [
        {
            "title": "Data Structures Easy to Advanced Course - Full Tutorial from a Google Engineer",
            "url": "https://www.youtube.com/watch?v=RBSGKlAvoiM",
            "type": "video",
        }
    ],
    "Algorithms": [
        {
            "title": "Algorithms and Data Structures Tutorial - Full Course for Beginners",
            "url": "https://www.youtube.com/watch?v=8hly31xKli0",
            "type": "video",
        }
    ],
    "Object-Oriented Programming": [
        {
            "title": "Object Oriented Programming (OOP) in Python 3",
            "url": "https://www.youtube.com/watch?v=JeznW_7DlB0",
            "type": "video",
        }
    ],
    "Operating Systems": [
        {
            "title": "Operating Systems Full Course",
            "url": "https://www.youtube.com/watch?v=26QPDBe-NB8",
            "type": "video",
        }
    ],
    "Networking": [
        {
            "title": "Computer Networking Course - Network Engineering",
            "url": "https://www.youtube.com/watch?v=qiQR5rTSshw",
            "type": "video",
        }
    ],
    "System Design": [
        {
            "title": "System Design Interview Course",
            "url": "https://www.youtube.com/watch?v=bUHFg8CZFws",
            "type": "video",
        }
    ],
    "C": [
        {
            "title": "C Programming Tutorial for Beginners",
            "url": "https://www.youtube.com/watch?v=KJgsSFOSQv0",
            "type": "video",
        }
    ],
    "C++": [
        {
            "title": "C++ Programming Course - Beginner to Advanced",
            "url": "https://www.youtube.com/watch?v=8jLOx1hD3_o",
            "type": "video",
        }
    ],
    "Java": [
        {
            "title": "Java Tutorial for Beginners",
            "url": "https://www.youtube.com/watch?v=eIrMbAQSU34",
            "type": "video",
        }
    ],
    "Python": [
        {
            "title": "Python Full Course for Beginners",
            "url": "https://www.youtube.com/watch?v=rfscVS0vtbw",
            "type": "video",
        }
    ],
    "JavaScript": [
        {
            "title": "JavaScript Tutorial for Beginners - Full Course",
            "url": "https://www.youtube.com/watch?v=PkZNo7MFNFg",
            "type": "video",
        }
    ],
    "TypeScript": [
        {
            "title": "TypeScript Course for Beginners",
            "url": "https://www.youtube.com/watch?v=30LWjhZzg50",
            "type": "video",
        }
    ],
    "Go": [
        {
            "title": "Learn Go Programming - Golang Tutorial for Beginners",
            "url": "https://www.youtube.com/watch?v=YS4e4q9oBaU",
            "type": "video",
        }
    ],
    "Rust": [
        {
            "title": "Rust Programming Course for Beginners",
            "url": "https://www.youtube.com/watch?v=BpPEoZW5IiY",
            "type": "video",
        }
    ],
    "PHP": [
        {
            "title": "PHP Tutorial for Beginners",
            "url": "https://www.youtube.com/watch?v=OK_JCtrrv-c",
            "type": "video",
        }
    ],
    "Ruby": [
        {
            "title": "Ruby Programming Language - Full Course",
            "url": "https://www.youtube.com/watch?v=t_ispmWmdjY",
            "type": "video",
        }
    ],
    "HTML": [
        {
            "title": "HTML Full Course - Build a Website Tutorial",
            "url": "https://www.youtube.com/watch?v=pQN-pnXPaVg",
            "type": "video",
        }
    ],
    "CSS": [
        {
            "title": "CSS Tutorial - Zero to Hero",
            "url": "https://www.youtube.com/watch?v=1Rs2ND1ryYc",
            "type": "video",
        }
    ],
    "React": [
        {
            "title": "React Course - Beginner's Tutorial for React JavaScript Library",
            "url": "https://www.youtube.com/watch?v=bMknfKXIFA8",
            "type": "video",
        }
    ],
    "Angular": [
        {
            "title": "Angular Full Course for Beginners",
            "url": "https://www.youtube.com/watch?v=3qBXWUpoPHo",
            "type": "video",
        }
    ],
    "Vue.js": [
        {
            "title": "Vue.js Course for Beginners",
            "url": "https://www.youtube.com/watch?v=FXpIoQ_rT_c",
            "type": "video",
        }
    ],
    "Next.js": [
        {
            "title": "Next.js Full Course for Beginners",
            "url": "https://www.youtube.com/watch?v=wm5gMKuwSYk",
            "type": "video",
        }
    ],
    "Node.js": [
        {
            "title": "Node.js and Express.js - Full Course",
            "url": "https://www.youtube.com/watch?v=Oe421EPjeBE",
            "type": "video",
        }
    ],
    "Express.js": [
        {
            "title": "Express.js Crash Course",
            "url": "https://www.youtube.com/watch?v=L72fhGm1tfE",
            "type": "video",
        }
    ],
    "FastAPI": [
        {
            "title": "FastAPI Course for Beginners",
            "url": "https://www.youtube.com/watch?v=0sOvCWFmrtA",
            "type": "video",
        }
    ],
    "Django": [
        {
            "title": "Django Course for Beginners",
            "url": "https://www.youtube.com/watch?v=F5mRW0jo-U4",
            "type": "video",
        }
    ],
    "Flask": [
        {
            "title": "Flask Course - Python Web Application Development",
            "url": "https://www.youtube.com/watch?v=Z1RJmh_OqeA",
            "type": "video",
        }
    ],
    "Spring Boot": [
        {
            "title": "Spring Boot Tutorial for Beginners",
            "url": "https://www.youtube.com/watch?v=9SGDpanrc8U",
            "type": "video",
        }
    ],
    "SQL": [
        {
            "title": "SQL Tutorial - Full Database Course for Beginners",
            "url": "https://www.youtube.com/watch?v=HXV3zeQKqGY",
            "type": "video",
        }
    ],
    "SQLite": [
        {
            "title": "SQLite Tutorial for Beginners",
            "url": "https://www.youtube.com/watch?v=byHcYRpMgI4",
            "type": "video",
        }
    ],
    "PostgreSQL": [
        {
            "title": "PostgreSQL Tutorial Full Course",
            "url": "https://www.youtube.com/watch?v=SpfIwlAYaKk",
            "type": "video",
        }
    ],
    "MySQL": [
        {
            "title": "MySQL Full Course for Beginners",
            "url": "https://www.youtube.com/watch?v=7S_tz1z_5bA",
            "type": "video",
        }
    ],
    "MongoDB": [
        {
            "title": "MongoDB Tutorial for Beginners",
            "url": "https://www.youtube.com/watch?v=ofme2o29ngU",
            "type": "video",
        }
    ],
    "Git": [
        {
            "title": "Git and GitHub for Beginners - Crash Course",
            "url": "https://www.youtube.com/watch?v=RGOj5yH7evk",
            "type": "video",
        }
    ],
    "REST API": [
        {
            "title": "REST API Concepts and Examples",
            "url": "https://www.youtube.com/watch?v=lsMQRaeKNDk",
            "type": "video",
        }
    ],
    "GraphQL": [
        {
            "title": "GraphQL Course for Beginners",
            "url": "https://www.youtube.com/watch?v=ed8SzALpx1Q",
            "type": "video",
        }
    ],
    "Docker": [
        {
            "title": "Docker Tutorial for Beginners",
            "url": "https://www.youtube.com/watch?v=fqMOX6JJhGo",
            "type": "video",
        }
    ],
    "Kubernetes": [
        {
            "title": "Kubernetes Course for Beginners",
            "url": "https://www.youtube.com/watch?v=X48VuDVv0do",
            "type": "video",
        }
    ],
    "AWS": [
        {
            "title": "AWS Cloud Practitioner Full Course",
            "url": "https://www.youtube.com/watch?v=SOTamWNgDKc",
            "type": "video",
        }
    ],
    "GCP": [
        {
            "title": "Google Cloud Platform Full Course",
            "url": "https://www.youtube.com/watch?v=jpno8FSqpc8",
            "type": "video",
        }
    ],
    "Azure": [
        {
            "title": "Microsoft Azure Fundamentals Full Course",
            "url": "https://www.youtube.com/watch?v=NKEFWyqJ5XA",
            "type": "video",
        }
    ],
    "DevOps": [
        {
            "title": "DevOps Engineering Course for Beginners",
            "url": "https://www.youtube.com/watch?v=j5Zsa_eOXeY",
            "type": "video",
        }
    ],
    "CI/CD": [
        {
            "title": "CI/CD Explained",
            "url": "https://www.youtube.com/watch?v=1er2cjUq1UI",
            "type": "video",
        }
    ],
    "Linux": [
        {
            "title": "Linux for Beginners",
            "url": "https://www.youtube.com/watch?v=sWbUDq4S6Y8",
            "type": "video",
        }
    ],
    "Machine Learning": [
        {
            "title": "Machine Learning Course for Beginners",
            "url": "https://www.youtube.com/watch?v=i_LwzRVP7bg",
            "type": "video",
        }
    ],
    "Deep Learning": [
        {
            "title": "Deep Learning Full Course",
            "url": "https://www.youtube.com/watch?v=aircAruvnKk",
            "type": "video",
        }
    ],
    "NLP": [
        {
            "title": "Natural Language Processing Full Course",
            "url": "https://www.youtube.com/watch?v=fOvTtapxa9c",
            "type": "video",
        }
    ],
    "Computer Vision": [
        {
            "title": "Computer Vision Full Course",
            "url": "https://www.youtube.com/watch?v=IA3WxTTPXqQ",
            "type": "video",
        }
    ],
    "Cybersecurity": [
        {
            "title": "Cyber Security Full Course for Beginners",
            "url": "https://www.youtube.com/watch?v=U_P23SqJaDc",
            "type": "video",
        }
    ],
    "Testing": [
        {
            "title": "Software Testing Tutorial for Beginners",
            "url": "https://www.youtube.com/watch?v=uz5PvLkGpyw",
            "type": "video",
        }
    ],
    "Microservices": [
        {
            "title": "Microservices Explained",
            "url": "https://www.youtube.com/watch?v=rv4LlmLmVWk",
            "type": "video",
        }
    ],
    "Distributed Systems": [
        {
            "title": "Distributed Systems in One Lesson",
            "url": "https://www.youtube.com/watch?v=Y6Ev8GIlbxc",
            "type": "video",
        }
    ],
}


def _subtopics_for_skill(skill_name: str, mode: str = "foundation", stage_index: int = 1) -> list[str]:
    canonical = _canonical_skill_label(skill_name)

    specific = {
        "Python": {
            "foundation": [
                [
                    "Python data types and mutability",
                    "Control flow and comprehensions",
                    "Functions, scope, and return values",
                ],
                [
                    "Modules and package imports",
                    "Exception handling patterns",
                    "File I/O with context managers",
                ],
                [
                    "Iterators and generators",
                    "Object-oriented design basics",
                    "Testing with pytest assertions",
                ],
            ],
            "gap": [
                [
                    "Decorators and context managers",
                    "Async functions and await patterns",
                    "Type hints and static analysis",
                ],
                [
                    "Concurrency: threading vs multiprocessing",
                    "Profiling and performance bottlenecks",
                    "Advanced error handling and retries",
                ],
                [
                    "Interview coding patterns in Python",
                    "Code readability and refactoring drills",
                    "Debugging edge cases under time pressure",
                ],
            ],
        },
        "FastAPI": {
            "foundation": [
                [
                    "Path operations and request validation",
                    "Pydantic models and response schemas",
                    "HTTP status codes and error responses",
                ],
                [
                    "Dependency injection basics",
                    "Route organization and APIRouter",
                    "Request/response middleware flow",
                ],
                [
                    "SQLModel/ORM integration basics",
                    "CRUD endpoint structure",
                    "Testing endpoints with TestClient",
                ],
            ],
            "gap": [
                [
                    "Background tasks and async endpoints",
                    "Auth dependencies and token validation",
                    "Database sessions and transactions",
                ],
                [
                    "Pagination, filtering, and sorting APIs",
                    "Rate limiting and API hardening",
                    "Observability: logging and tracing hooks",
                ],
                [
                    "Designing interview-ready API architecture",
                    "Trade-offs in sync vs async handlers",
                    "Failure-mode and resilience patterns",
                ],
            ],
        },
        "SQL": {
            "foundation": [
                [
                    "SELECT, WHERE, GROUP BY, ORDER BY",
                    "INNER/LEFT joins and relationship mapping",
                    "Aggregate functions and HAVING",
                ],
                [
                    "CASE statements and conditional logic",
                    "CTEs for readable query structure",
                    "Subqueries and correlated subqueries",
                ],
                [
                    "Schema design and normalization basics",
                    "Indexes and execution plan reading",
                    "Transaction boundaries and rollback",
                ],
            ],
            "gap": [
                [
                    "Window functions with PARTITION BY",
                    "Advanced join strategies",
                    "Query optimization heuristics",
                ],
                [
                    "Isolation levels and locking behavior",
                    "Data consistency patterns",
                    "Performance tuning with real datasets",
                ],
                [
                    "Interview SQL problem decomposition",
                    "Explaining query complexity clearly",
                    "Debugging incorrect query results fast",
                ],
            ],
        },
        "React": {
            "foundation": [
                [
                    "JSX and component composition",
                    "Props/state flow",
                    "Event handling patterns",
                ],
                [
                    "useEffect dependency management",
                    "Component state normalization",
                    "Form handling and validation",
                ],
                [
                    "Routing and page-level state",
                    "API fetching and loading/error states",
                    "Component testing with React Testing Library",
                ],
            ],
            "gap": [
                [
                    "Memoization and render optimization",
                    "State architecture and custom hooks",
                    "Error boundaries and resilient UI patterns",
                ],
                [
                    "Performance profiling with React DevTools",
                    "Data caching and stale state handling",
                    "Accessibility and semantic UI checks",
                ],
                [
                    "Frontend interview question walkthroughs",
                    "Trade-offs in state management choices",
                    "Refactoring a feature for maintainability",
                ],
            ],
        },
    }

    stage = max(1, _safe_int(stage_index, 1))

    if canonical in specific:
        tracks = specific[canonical]["gap"] if mode == "gap" else specific[canonical]["foundation"]
        return tracks[min(stage - 1, len(tracks) - 1)]

    if mode == "gap":
        fallback_tracks = [
            [
                f"Intermediate workflows in {canonical}",
                f"Debugging common failures in {canonical}",
                f"Performance basics in {canonical}",
            ],
            [
                f"Advanced architecture choices in {canonical}",
                f"Scalability and maintainability in {canonical}",
                f"Testing strategy for {canonical}",
            ],
            [
                f"Interview scenarios using {canonical}",
                f"Trade-off discussion for {canonical}",
                f"Rapid problem solving with {canonical}",
            ],
        ]
        return fallback_tracks[min(stage - 1, len(fallback_tracks) - 1)]

    fallback_tracks = [
        [
            f"Fundamentals and terminology in {canonical}",
            f"Setup and first working example in {canonical}",
            f"Core workflow in {canonical}",
        ],
        [
            f"Applied exercises in {canonical}",
            f"Error handling patterns in {canonical}",
            f"Reusable patterns in {canonical}",
        ],
        [
            f"Real-world mini project with {canonical}",
            f"Optimization and maintainability in {canonical}",
            f"Interview explanation practice for {canonical}",
        ],
    ]
    return fallback_tracks[min(stage - 1, len(fallback_tracks) - 1)]


def _study_mode_for_occurrence(skill: dict, occurrence_index: int) -> str:
    category = str(skill.get("category", "")).lower()
    if category == "present":
        return "gap"
    if occurrence_index <= 2:
        return "foundation"
    return "gap"


def _topic_label_for_progression(skill_name: str, mode: str, occurrence_index: int) -> str:
    canonical = _canonical_skill_label(skill_name)
    if mode == "foundation":
        labels = ["Core Fundamentals", "Applied Basics", "Hands-on Build"]
    else:
        labels = ["Gap Closure", "Advanced Patterns", "Interview Drill"]
    base = labels[min(max(1, occurrence_index) - 1, len(labels) - 1)]
    return f"{canonical} - {base}"


def _ensure_progressive_subtopics(
    skill_name: str,
    mode: str,
    occurrence_index: int,
    candidate_subtopics: list[str],
    used_subtopics: set[str],
) -> list[str]:
    cleaned = []
    seen_local = set()
    for subtopic in candidate_subtopics:
        value = " ".join(str(subtopic).strip().split())
        if not _study_subtopic_is_specific(value):
            continue
        key = value.lower()
        if key in seen_local or key in used_subtopics:
            continue
        seen_local.add(key)
        cleaned.append(value)
        if len(cleaned) >= 5:
            break

    progression_bank = _subtopics_for_skill(skill_name, mode=mode, stage_index=occurrence_index)
    for subtopic in progression_bank:
        value = " ".join(str(subtopic).strip().split())
        key = value.lower()
        if key in seen_local or key in used_subtopics:
            continue
        seen_local.add(key)
        cleaned.append(value)
        if len(cleaned) >= 5:
            break

    if len(cleaned) < 3:
        next_bank = _subtopics_for_skill(skill_name, mode=mode, stage_index=occurrence_index + 1)
        for subtopic in next_bank:
            value = " ".join(str(subtopic).strip().split())
            key = value.lower()
            if key in seen_local or key in used_subtopics:
                continue
            seen_local.add(key)
            cleaned.append(value)
            if len(cleaned) >= 3:
                break

    for item in cleaned:
        used_subtopics.add(item.lower())

    return cleaned


def _safe_int(value, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _safe_float(value, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _parse_json_object_from_model_text(raw_text: str) -> dict | None:
    cleaned = (raw_text or "").strip().replace("```json", "").replace("```", "").strip()
    if not cleaned:
        return None

    try:
        parsed = json.loads(cleaned)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        pass

    # Recover from model pre/post text by extracting the outer JSON object.
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None

    candidate = cleaned[start : end + 1]
    try:
        parsed = json.loads(candidate)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None


def _classify_ai_generation_error(exc: Exception) -> str:
    message = str(exc).lower()
    if "429" in message or "quota" in message or "resourceexhausted" in message:
        return "ai_unavailable:quota_exceeded"
    if "404" in message or "not found" in message:
        return "ai_unavailable:model_not_found"
    if "timeout" in message or "deadline" in message:
        return "ai_unavailable:timeout"
    return "ai_unavailable:model_unavailable"


GENERIC_STUDY_SUBTOPIC_PATTERNS = [
    r"^core concepts of\b",
    r"^hands-on basics in\b",
    r"^common mistakes and best practices in\b",
    r"^advanced patterns in\b",
    r"^performance and debugging in\b",
    r"^interview-style problem solving with\b",
    r"^foundation:?\b",
    r"^practice:?\b",
    r"^gap fill:?\b",
]


GENERIC_STUDY_TOPIC_PATTERNS = [
    r"^foundation:?\b",
    r"^practice:?\b",
    r"^gap fill:?\b",
    r"^study\b",
    r"^review\b",
]


def _study_subtopic_is_specific(value: str) -> bool:
    normalized = " ".join(str(value).strip().split())
    if len(normalized) < 4:
        return False

    lowered = normalized.lower()
    for pattern in GENERIC_STUDY_SUBTOPIC_PATTERNS:
        if re.match(pattern, lowered):
            return False

    return True


def _normalize_topic_name_key(value: str) -> str:
    normalized = " ".join(str(value).strip().lower().split())
    normalized = re.sub(r"[^a-z0-9\s]", "", normalized)
    return normalized


def _study_topic_name_is_specific(value: str) -> bool:
    normalized = " ".join(str(value).strip().split())
    if len(normalized) < 4:
        return False

    lowered = normalized.lower()
    for pattern in GENERIC_STUDY_TOPIC_PATTERNS:
        if re.match(pattern, lowered):
            return False

    return True


def _resource_url_is_generic_search(url: str) -> bool:
    lowered = str(url).strip().lower()
    generic_search_signals = [
        "youtube.com/results",
        "google.com/search",
        "bing.com/search",
        "search.yahoo.com/search",
        "freecodecamp.org/news/search",
    ]
    return any(signal in lowered for signal in generic_search_signals)


def _study_plan_uses_day_numbers(total_days: int) -> bool:
    return total_days <= 7


def _study_plan_target_entry_count(total_days: int) -> int:
    if _study_plan_uses_day_numbers(total_days):
        return max(1, total_days)
    return MAX_STUDY_PLAN_ENTRIES


def _supplement_topic_resources(skill_name: str, resources: list[dict], seen_resource_urls: set[str]) -> list[dict]:
    cleaned_resources = list(resources)
    for resource in _free_resources_for_skill(skill_name):
        title = " ".join(str(resource.get("title", "")).strip().split())
        url = " ".join(str(resource.get("url", "")).strip().split())
        rtype = " ".join(str(resource.get("type", "resource")).strip().split()) or "resource"
        if not title or not url or not url.startswith("http"):
            continue
        if _resource_url_is_generic_search(url):
            continue
        url_key = url.lower()
        if url_key in seen_resource_urls:
            continue
        seen_resource_urls.add(url_key)
        cleaned_resources.append({"title": title, "url": url, "type": rtype})
        if len(cleaned_resources) >= 3:
            break

    has_video = any(str(item.get("type", "")).lower() == "video" or "youtube.com" in str(item.get("url", "")).lower() for item in cleaned_resources)
    if not has_video:
        canonical = _canonical_skill_label(skill_name)
        video_candidates = VIDEO_RESOURCES_BY_SKILL.get(canonical, [])

        for resource in video_candidates:
            title = " ".join(str(resource.get("title", "")).strip().split())
            url = " ".join(str(resource.get("url", "")).strip().split())
            if not title or not url or not url.startswith("http"):
                continue
            url_key = url.lower()
            if url_key in seen_resource_urls:
                continue
            seen_resource_urls.add(url_key)
            cleaned_resources.append({"title": title, "url": url, "type": "video"})
            has_video = True
            break

    if not has_video:
        canonical = _canonical_skill_label(skill_name)
        query = canonical.replace(" ", "+")
        url = f"https://www.youtube.com/results?search_query={query}+tutorial"
        url_key = url.lower()
        if url_key not in seen_resource_urls:
            seen_resource_urls.add(url_key)
            cleaned_resources.append(
                {
                    "title": f"YouTube: {canonical} tutorial",
                    "url": url,
                    "type": "video",
                }
            )
    return cleaned_resources


def _build_suggested_videos(topics: list[dict], skills: list[dict]) -> list[dict]:
    suggestions = []
    seen_urls = set()

    for topic in topics:
        for resource in topic.get("resources", []):
            url = str(resource.get("url", "")).strip()
            if "youtube.com" not in url.lower() and "youtu.be" not in url.lower():
                continue
            key = url.lower()
            if key in seen_urls:
                continue
            seen_urls.add(key)
            suggestions.append(
                {
                    "skill_name": topic["skill_name"],
                    "title": resource.get("title", f"{topic['skill_name']} video"),
                    "url": url,
                    "type": "video",
                }
            )

    for skill in skills:
        canonical = _canonical_skill_label(skill["skill_name"])
        curated_resources = VIDEO_SUGGESTIONS_BY_SKILL.get(canonical, [])
        if not curated_resources:
            curated_resources = VIDEO_RESOURCES_BY_SKILL.get(canonical, [])
        for resource in curated_resources:
            url = str(resource["url"]).strip()
            key = url.lower()
            if key in seen_urls:
                continue
            seen_urls.add(key)
            suggestions.append(
                {
                    "skill_name": canonical,
                    "title": resource["title"],
                    "url": url,
                    "type": resource.get("type", "video"),
                }
            )

    return suggestions[:4]


def _study_weight_for_skill(skill: dict) -> float:
    category = str(skill.get("category", "")).lower()
    proficiency = skill.get("proficiency_score")
    if proficiency is None:
        proficiency = 0.5
    proficiency = max(0.0, min(1.0, _safe_float(proficiency, 0.5)))
    gap = 1.0 - proficiency

    # Priority weight is already derived from proficiency in Module 3, but may be null.
    raw_priority = skill.get("priority_weight")
    if raw_priority is None:
        raw_priority = round(1.0 + gap * 4.0, 2)
    priority_norm = max(0.0, min(1.0, (_safe_float(raw_priority, 1.0) - 1.0) / 4.0))

    if category == "lacking":
        return 1.2

    # Present skills: weaker scores and higher priority weights get more time share.
    return round(0.55 + (gap * 1.1) + (priority_norm * 0.85), 3)


def _estimated_hours_for_skill_topic(skill: dict, occurrence_index: int = 1) -> float:
    category = str(skill.get("category", "")).lower()
    proficiency = skill.get("proficiency_score")
    raw_priority = skill.get("priority_weight")
    priority_norm = max(0.0, min(1.0, (_safe_float(raw_priority, 1.0) - 1.0) / 4.0))

    if category == "present":
        if proficiency is None:
            proficiency = 0.5
        proficiency = max(0.0, min(1.0, _safe_float(proficiency, 0.5)))
        gap = 1.0 - proficiency
        hours = 1.0 + (gap * 3.2) + (priority_norm * 0.8)
    else:
        hours = 2.6 + (priority_norm * 0.6)

    if occurrence_index > 1:
        hours -= min(0.6, (occurrence_index - 1) * 0.2)

    return round(max(1.0, min(5.0, hours)), 2)


def _rebalance_topics_for_time_and_weight(topics: list[dict], skills: list[dict], total_days: int) -> list[dict]:
    if not topics:
        return topics

    skill_map = {int(s["id"]): s for s in skills}
    max_topics = _study_plan_target_entry_count(total_days)
    use_day_numbers = _study_plan_uses_day_numbers(total_days)

    # Attach score-driven rank hints.
    ranked = []
    for idx, topic in enumerate(topics):
        skill = skill_map.get(int(topic["skill_id"]))
        weight = _study_weight_for_skill(skill) if skill else 1.0
        category = str((skill or {}).get("category", "")).lower()
        boost = 1.0
        if category == "present":
            boost = 1.15
        ranked.append((weight * boost, idx, topic))

    ranked.sort(key=lambda x: x[0], reverse=True)

    # Keep strongest weighted topics, then restore original order to preserve progression.
    trimmed_with_index = ranked[:max_topics]
    trimmed_with_index.sort(key=lambda x: x[1])
    trimmed = [item[2] for item in trimmed_with_index]

    ordered = list(trimmed)

    skill_occurrences = {}
    for idx, topic in enumerate(ordered, start=1):
        skill_id = int(topic["skill_id"])
        skill_occurrences[skill_id] = skill_occurrences.get(skill_id, 0) + 1
        topic["estimated_hours"] = _estimated_hours_for_skill_topic(
            skill_map.get(skill_id, {}),
            occurrence_index=skill_occurrences[skill_id],
        )
        topic["day_number"] = idx if use_day_numbers else idx

    return ordered


def _fallback_generate_study_topics(skills: list[dict], total_days: int) -> list[dict]:
    lacking = [s for s in skills if s["category"] == "lacking"]
    present = [s for s in skills if s["category"] == "present"]

    present_sorted = sorted(
        present,
        key=lambda x: x["proficiency_score"] if x["proficiency_score"] is not None else 0.5,
    )

    ordered_skills = [*lacking, *present_sorted]
    if not ordered_skills:
        return []

    required_topics = _study_plan_target_entry_count(total_days)
    scheduled = []
    occurrence_by_skill = {}
    used_subtopics_by_skill = {}

    for idx in range(required_topics):
        skill = ordered_skills[idx % len(ordered_skills)]
        skill_id = int(skill["id"])
        occurrence = occurrence_by_skill.get(skill_id, 0) + 1
        occurrence_by_skill[skill_id] = occurrence

        mode = _study_mode_for_occurrence(skill, occurrence)
        used_subtopics = used_subtopics_by_skill.setdefault(skill_id, set())
        subtopics = _ensure_progressive_subtopics(
            skill_name=skill["skill_name"],
            mode=mode,
            occurrence_index=occurrence,
            candidate_subtopics=[],
            used_subtopics=used_subtopics,
        )

        topic_name = _topic_label_for_progression(skill["skill_name"], mode, occurrence)
        description = (
            f"Build stronger {skill['skill_name']} depth with targeted practice for this stage."
            if mode == "gap"
            else f"Establish {skill['skill_name']} fundamentals and apply them in short exercises."
        )

        scheduled.append(
            {
                "day_number": idx + 1,
                "skill_id": skill_id,
                "skill_name": skill["skill_name"],
                "topic_name": topic_name,
                "description": description,
                "subtopics": subtopics,
                "estimated_hours": _estimated_hours_for_skill_topic(skill, occurrence_index=occurrence),
                "resources": _free_resources_for_skill(skill["skill_name"]),
            }
        )

    return scheduled


def _normalize_study_topics_from_ai(raw_topics: list[dict], skills: list[dict], total_days: int) -> list[dict]:
    if not isinstance(raw_topics, list):
        return []

    skill_name_map = {_canonical_skill_label(s["skill_name"]).lower(): s for s in skills}
    normalized = []
    seen_resource_urls = set()
    seen_topic_name_keys = set()
    seen_subtopic_signatures_by_skill = {}
    seen_subtopic_values_by_skill = {}
    skill_occurrence_count = {}
    use_day_numbers = _study_plan_uses_day_numbers(total_days)
    target_entry_count = _study_plan_target_entry_count(total_days)

    for topic in raw_topics:
        if not isinstance(topic, dict):
            continue

        day_number = _safe_int(topic.get("day_number", len(normalized) + 1), len(normalized) + 1)
        day_number = max(1, min(target_entry_count, day_number))

        skill_name = _clean_skill_value(topic.get("skill_name", ""))
        if not skill_name:
            continue

        skill_key = _canonical_skill_label(skill_name).lower()
        matched_skill = skill_name_map.get(skill_key)
        if not matched_skill:
            continue

        topic_name = _clean_skill_value(topic.get("topic_name", ""))
        if not topic_name:
            continue
        if not _study_topic_name_is_specific(topic_name):
            continue
        topic_name_key = _normalize_topic_name_key(topic_name)
        if not topic_name_key:
            continue

        description = " ".join(str(topic.get("description", "")).strip().split())
        if not description:
            description = f"Study {matched_skill['skill_name']} with interview-focused exercises."

        raw_subtopics = topic.get("subtopics", [])
        if isinstance(raw_subtopics, str):
            raw_subtopics = [part.strip() for part in re.split(r"[,;\n]", raw_subtopics) if part.strip()]
        skill_id = int(matched_skill["id"])
        occurrence_index = skill_occurrence_count.get(skill_id, 0) + 1
        mode = _study_mode_for_occurrence(matched_skill, occurrence_index)
        used_subtopics = seen_subtopic_values_by_skill.setdefault(skill_id, set())
        cleaned_subtopics = _ensure_progressive_subtopics(
            skill_name=matched_skill["skill_name"],
            mode=mode,
            occurrence_index=occurrence_index,
            candidate_subtopics=list(raw_subtopics)[:6],
            used_subtopics=used_subtopics,
        )

        if len(cleaned_subtopics) < 3:
            continue

        subtopic_signature = tuple(sorted(item.lower() for item in cleaned_subtopics))
        prior_signatures = seen_subtopic_signatures_by_skill.setdefault(skill_id, [])
        if subtopic_signature in prior_signatures:
            continue

        skip_for_high_overlap = False
        subtopic_set = set(subtopic_signature)
        for prior_signature in prior_signatures:
            prior_set = set(prior_signature)
            overlap = len(subtopic_set & prior_set)
            overlap_ratio = overlap / max(1, min(len(subtopic_set), len(prior_set)))
            if overlap_ratio >= 0.67:
                skip_for_high_overlap = True
                break
        if skip_for_high_overlap:
            continue

        if topic_name_key in seen_topic_name_keys:
            topic_name = f"{topic_name} (Level {occurrence_index})"
            topic_name_key = _normalize_topic_name_key(topic_name)
            if not topic_name_key or topic_name_key in seen_topic_name_keys:
                topic_name = _topic_label_for_progression(matched_skill["skill_name"], mode, occurrence_index)
                topic_name_key = _normalize_topic_name_key(topic_name)
                if not topic_name_key or topic_name_key in seen_topic_name_keys:
                    topic_name = f"{matched_skill['skill_name']} Study Block {occurrence_index}"
                    topic_name_key = _normalize_topic_name_key(topic_name)
                    if not topic_name_key or topic_name_key in seen_topic_name_keys:
                        continue

        estimated_hours = _safe_float(topic.get("estimated_hours", 1.5), 1.5)
        estimated_hours = max(0.5, min(6.0, estimated_hours))

        resources = topic.get("resources", [])
        if isinstance(resources, dict):
            resources = [resources]
        if not isinstance(resources, list):
            resources = []
        cleaned_resources = []
        for resource in resources[:4]:
            if not isinstance(resource, dict):
                continue
            title = " ".join(str(resource.get("title", "")).strip().split())
            url = " ".join(str(resource.get("url", "")).strip().split())
            rtype = " ".join(str(resource.get("type", "resource")).strip().split())
            if not title or not url or not url.startswith("http"):
                continue
            if _resource_url_is_generic_search(url):
                continue
            url_key = url.lower()
            if url_key in seen_resource_urls:
                continue
            seen_resource_urls.add(url_key)
            cleaned_resources.append({"title": title, "url": url, "type": rtype or "resource"})

        cleaned_resources = _supplement_topic_resources(matched_skill["skill_name"], cleaned_resources, seen_resource_urls)

        seen_topic_name_keys.add(topic_name_key)
        prior_signatures.append(subtopic_signature)
        skill_occurrence_count[skill_id] = occurrence_index

        normalized.append(
            {
                "day_number": day_number,
                "skill_id": matched_skill["id"],
                "skill_name": matched_skill["skill_name"],
                "topic_name": topic_name,
                "description": description,
                "subtopics": cleaned_subtopics,
                "estimated_hours": estimated_hours,
                "resources": cleaned_resources,
            }
        )

    if not normalized:
        return []

    min_topics_required = _study_plan_target_entry_count(total_days)
    if len(normalized) < min_topics_required:
        return []

    normalized.sort(key=lambda x: (x["day_number"], x["skill_name"], x["topic_name"]))
    if not use_day_numbers:
        for idx, topic in enumerate(normalized, start=1):
            topic["day_number"] = idx
    return _rebalance_topics_for_time_and_weight(normalized, skills, total_days)


def _generate_study_topics_with_gemini(skills: list[dict], total_days: int) -> tuple[list[dict] | None, str]:
    if not GEMINI_API_KEY:
        return None, "ai_unavailable:no_api_key"

    skills_for_prompt = [
        {
            "skill_name": s["skill_name"],
            "category": s["category"],
            "proficiency_score": s["proficiency_score"],
            "priority_weight": s["priority_weight"],
        }
        for s in skills
    ]
    use_day_numbers = _study_plan_uses_day_numbers(total_days)
    target_topic_count = _study_plan_target_entry_count(total_days)
    numbering_instruction = (
        f"- Use day_number values 1 through {target_topic_count}; create exactly one topic for each day."
        if use_day_numbers
        else "- Use day_number values 1 through 7 only as internal order. Do not phrase the plan as Day 1, Day 2, etc. Make each topic a study block or focus area."
    )

    prompt = f"""
You are creating a practical interview prep study plan.

Constraints:
- total_days: {total_days}
- generate exactly {target_topic_count} topic objects
- create one topic entry per item, not multiple entries per day
{numbering_instruction}
- Every topic_name must be unique across the whole plan.
- Use lacking skills for foundational learning based on available time.
- Use present skills for targeted weak-gap improvement based on proficiency_score and priority_weight.
- Shape the sequence by time left: early entries for foundations, middle entries for applied work, final entries for interview revision, weak-area drills, and mock-style practice.
- If the same skill appears more than once, later entries must clearly advance beyond earlier ones. Do not repeat the same topic_name or nearly the same subtopics for that skill.
- For every topic include 3-5 concrete subtopics to study that exact day.
- Subtopics must be specific concept names like "Data types", "Primary keys", "Window functions", "CASE statements", "Joins", "CTEs", "Indexes", "Transactions", "Async functions", or "Decorators", depending on the skill.
- Do not output vague filler like "Core concepts of X", "Hands-on basics in X", "Common mistakes", "Advanced patterns", or "Best practices".
- Do not reuse the same 2 or 3 subtopics across multiple entries for the same skill.
- Set estimated_hours based on weakness: weaker assessed skills should get more time, stronger skills less time, and each entry must stay between 1 and 5 hours.
- Each topic must include 1-3 free resources directly relevant to that day's subtopics.
- Use direct links to docs, tutorials, articles, videos, or exercises.
- Do not use generic search result pages.
- Do not repeat the same URL across different days unless absolutely unavoidable.
- Make resources differ across days so the learner is not seeing the same links repeated.
- Output strict JSON only.

Skills:
{json.dumps(skills_for_prompt)}

Return JSON format:
{{
  "topics": [
    {{
      "day_number": 1,
      "skill_name": "Python",
      "topic_name": "Async API patterns",
      "description": "What to study and practice in short actionable terms.",
            "subtopics": ["Async functions", "Event loop basics", "Await patterns"],
      "estimated_hours": 1.5,
      "resources": [
        {{"title": "Resource title", "url": "https://...", "type": "docs"}}
      ]
    }}
  ]
}}
"""

    model_candidates = _get_model_candidates()
    deadline = time.monotonic() + 16
    last_reason = "ai_unavailable:model_unavailable"

    for model_name in model_candidates:
        if time.monotonic() >= deadline:
            last_reason = "ai_unavailable:timeout"
            break
        try:
            response = _generate_gemini_content(model_name, prompt, timeout_seconds=10)
            parsed = _parse_json_object_from_model_text(response.text or "")
            if not parsed:
                continue
            raw_topics = parsed.get("topics", [])
            normalized = _normalize_study_topics_from_ai(raw_topics, skills, total_days)
            if normalized:
                return normalized, f"ai:{model_name}"
        except Exception as exc:
            last_reason = _classify_ai_generation_error(exc)
            continue

    return None, last_reason


def _save_study_plan(session_id: int, total_days: int, topics: list[dict], generation_engine: str) -> int:
    conn = get_connection()
    try:
        cursor = conn.cursor()

        cursor.execute("SELECT id FROM study_plans WHERE session_id = ?", (session_id,))
        existing = cursor.fetchone()
        if existing:
            plan_id = existing["id"]
            cursor.execute("DELETE FROM study_topics WHERE plan_id = ?", (plan_id,))
            cursor.execute(
                "UPDATE study_plans SET total_days = ?, created_at = CURRENT_TIMESTAMP WHERE id = ?",
                (total_days, plan_id),
            )
        else:
            cursor.execute(
                "INSERT INTO study_plans (session_id, total_days) VALUES (?, ?)",
                (session_id, total_days),
            )
            plan_id = cursor.lastrowid

        for topic in topics:
            cursor.execute(
                """
                INSERT INTO study_topics (
                    plan_id,
                    skill_id,
                    topic_name,
                    description,
                    subtopics,
                    resources,
                    estimated_hours,
                    day_number,
                    is_completed
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
                """,
                (
                    plan_id,
                    topic["skill_id"],
                    topic["topic_name"],
                    topic["description"],
                    json.dumps(topic.get("subtopics", [])),
                    json.dumps(topic["resources"]),
                    topic["estimated_hours"],
                    topic["day_number"],
                ),
            )

        cursor.execute(
            "UPDATE sessions SET status = ? WHERE id = ?",
            ("plan_generated", session_id),
        )
        conn.commit()
        return plan_id
    finally:
        conn.close()


def _study_plan_response(session_id: int) -> dict:
    session = _get_session_by_id(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="Session not found.")

    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, total_days, created_at FROM study_plans WHERE session_id = ?",
            (session_id,),
        )
        plan = cursor.fetchone()
        if plan is None:
            raise HTTPException(status_code=404, detail="Study plan not found.")

        cursor.execute(
            """
            SELECT id AS skill_id, skill_name, category, proficiency_score, priority_weight
            FROM skills
            WHERE session_id = ?
            ORDER BY category ASC, priority_weight DESC, skill_name ASC
            """,
            (session_id,),
        )
        session_skill_rows = [dict(row) for row in cursor.fetchall()]

        cursor.execute(
            """
            SELECT
                st.id,
                st.skill_id,
                s.skill_name,
                s.category,
                s.proficiency_score,
                s.priority_weight,
                st.topic_name,
                st.description,
                st.subtopics,
                st.resources,
                st.estimated_hours,
                st.day_number,
                st.is_completed
            FROM study_topics st
            JOIN skills s ON s.id = st.skill_id
            WHERE st.plan_id = ?
            ORDER BY st.day_number ASC, st.id ASC
            """,
            (plan["id"],),
        )
        topic_rows = cursor.fetchall()
    finally:
        conn.close()

    topics = []
    completed_count = 0
    for row in topic_rows:
        resources = []
        subtopics = []
        try:
            resources = json.loads(row["resources"] or "[]")
        except Exception:
            resources = []
        try:
            subtopics = json.loads(row["subtopics"] or "[]")
        except Exception:
            subtopics = []

        item = {
            "id": row["id"],
            "skill_id": row["skill_id"],
            "skill_name": row["skill_name"],
            "category": row["category"],
            "proficiency_score": row["proficiency_score"],
            "priority_weight": row["priority_weight"],
            "topic_name": row["topic_name"],
            "description": row["description"],
            "subtopics": subtopics,
            "resources": resources,
            "estimated_hours": row["estimated_hours"],
            "day_number": row["day_number"],
            "is_completed": bool(row["is_completed"]),
        }
        topics.append(item)
        if item["is_completed"]:
            completed_count += 1

    days = {}
    for topic in topics:
        day_number = topic["day_number"]
        days.setdefault(day_number, []).append(topic)

    day_groups = [
        {"day_number": day, "topics": days[day]} for day in sorted(days.keys())
    ]

    skill_map = {int(skill["skill_id"]): skill for skill in session_skill_rows}
    present = [s for s in skill_map.values() if s["category"] == "present"]
    lacking = [s for s in skill_map.values() if s["category"] == "lacking"]

    total_topics = len(topics)
    progress_percent = 0 if total_topics == 0 else round((completed_count / total_topics) * 100, 2)
    days_remaining = _days_until(session["interview_date"]) if session["interview_date"] else None
    plan_mode = "daily" if _study_plan_uses_day_numbers(plan["total_days"]) else "focus"
    suggested_videos = _build_suggested_videos(topics, session_skill_rows)

    return {
        "session_id": session_id,
        "plan_id": plan["id"],
        "interview_date": session["interview_date"],
        "days_remaining": days_remaining,
        "total_days": plan["total_days"],
        "created_at": plan["created_at"],
        "total_topics": total_topics,
        "completed_topics": completed_count,
        "progress_percent": progress_percent,
        "plan_mode": plan_mode,
        "day_groups": day_groups,
        "focus_topics": topics if plan_mode == "focus" else [],
        "suggested_videos": suggested_videos,
        "skill_breakdown": {
            "present": sorted(present, key=lambda x: x["skill_name"].lower()),
            "lacking": sorted(lacking, key=lambda x: x["skill_name"].lower()),
        },
    }


@app.post("/api/module4/sessions/{session_id}/generate-plan")
async def generate_module4_plan(session_id: int):
    session = _get_session_by_id(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="Session not found.")

    interview_date = session["interview_date"]
    if not interview_date:
        raise HTTPException(status_code=400, detail="Interview date missing for this session.")

    total_days = max(1, _days_until(interview_date))

    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, skill_name, category, proficiency_score, priority_weight
            FROM skills
            WHERE session_id = ?
            ORDER BY id ASC
            """,
            (session_id,),
        )
        skills = [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()

    if not skills:
        raise HTTPException(status_code=400, detail="No skills found for this session. Extract skills first.")

    topics, generation_engine = _generate_study_topics_with_gemini(skills, total_days)
    if topics is None:
        topics = _fallback_generate_study_topics(skills, total_days)
        if not topics:
            raise HTTPException(
                status_code=503,
                detail=(
                    "AI study plan generation is currently unavailable. "
                    "Retry shortly; if this persists, verify GEMINI_API_KEY/model/quota settings."
                ),
            )
        generation_engine = f"fallback:{generation_engine}"

    _save_study_plan(session_id, total_days, topics, generation_engine)

    # Pre-generate the final test so the user can open it instantly from the plan page.
    try:
        questions, test_engine = _generate_final_practice_test_questions(session_id)
        if questions is not None:
            _save_final_practice_test(session_id, questions, test_engine)
    except Exception:
        # Plan generation should still succeed even if practice test pre-generation fails.
        pass

    response = _study_plan_response(session_id)
    response["generation_engine"] = generation_engine
    return response


@app.get("/api/module4/sessions/{session_id}/plan")
async def get_module4_plan(session_id: int):
    return _study_plan_response(session_id)


@app.patch("/api/module4/topics/{topic_id}/completion")
async def update_module4_topic_completion(topic_id: int, payload: TopicCompletionUpdate):
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT st.id, sp.session_id
            FROM study_topics st
            JOIN study_plans sp ON sp.id = st.plan_id
            WHERE st.id = ?
            """,
            (topic_id,),
        )
        row = cursor.fetchone()
        if row is None and (payload.plan_id is not None or payload.topic_name):
            conditions = ["sp.id = ?"]
            params: list[object] = [payload.plan_id or -1]

            if payload.topic_name:
                conditions.append("LOWER(st.topic_name) = LOWER(?)")
                params.append(payload.topic_name)
            if payload.skill_id is not None:
                conditions.append("st.skill_id = ?")
                params.append(payload.skill_id)
            if payload.day_number is not None:
                conditions.append("st.day_number = ?")
                params.append(payload.day_number)

            cursor.execute(
                f"""
                SELECT st.id, sp.session_id
                FROM study_topics st
                JOIN study_plans sp ON sp.id = st.plan_id
                WHERE {' AND '.join(conditions)}
                ORDER BY st.day_number ASC, st.id ASC
                LIMIT 1
                """,
                params,
            )
            row = cursor.fetchone()

        if row is None:
            raise HTTPException(status_code=404, detail="Study topic not found.")

        cursor.execute(
            "UPDATE study_topics SET is_completed = ? WHERE id = ?",
            (1 if payload.is_completed else 0, topic_id),
        )
        conn.commit()
        session_id = row["session_id"]
    finally:
        conn.close()

    return {
        "topic_id": topic_id,
        "is_completed": payload.is_completed,
        "plan": _study_plan_response(session_id),
    }


def _parse_practice_test_json(raw_text: str) -> list[dict]:
    parsed = _parse_json_object_from_model_text(raw_text)
    if not parsed:
        return []
    questions = parsed.get("questions", [])
    return questions if isinstance(questions, list) else []


def _normalize_practice_test_questions(raw_questions: list[dict]) -> list[dict]:
    normalized = []
    seen_questions = set()

    for idx, item in enumerate(raw_questions, start=1):
        if not isinstance(item, dict):
            continue

        question_text = " ".join(str(item.get("question_text", "")).strip().split())
        if len(question_text) < 12:
            continue

        question_key = question_text.lower()
        if question_key in seen_questions:
            continue

        options = item.get("options", [])
        if not isinstance(options, list):
            continue
        cleaned_options = []
        seen_options = set()
        for option in options:
            value = " ".join(str(option).strip().split())
            key = value.lower()
            if len(value) < 2 or key in seen_options:
                continue
            seen_options.add(key)
            cleaned_options.append(value)

        if len(cleaned_options) != 4:
            continue

        correct_option_index = _safe_int(item.get("correct_option_index", 0), 0)
        if correct_option_index < 1 or correct_option_index > 4:
            continue

        skill_name = " ".join(str(item.get("skill_name", "")).strip().split())
        topic_name = " ".join(str(item.get("topic_name", "")).strip().split())

        normalized.append(
            {
                "question_index": len(normalized) + 1,
                "skill_name": skill_name,
                "topic_name": topic_name,
                "question_text": question_text,
                "options": cleaned_options,
                "correct_option_index": correct_option_index,
            }
        )
        seen_questions.add(question_key)

        if len(normalized) >= 10:
            break

    return normalized


def _practice_test_questions_are_too_repetitive(questions: list[dict]) -> bool:
    if len(questions) < 10:
        return True

    question_text_keys = set()
    topic_keys = set()
    for question in questions:
        question_text = _clean_skill_value(question.get("question_text", "")).lower()
        topic_name = _clean_skill_value(question.get("topic_name", "")).lower()
        if question_text:
            question_text_keys.add(question_text)
        if topic_name:
            topic_keys.add(topic_name)

    if len(question_text_keys) < 8:
        return True
    if len(topic_keys) < 4:
        return True
    return False


def _practice_test_key_candidates() -> list[str]:
    ordered_unique = []
    seen = set()
    for key in [PRACTICE_TEST_GEMINI_API_KEY, QUESTION_GEMINI_API_KEY, GEMINI_API_KEY]:
        normalized = (key or "").strip()
        if not normalized:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        ordered_unique.append(normalized)
    return ordered_unique


def _split_round_robin(items: list[dict], bucket_count: int) -> list[list[dict]]:
    bucket_count = max(1, bucket_count)
    buckets = [[] for _ in range(bucket_count)]
    for index, item in enumerate(items):
        buckets[index % bucket_count].append(item)
    return buckets


def _generate_practice_test_batch_questions(
    api_key: str,
    skills: list[dict],
    topic_slice: list[dict],
    target_count: int,
    used_question_texts: set[str],
    used_topic_names: set[str],
    batch_label: str,
) -> list[dict]:
    if not api_key or target_count <= 0:
        return []

    prompt = f"""
You are generating a subset of a final mixed interview practice test.

Requirements:
- Generate exactly {target_count} multiple choice questions.
- Use only the skills and topics in this batch.
- Every question must have exactly 4 topic related options.
- Exactly one correct option per question.
- Questions must be distinct from any previously generated questions.
- Do not repeat the same topic_name or question_text.
- Keep language clear and concise.
- Output strict JSON only.

Batch label: {batch_label}

Already generated question texts:
{json.dumps(sorted(used_question_texts))}

Already used topic names:
{json.dumps(sorted(used_topic_names))}

Skills context:
{json.dumps(skills)}

Topic batch context:
{json.dumps(topic_slice)}

Return format:
{{
  "questions": [
    {{
      "skill_name": "Python",
      "topic_name": "Async API patterns",
      "question_text": "Which statement about ... is correct?",
      "options": ["Option A", "Option B", "Option C", "Option D"],
      "correct_option_index": 2
    }}
  ]
}}
"""

    last_reason = "ai_unavailable:model_unavailable"
    for model_name in _get_model_candidates():
        try:
            response = _generate_gemini_content(
                model_name,
                prompt,
                timeout_seconds=14,
                api_key=api_key,
            )
            raw_questions = _parse_practice_test_json(response.text or "")
            normalized = _normalize_practice_test_questions(raw_questions)
            filtered = []
            for question in normalized:
                question_key = _normalize_topic_name_key(question.get("question_text", ""))
                topic_key = _normalize_topic_name_key(question.get("topic_name", ""))
                if not question_key or question_key in used_question_texts:
                    continue
                if topic_key and topic_key in used_topic_names:
                    continue
                filtered.append(question)

            if filtered:
                return filtered
        except Exception as exc:
            last_reason = _classify_ai_generation_error(exc)
            continue

    return []


def _build_fallback_final_practice_test_questions(skills: list[dict], topic_context: list[dict]) -> list[dict]:
    if not skills:
        return []

    topic_pool = []
    for item in topic_context:
        topic_name = _clean_skill_value(item.get("topic_name", ""))
        for subtopic in item.get("subtopics", []) or []:
            value = _clean_skill_value(subtopic)
            if value:
                topic_pool.append(value)
        if topic_name:
            topic_pool.append(topic_name)

    if not topic_pool:
        topic_pool = [_clean_skill_value(skill["skill_name"]) for skill in skills if _clean_skill_value(skill.get("skill_name", ""))]

    topic_pool = [value for value in _normalized_unique(topic_pool) if value]

    generic_distractors = [
        "Primary key relationships",
        "Caching strategy",
        "Error handling",
        "Concurrency model",
        "State management",
        "Input validation",
        "Transactions",
        "Index usage",
        "API routing",
        "Testing strategy",
        "Deployment setup",
        "Performance tuning",
    ]

    templates = [
        "Which statement best describes {focus} in {topic_name}?",
        "What is the main purpose of {focus} when studying {topic_name}?",
        "Which scenario most directly uses {focus} in {topic_name}?",
        "Which option is the best example of {focus} for {topic_name}?",
        "How would you explain {focus} in the context of {topic_name}?",
        "Which choice is closest to the correct approach for {topic_name} and {focus}?",
        "What is the most likely interview takeaway from {focus} in {topic_name}?",
        "Which answer best matches the role of {focus} here: {topic_name}?",
        "Which of these is the strongest description of {focus} for {topic_name}?",
        "What does {focus} contribute to {topic_name}?",
    ]

    questions = []
    seen_question_texts = set()
    for idx in range(10):
        topic = topic_context[idx % len(topic_context)] if topic_context else {"skill_name": skills[idx % len(skills)]["skill_name"], "topic_name": skills[idx % len(skills)]["skill_name"], "subtopics": []}
        skill_name = _clean_skill_value(topic.get("skill_name", "")) or _clean_skill_value(skills[idx % len(skills)]["skill_name"])
        topic_name = _clean_skill_value(topic.get("topic_name", "")) or skill_name

        subtopics = []
        for subtopic in topic.get("subtopics", []) or []:
            value = _clean_skill_value(subtopic)
            if value and value.lower() not in {item.lower() for item in subtopics}:
                subtopics.append(value)

        focus = subtopics[idx % len(subtopics)] if subtopics else f"Core idea in {topic_name}"
        correct_answer = focus

        distractors = []
        for candidate in [*topic_pool, *generic_distractors]:
            if candidate.lower() == correct_answer.lower():
                continue
            if candidate.lower() in {item.lower() for item in distractors}:
                continue
            distractors.append(candidate)
            if len(distractors) >= 3:
                break

        while len(distractors) < 3:
            fallback_value = f"Alternative concept {len(distractors) + 1}"
            if fallback_value.lower() != correct_answer.lower() and fallback_value.lower() not in {item.lower() for item in distractors}:
                distractors.append(fallback_value)

        options = [correct_answer, *distractors[:3]]
        random.shuffle(options)
        correct_option_index = options.index(correct_answer) + 1

        question_text = templates[idx % len(templates)].format(focus=focus, topic_name=topic_name)
        suffix = 2
        base_question_text = question_text
        while question_text.lower() in seen_question_texts:
            question_text = f"{base_question_text} (set {suffix})"
            suffix += 1
        seen_question_texts.add(question_text.lower())

        questions.append(
            {
                "question_index": idx + 1,
                "skill_name": skill_name,
                "topic_name": topic_name,
                "question_text": question_text,
                "options": options,
                "correct_option_index": correct_option_index,
            }
        )

    return questions


def _generate_final_practice_test_questions(session_id: int) -> tuple[list[dict] | None, str]:
    api_keys = _practice_test_key_candidates()
    if not api_keys:
        return None, "ai_unavailable:no_practice_test_api_key"
    last_reason = "ai_unavailable:model_unavailable"

    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT skill_name, category, proficiency_score, priority_weight
            FROM skills
            WHERE session_id = ?
            ORDER BY category ASC, priority_weight DESC, skill_name ASC
            """,
            (session_id,),
        )
        skills = [dict(row) for row in cursor.fetchall()]

        cursor.execute(
            """
            SELECT s.skill_name, st.topic_name, st.subtopics
            FROM study_topics st
            JOIN study_plans sp ON sp.id = st.plan_id
            JOIN skills s ON s.id = st.skill_id
            WHERE sp.session_id = ?
            ORDER BY st.day_number ASC, st.id ASC
            """,
            (session_id,),
        )
        topic_rows = cursor.fetchall()
    finally:
        conn.close()

    if not skills:
        return None, "ai_unavailable:no_skills"

    topic_context = []
    for row in topic_rows:
        subtopics = []
        try:
            subtopics = json.loads(row["subtopics"] or "[]")
        except Exception:
            subtopics = []
        topic_context.append(
            {
                "skill_name": row["skill_name"],
                "topic_name": row["topic_name"],
                "subtopics": subtopics,
            }
        )

    topic_batches = _split_round_robin(topic_context, len(api_keys))
    if not topic_batches:
        topic_batches = [[] for _ in api_keys]

    collected = []
    used_question_texts = set()
    used_topic_names = set()
    base_target = 10 // len(api_keys)
    remainder = 10 % len(api_keys)

    for index, api_key in enumerate(api_keys):
        batch_target = base_target + (1 if index < remainder else 0)
        topic_slice = topic_batches[index] if index < len(topic_batches) else []
        batch_questions = _generate_practice_test_batch_questions(
            api_key=api_key,
            skills=skills,
            topic_slice=topic_slice,
            target_count=batch_target,
            used_question_texts=used_question_texts,
            used_topic_names=used_topic_names,
            batch_label=f"batch-{index + 1}",
        )

        for question in batch_questions:
            question_key = _normalize_topic_name_key(question.get("question_text", ""))
            topic_key = _normalize_topic_name_key(question.get("topic_name", ""))
            if not question_key or question_key in used_question_texts:
                continue
            if topic_key and topic_key in used_topic_names:
                continue
            used_question_texts.add(question_key)
            if topic_key:
                used_topic_names.add(topic_key)
            collected.append(question)

        if len(collected) >= 10:
            break

    if len(collected) >= 10:
        normalized = collected[:10]
        if not _practice_test_questions_are_too_repetitive(normalized):
            for idx, question in enumerate(normalized, start=1):
                question["question_index"] = idx
            return normalized, "ai:multi-key"

    fallback_questions = _build_fallback_final_practice_test_questions(skills, topic_context)
    if len(fallback_questions) == 10:
        return fallback_questions, f"fallback:{last_reason}"

    return None, last_reason


def _save_final_practice_test(session_id: int, questions: list[dict], generation_engine: str) -> int:
    conn = get_connection()
    try:
        cursor = conn.cursor()

        cursor.execute("SELECT id FROM practice_tests WHERE session_id = ?", (session_id,))
        existing = cursor.fetchone()
        if existing:
            test_id = int(existing["id"])
            cursor.execute("DELETE FROM practice_test_questions WHERE test_id = ?", (test_id,))
            cursor.execute(
                """
                UPDATE practice_tests
                SET total_questions = ?, score_percent = NULL, submitted_at = NULL, created_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (len(questions), test_id),
            )
        else:
            cursor.execute(
                "INSERT INTO practice_tests (session_id, total_questions) VALUES (?, ?)",
                (session_id, len(questions)),
            )
            test_id = int(cursor.lastrowid)

        for question in questions:
            cursor.execute(
                """
                INSERT INTO practice_test_questions (
                    test_id,
                    question_index,
                    skill_name,
                    topic_name,
                    question_text,
                    options,
                    correct_option_index
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    test_id,
                    question["question_index"],
                    question.get("skill_name", ""),
                    question.get("topic_name", ""),
                    question["question_text"],
                    json.dumps(question["options"]),
                    question["correct_option_index"],
                ),
            )

        cursor.execute("UPDATE sessions SET status = ? WHERE id = ?", ("practice_test_ready", session_id))
        conn.commit()
        return test_id
    finally:
        conn.close()


def _practice_test_response(session_id: int) -> dict:
    session = _get_session_by_id(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="Session not found.")

    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, total_questions, score_percent, submitted_at, created_at FROM practice_tests WHERE session_id = ?",
            (session_id,),
        )
        test = cursor.fetchone()
        if test is None:
            raise HTTPException(status_code=404, detail="Practice test not found.")

        cursor.execute(
            """
            SELECT id, question_index, skill_name, topic_name, question_text, options
            FROM practice_test_questions
            WHERE test_id = ?
            ORDER BY question_index ASC, id ASC
            """,
            (test["id"],),
        )
        question_rows = cursor.fetchall()
    finally:
        conn.close()

    questions = []
    for row in question_rows:
        options = []
        try:
            options = json.loads(row["options"] or "[]")
        except Exception:
            options = []
        questions.append(
            {
                "id": row["id"],
                "question_index": row["question_index"],
                "skill_name": row["skill_name"],
                "topic_name": row["topic_name"],
                "question_text": row["question_text"],
                "options": options,
            }
        )

    return {
        "session_id": session_id,
        "test_id": int(test["id"]),
        "total_questions": int(test["total_questions"] or len(questions) or 10),
        "created_at": test["created_at"],
        "submitted_at": test["submitted_at"],
        "is_submitted": test["submitted_at"] is not None,
        "score_percent": test["score_percent"],
        "questions": questions,
    }


@app.post("/api/module5/sessions/{session_id}/generate-test")
async def generate_module5_final_test(session_id: int):
    session = _get_session_by_id(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="Session not found.")

    plan = _study_plan_response(session_id)
    if (plan.get("progress_percent") or 0) < 100:
        raise HTTPException(status_code=400, detail="Complete 100% of the study plan before taking the practice test.")

    questions, generation_engine = _generate_final_practice_test_questions(session_id)
    if questions is None:
        raise HTTPException(
            status_code=503,
            detail=(
                "AI practice test generation is currently unavailable. "
                "Set PRACTICE_TEST_GEMINI_API_KEY (or reuse an existing Gemini key) and verify model/quota settings."
            ),
        )

    _save_final_practice_test(session_id, questions, generation_engine)
    response = _practice_test_response(session_id)
    response["generation_engine"] = generation_engine
    return response


@app.get("/api/module5/sessions/{session_id}/test")
async def get_module5_final_test(session_id: int):
    return _practice_test_response(session_id)


@app.post("/api/module5/tests/{test_id}/submit")
async def submit_module5_final_test(test_id: int, payload: PracticeTestSubmission):
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id, session_id FROM practice_tests WHERE id = ?", (test_id,))
        test = cursor.fetchone()
        if test is None:
            raise HTTPException(status_code=404, detail="Practice test not found.")

        cursor.execute(
            "SELECT id, correct_option_index FROM practice_test_questions WHERE test_id = ?",
            (test_id,),
        )
        question_rows = cursor.fetchall()
        if not question_rows:
            raise HTTPException(status_code=400, detail="No questions found for this test.")

        correct_by_question = {int(row["id"]): int(row["correct_option_index"]) for row in question_rows}
        total_questions = len(correct_by_question)

        answer_map = {}
        for answer in payload.answers:
            selected = _safe_int(answer.selected_option_index, 0)
            if selected < 1 or selected > 4:
                raise HTTPException(status_code=400, detail="Selected option index must be between 1 and 4.")
            answer_map[int(answer.question_id)] = selected

        if len(answer_map) != total_questions:
            raise HTTPException(status_code=400, detail="Submit answers for all test questions.")

        if set(answer_map.keys()) != set(correct_by_question.keys()):
            raise HTTPException(status_code=400, detail="Answer set does not match the generated test questions.")

        correct_count = sum(1 for qid, selected in answer_map.items() if selected == correct_by_question[qid])
        score_percent = round((correct_count / total_questions) * 100, 2)

        cursor.execute(
            "UPDATE practice_tests SET score_percent = ?, submitted_at = CURRENT_TIMESTAMP WHERE id = ?",
            (score_percent, test_id),
        )
        cursor.execute("UPDATE sessions SET status = ? WHERE id = ?", ("practice_test_completed", test["session_id"]))
        conn.commit()
        session_id = int(test["session_id"])
    finally:
        conn.close()

    return {
        "test_id": test_id,
        "session_id": session_id,
        "correct_count": correct_count,
        "total_questions": total_questions,
        "score_percent": score_percent,
        "test": _practice_test_response(session_id),
    }
