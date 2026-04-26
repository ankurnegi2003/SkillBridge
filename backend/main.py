import io
import json
import os
import random
import re
import sqlite3
import time
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone

import google.generativeai as genai
from fastapi import BackgroundTasks, FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from config import GEMINI_API_KEY, GEMINI_MODEL
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
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)


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
            model = genai.GenerativeModel(model_name)
            # Keep extraction responsive; if the model is slow, fall back to deterministic matching.
            response = model.generate_content(prompt, request_options={"timeout": 10})
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
    if not GEMINI_API_KEY:
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
            model = genai.GenerativeModel(model_name)
            # Keep assessment start responsive; fallback questions are used if model is slow.
            response = model.generate_content(prompt, request_options={"timeout": 12})
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

    skills = _get_session_present_skills(session_id)
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


def _subtopics_for_skill(skill_name: str, mode: str = "foundation") -> list[str]:
    canonical = _canonical_skill_label(skill_name)

    specific = {
        "Python": {
            "foundation": [
                "Core data structures and comprehensions",
                "Functions, scope, and modules",
                "Exceptions and file handling",
            ],
            "gap": [
                "Decorators and context managers",
                "Async functions and await patterns",
                "Multithreading vs multiprocessing",
            ],
        },
        "FastAPI": {
            "foundation": [
                "Path operations and request validation",
                "Pydantic models and response schemas",
                "Dependency injection basics",
            ],
            "gap": [
                "Background tasks and async endpoints",
                "Auth, middleware, and error handling",
                "Database sessions and transactions",
            ],
        },
        "SQL": {
            "foundation": [
                "SELECT, WHERE, GROUP BY, ORDER BY",
                "INNER/LEFT joins and relationship mapping",
                "Aggregations and filtering patterns",
            ],
            "gap": [
                "Window functions",
                "Indexing and query plans",
                "Transactions and isolation levels",
            ],
        },
        "React": {
            "foundation": [
                "JSX and component composition",
                "Props/state flow",
                "useEffect and lifecycle thinking",
            ],
            "gap": [
                "Memoization and render optimization",
                "State architecture and custom hooks",
                "Error boundaries and resilient UI patterns",
            ],
        },
    }

    if canonical in specific:
        if mode == "gap":
            return specific[canonical]["gap"]
        return specific[canonical]["foundation"]

    if mode == "gap":
        return [
            f"Advanced patterns in {canonical}",
            f"Performance and debugging in {canonical}",
            f"Interview-style problem solving with {canonical}",
        ]

    return [
        f"Core concepts of {canonical}",
        f"Hands-on basics in {canonical}",
        f"Common mistakes and best practices in {canonical}",
    ]


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


def _rebalance_topics_for_time_and_weight(topics: list[dict], skills: list[dict], total_days: int) -> list[dict]:
    if not topics:
        return topics

    skill_map = {int(s["id"]): s for s in skills}
    slots_per_day = 2 if total_days >= 3 else 1
    max_topics = max(1, total_days * slots_per_day)

    # Attach score-driven rank hints.
    ranked = []
    for topic in topics:
        skill = skill_map.get(int(topic["skill_id"]))
        weight = _study_weight_for_skill(skill) if skill else 1.0
        category = str((skill or {}).get("category", "")).lower()
        boost = 1.0
        if category == "present":
            boost = 1.15
        ranked.append((weight * boost, topic))

    ranked.sort(key=lambda x: x[0], reverse=True)

    # Keep the strongest weighted topics if model returns too many.
    trimmed = [item[1] for item in ranked[:max_topics]]

    # If model returns too few topics for available days, duplicate weak areas first.
    while len(trimmed) < min(max_topics, max(1, total_days)):
        source = ranked[len(trimmed) % len(ranked)][1]
        clone = dict(source)
        clone["topic_name"] = f"{source['topic_name']} (Review)"
        trimmed.append(clone)

    # Time budget scales with days left; cap excessive daily burden.
    daily_hours = 2.5 if total_days > 3 else 2.0
    total_budget = max(2.0, total_days * daily_hours)

    current_total = sum(max(0.5, _safe_float(t.get("estimated_hours"), 1.5)) for t in trimmed)
    if current_total <= 0:
        current_total = float(len(trimmed))

    scale = total_budget / current_total
    for topic in trimmed:
        base_hours = max(0.5, _safe_float(topic.get("estimated_hours"), 1.5))
        topic["estimated_hours"] = round(max(0.75, min(4.0, base_hours * scale)), 2)

    # Recompute order (weakest first) and spread across available days.
    ordered = sorted(
        trimmed,
        key=lambda t: _study_weight_for_skill(skill_map.get(int(t["skill_id"]), {})),
        reverse=True,
    )

    idx = 0
    for day in range(1, total_days + 1):
        for _ in range(slots_per_day):
            if idx >= len(ordered):
                break
            ordered[idx]["day_number"] = day
            idx += 1
        if idx >= len(ordered):
            break

    return ordered


def _fallback_generate_study_topics(skills: list[dict], total_days: int) -> list[dict]:
    lacking = [s for s in skills if s["category"] == "lacking"]
    present = [s for s in skills if s["category"] == "present"]

    present_sorted = sorted(
        present,
        key=lambda x: x["proficiency_score"] if x["proficiency_score"] is not None else 0.5,
    )

    seeds = []
    for skill in lacking:
        seeds.append(
            {
                "skill_id": skill["id"],
                "skill_name": skill["skill_name"],
                "topic_name": f"Foundation: {skill['skill_name']}",
                "description": f"Build core concepts and terminology for {skill['skill_name']} from first principles.",
                "subtopics": _subtopics_for_skill(skill["skill_name"], "foundation"),
                "estimated_hours": 2.0,
                "resources": _free_resources_for_skill(skill["skill_name"]),
            }
        )
        seeds.append(
            {
                "skill_id": skill["id"],
                "skill_name": skill["skill_name"],
                "topic_name": f"Practice: {skill['skill_name']}",
                "description": f"Complete a guided hands-on exercise for {skill['skill_name']} and note common mistakes.",
                "subtopics": _subtopics_for_skill(skill["skill_name"], "foundation"),
                "estimated_hours": 2.0,
                "resources": _free_resources_for_skill(skill["skill_name"]),
            }
        )

    for skill in present_sorted:
        proficiency = skill["proficiency_score"] if skill["proficiency_score"] is not None else 0.5
        gap_focus = "advanced" if proficiency >= 0.75 else "intermediate"
        seeds.append(
            {
                "skill_id": skill["id"],
                "skill_name": skill["skill_name"],
                "topic_name": f"Gap Fill: {skill['skill_name']}",
                "description": f"Target {gap_focus} gaps in {skill['skill_name']} based on assessment score and improve weak patterns.",
                "subtopics": _subtopics_for_skill(skill["skill_name"], "gap"),
                "estimated_hours": 1.5,
                "resources": _free_resources_for_skill(skill["skill_name"]),
            }
        )

    if not seeds:
        return []

    topics_per_day = 2 if total_days >= 3 else 1
    required_topics = max(total_days, total_days * topics_per_day)

    expanded = []
    idx = 0
    while len(expanded) < required_topics:
        expanded.append(dict(seeds[idx % len(seeds)]))
        idx += 1

    scheduled = []
    ptr = 0
    for day in range(1, total_days + 1):
        for _ in range(topics_per_day):
            if ptr >= len(expanded):
                break
            topic = expanded[ptr]
            ptr += 1
            topic["day_number"] = day
            scheduled.append(topic)

    return scheduled


def _normalize_study_topics_from_ai(raw_topics: list[dict], skills: list[dict], total_days: int) -> list[dict]:
    if not isinstance(raw_topics, list):
        return []

    skill_name_map = {_canonical_skill_label(s["skill_name"]).lower(): s for s in skills}
    normalized = []

    for topic in raw_topics:
        if not isinstance(topic, dict):
            continue

        day_number = _safe_int(topic.get("day_number", 1), 1)
        day_number = max(1, min(total_days, day_number))

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

        description = " ".join(str(topic.get("description", "")).strip().split())
        if not description:
            description = f"Study {matched_skill['skill_name']} with interview-focused exercises."

        raw_subtopics = topic.get("subtopics", [])
        if isinstance(raw_subtopics, str):
            raw_subtopics = [part.strip() for part in re.split(r"[,;\n]", raw_subtopics) if part.strip()]
        cleaned_subtopics = []
        for subtopic in list(raw_subtopics)[:6]:
            value = " ".join(str(subtopic).strip().split())
            if len(value) < 3:
                continue
            cleaned_subtopics.append(value)

        if not cleaned_subtopics:
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
            cleaned_resources.append({"title": title, "url": url, "type": rtype or "resource"})

        if not cleaned_resources:
            continue

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

    normalized.sort(key=lambda x: (x["day_number"], x["skill_name"], x["topic_name"]))
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

    prompt = f"""
You are creating a practical interview prep study plan.

Constraints:
- total_days: {total_days}
- Use lacking skills for foundational learning based on available time.
- Use present skills for targeted weak-gap improvement based on proficiency_score and priority_weight.
- Keep plan realistic: 1-2 topics per day.
- For every topic include 3-5 concrete subtopics to study that day.
- Prefer explicit subtopics such as async functions, decorators, multithreading (when relevant), APIs, indexing, etc.
- Each topic must include free resources only.
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
            model = genai.GenerativeModel(model_name)
            response = model.generate_content(prompt, request_options={"timeout": 10})
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

    skill_map = {}
    for topic in topics:
        sid = topic["skill_id"]
        if sid in skill_map:
            continue
        skill_map[sid] = {
            "skill_id": sid,
            "skill_name": topic["skill_name"],
            "category": topic["category"],
            "proficiency_score": topic["proficiency_score"],
            "priority_weight": topic["priority_weight"],
        }

    present = [s for s in skill_map.values() if s["category"] == "present"]
    lacking = [s for s in skill_map.values() if s["category"] == "lacking"]

    total_topics = len(topics)
    progress_percent = 0 if total_topics == 0 else round((completed_count / total_topics) * 100, 2)
    days_remaining = _days_until(session["interview_date"]) if session["interview_date"] else None

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
        "day_groups": day_groups,
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
                    "Study plan generation is currently unavailable. "
                    "Retry shortly; if this persists, verify GEMINI_API_KEY/model/quota settings."
                ),
            )
        generation_engine = f"fallback:{generation_engine}"

    _save_study_plan(session_id, total_days, topics, generation_engine)

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
