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
- Each question must have exactly four options.
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
