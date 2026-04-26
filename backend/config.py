import os

from dotenv import load_dotenv

load_dotenv()


def _csv_env(name: str, default: str = "") -> list[str]:
    raw_value = os.getenv(name, default)
    return [item.strip() for item in raw_value.split(",") if item.strip()]


GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
QUESTION_GEMINI_API_KEY = os.getenv("QUESTION_GEMINI_API_KEY", "")
PRACTICE_TEST_GEMINI_API_KEY = os.getenv("PRACTICE_TEST_GEMINI_API_KEY", "")
GEMINI_MODEL = "gemini-1.5-flash-latest"

MAX_SKILLS_FOR_QUESTION_GENERATION = 3
MAX_QUESTIONS_PER_SKILL = 3
ASSESSMENT_PASS_THRESHOLD = 0.65
ALLOWED_ORIGINS = _csv_env(
    "ALLOWED_ORIGINS",
    "http://localhost:5173,http://127.0.0.1:5173",
)

if not GEMINI_API_KEY:
    print("WARNING: GEMINI_API_KEY not set in .env file")
