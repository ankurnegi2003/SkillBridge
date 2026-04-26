import os
from dotenv import load_dotenv

load_dotenv()

# ── Gemini API ─────────────────────────────────────────────────────────────────
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL   = "gemini-1.5-flash-latest"  # Free tier model

# ── App Settings ───────────────────────────────────────────────────────────────
MAX_QUESTIONS_PER_SKILL = 3               # How many questions to ask per skill
ASSESSMENT_PASS_THRESHOLD = 0.65          # Score above this → "proficient"

if not GEMINI_API_KEY:
    print("⚠️  WARNING: GEMINI_API_KEY not set in .env file")
