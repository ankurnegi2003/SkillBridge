# SkillBridge — AI Skill Assessment & Personalised Learning Agent

## Tech Stack
- **Backend**: Python + FastAPI + SQLite
- **Frontend**: React + Vite
- **AI**: Google Gemini 1.5 Flash (free tier)
- **DB**: SQLite (no setup needed, file-based)

---

## ⚡ One-Time Setup (Windows)

### 1. Get Your Free Gemini API Key
1. Go to → https://aistudio.google.com/app/apikey
2. Sign in with Google → Click "Create API Key"
3. Copy the key

### 2. Set Up the Backend

Open **Command Prompt** or **PowerShell** in the project folder:

```bash
cd skill-assessment-agent\backend

# Create virtual environment
python -m venv venv

# Activate it (Command Prompt)
venv\Scripts\activate.bat

# OR activate it (PowerShell)
venv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt

# Add your API key
# Open backend\.env and replace: your_gemini_api_key_here → your actual key
```

### 3. Set Up the Frontend

Open a **second** terminal:

```bash
cd skill-assessment-agent\frontend

# Install Node dependencies
npm install
```

---

## 🚀 Running the App (Every Time)

**Terminal 1 — Backend:**
```bash
cd skill-assessment-agent\backend
venv\Scripts\activate.bat
uvicorn main:app --reload --port 8000
```

**Terminal 2 — Frontend:**
```bash
cd skill-assessment-agent\frontend
npm run dev
```

Then open → **http://localhost:5173**

---

## 🗺️ Module Build Progress

- [x] **M1** — Project Scaffold (backend skeleton + DB schema + React shell)
- [x] **M2** — Resume Upload + JD Input + Skill Extraction
- [x] **M3** — Proficiency Assessment (AI questions + scoring)
- [ ] **M4** — Study Plan Generator
- [ ] **M5** — Progress Tracking Dashboard
- [ ] **M6** — Extended Scope (assignments, mock interviews)

---

## Module 2 Features Implemented

- Resume input via pasted text or PDF upload
- Job description text input
- Interview date picker with days remaining support
- Gemini-powered skill extraction into:
	- Present Skills (in resume + JD)
	- Lacking Skills (in JD but not resume)
- Session + skill persistence in SQLite tables (`sessions`, `skills`)
- Frontend split-view with color-coded Present vs Lacking skills

### Module 2 API

`POST /api/module2/extract-skills`

Form fields:
- `resume_text` (optional when `resume_file` is provided)
- `resume_file` (optional when `resume_text` is provided, must be PDF)
- `job_description` (required)
- `interview_date` (required, `YYYY-MM-DD`)

Response:
- `session_id`
- `interview_date`
- `days_remaining`
- `present_skills`
- `lacking_skills`

## Module 3 Features Implemented

- Generates 5 Gemini-powered questions per present skill, increasing in difficulty
- Conversational, one-question-at-a-time assessment UI
- Answers are scored by Gemini on a 0-1 scale with instant feedback
- Skill proficiency averages are saved back to `skills`
- Priority weights are recalculated so weaker skills get more study time later
- All question attempts are saved in `questions`

### Module 3 API

`POST /api/module3/sessions/{session_id}/start-assessment`
- Generates missing questions for all present skills and starts the assessment

`GET /api/module3/sessions/{session_id}/next-question`
- Returns the next unanswered question or a completion summary

`POST /api/module3/questions/{question_id}/answer`
- Saves the answer, Gemini score, feedback, and recalculates skill weight

`GET /api/module3/sessions/{session_id}/summary`
- Returns per-skill scores, priority weights, and progress

---

## 📁 Project Structure

```
skill-assessment-agent/
├── backend/
│   ├── main.py          # FastAPI app + routes
│   ├── database.py      # SQLite schema + helpers
│   ├── config.py        # API keys, settings
│   ├── .env             # Your secret keys (never commit this)
│   └── requirements.txt
├── frontend/
│   ├── src/
│   │   ├── App.jsx      # Main app + routing
│   │   ├── main.jsx     # React entry point
│   │   └── index.css    # Global styles + design tokens
│   ├── index.html
│   ├── vite.config.js
│   └── package.json
└── README.md
```
