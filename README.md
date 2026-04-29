# SkillBridge: AI Skill Assessment and Interview Prep Agent

## Project live at - https://skill-bridge-steel.vercel.app/
Note- The backend is on free tier Render, after clicking extract skills it will most probably give timeout error(backend takes 40-50 sec to wake up), try clicking a second time it will definitely work.

SkillBridge is an end-to-end interview preparation application that:
1. extracts present and lacking skills from resume plus job description,
2. runs a skill assessment with AI-generated MCQ questions,
3. builds a progressive study plan up to interview day,
4. tracks topic completion,
5. generates a final 10-question mixed practice test.

The project has a FastAPI backend, a React/Vite frontend, and a local SQLite database.

## What This Project Does

### Module 2: Skill Extraction
1. Accepts resume text and/or resume PDF.
2. Accepts job description and interview date.
3. Uses Gemini to classify skills into:
   - present skills
   - lacking skills
4. Saves session and skills in SQLite.

### Module 3: Proficiency Assessment
1. Generates assessment questions for present skills.
2. Runs one-question-at-a-time MCQ flow.
3. Scores each answer and stores feedback.
4. Recalculates per-skill proficiency and priority weight.

### Module 4: Study Plan Generator
1. Builds a day-wise or focus-block plan based on time to interview.
2. Balances entries by skill weakness and category.
3. Adds subtopics and learning resources.
4. Tracks completion via topic checkboxes.

### Module 5: Final Practice Test
1. Requires 100% study-plan completion.
2. Pre-generates and/or generates a 10-question mixed test.
3. Uses multi-key Gemini batching to improve diversity.
4. Scores submission and stores final percentage.

## Tech Stack

- Backend: FastAPI, Pydantic, SQLite
- Frontend: React 18, Vite, Axios
- AI: Google Gemini (gemini-1.5-flash-latest)
- Runtime: Python 3.10+ recommended, Node.js 18+ recommended

## Repository Layout

```
code/
|-- backend/
|   |-- main.py
|   |-- database.py
|   |-- config.py
|   |-- requirements.txt
|   |-- .env.example
|   `-- assessment.db            # created at runtime
|-- frontend/
|   |-- package.json
|   |-- index.html
|   |-- vite.config.js
|   `-- src/
|       |-- App.jsx
|       |-- main.jsx
|       `-- index.css
|-- checkpoint_module3_stable_2026-04-26/
`-- README.md
```

## API Summary

- `GET /` and `GET /health`
- `POST /api/module2/extract-skills`
- `GET /api/sessions/{session_id}/skills`
- `POST /api/module3/sessions/{session_id}/start-assessment`
- `GET /api/module3/sessions/{session_id}/next-question`
- `POST /api/module3/questions/{question_id}/answer`
- `GET /api/module3/sessions/{session_id}/summary`
- `POST /api/module4/sessions/{session_id}/generate-plan`
- `GET /api/module4/sessions/{session_id}/plan`
- `PATCH /api/module4/topics/{topic_id}/completion`
- `POST /api/module5/sessions/{session_id}/generate-test`
- `GET /api/module5/sessions/{session_id}/test`
- `POST /api/module5/tests/{test_id}/submit`

## Environment Variables

Create `backend/.env` from `backend/.env.example`.

Required/used keys:
- `GEMINI_API_KEY`
- `QUESTION_GEMINI_API_KEY`
- `PRACTICE_TEST_GEMINI_API_KEY`
- `ALLOWED_ORIGINS` (comma-separated frontend URLs allowed by CORS)

You can reuse the same key in all three variables, or use separate keys to spread quota usage.

## Local Setup (Windows)

### 1. Backend setup

From project root:

```powershell
cd backend
python -m venv ..\.venv
..\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env
```

Then open `backend/.env` and add your Gemini keys.

### 2. Frontend setup

From project root in a second terminal:

```powershell
cd frontend
npm install
```

## Run the Project

### Terminal A (backend)

```powershell
cd backend
..\.venv\Scripts\Activate.ps1
uvicorn main:app --reload --port 8000
```

### Terminal B (frontend)

```powershell
cd frontend
npm run dev
```

Open `http://localhost:5173`.

## Free Deployment (Recommended)

This project does not need Docker for a basic free deployment.

Recommended setup:
- Frontend: Vercel
- Backend: Render Web Service

Why this setup:
- Vercel handles Vite/React easily.
- Render runs FastAPI easily.
- The app gets a public URL, so it works on phone, laptop, tablet, or any device with a browser.

Important:
- GitHub Pages can host only the frontend. It cannot run the FastAPI backend.
- Your backend currently uses SQLite. On free hosting, SQLite is fine for demo use, but data may reset after restart or redeploy because free services often use ephemeral storage.
- For a hackathon or portfolio demo, this is usually okay. For long-term persistent data, move later to a hosted database like Postgres.

### Step 1: Push the repo to GitHub

From the project root:

```powershell
git init
git add .
git commit -m "Initial commit"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO.git
git push -u origin main
```

If the repo already exists, just commit and push your latest changes.

### Step 2: Deploy the backend on Render

1. Sign in to Render.
2. Click `New` -> `Web Service`.
3. Connect your GitHub repo.
4. Select this repo.
5. Set:
   - Root Directory: `backend`
   - Environment: `Python 3`
   - Build Command: `pip install -r requirements.txt`
   - Start Command: `uvicorn main:app --host 0.0.0.0 --port $PORT`
6. Add environment variables:
   - `GEMINI_API_KEY`
   - `QUESTION_GEMINI_API_KEY`
   - `PRACTICE_TEST_GEMINI_API_KEY`
   - `ALLOWED_ORIGINS=http://localhost:5173,http://127.0.0.1:5173`
7. Deploy.

After deployment, copy your backend URL, for example:

```text
https://skillbridge-api.onrender.com
```

Test it in the browser:

```text
https://skillbridge-api.onrender.com/health
```

If health works, backend is live.

### Step 3: Deploy the frontend on Vercel

1. Sign in to Vercel.
2. Click `Add New...` -> `Project`.
3. Import the same GitHub repo.
4. Set:
   - Root Directory: `frontend`
   - Framework Preset: `Vite`
5. Add environment variable:
   - `VITE_API_BASE=https://skillbridge-api.onrender.com`
6. Deploy.

After deployment, copy the frontend URL, for example:

```text
https://skillbridge.vercel.app
```

### Step 4: Update backend CORS with your real frontend URL

Go back to Render and change `ALLOWED_ORIGINS` to include your deployed frontend URL:

```text
https://skillbridge.vercel.app,http://localhost:5173,http://127.0.0.1:5173
```

Save and redeploy the backend.

### Step 5: Test from any device

Open the Vercel frontend URL on:
- your laptop
- your phone
- another computer

As long as the device has internet and a browser, it should work.

### If you want to use GitHub Pages instead of Vercel

You still need Render or another backend host for FastAPI.
GitHub Pages alone is not enough for this project.

## Typical User Flow

1. Submit resume + JD + interview date.
2. Review extracted present/lacking skills.
3. Complete assessment questions.
4. Generate and follow study plan.
5. Mark all study topics complete.
6. Take and submit final practice test.

## Reset Data and Restore Project State

### A. Reset only runtime data (recommended for fresh testing)

1. Stop backend server.
2. Delete database file:

```powershell
Remove-Item backend\assessment.db -ErrorAction SilentlyContinue
Remove-Item backend\assessment.db-wal -ErrorAction SilentlyContinue
Remove-Item backend\assessment.db-shm -ErrorAction SilentlyContinue
```

3. Start backend again. Tables are auto-created at startup.

### B. Restore code to latest committed state (git)

Warning: this discards uncommitted changes.

```powershell
git status
git restore .
git clean -fd
```

If you want to keep your edits, commit or stash first:

```powershell
git add .
git commit -m "save work"
# or
git stash push -u
```

### C. Restore selected files from local checkpoint folder

This repository includes `checkpoint_module3_stable_2026-04-26/` with baseline files.
You can manually copy checkpoint files back into `backend/` or `frontend/src/` if you want a partial rollback.

## Troubleshooting

### Backend fails to start
- Ensure virtual environment is activated.
- Ensure dependencies are installed from `backend/requirements.txt`.
- Check that Gemini keys exist in `backend/.env`.

### Study plan checkboxes fail
- Refresh the page after plan regeneration.
- Confirm backend is running and DB is writable.
- If needed, reset DB (section A above).

### Practice test generation unavailable
- Confirm at least one Gemini key is configured.
- Confirm quota is available.
- Complete study plan to 100% before generating test.

## Notes

- Database is local SQLite file storage, no external DB required.
- CORS is controlled by `ALLOWED_ORIGINS`.
- This project is built for rapid local development and demo workflows.
