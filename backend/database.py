import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "assessment.db")

def get_connection():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row  # Return rows as dicts
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _table_columns(cursor, table_name):
    cursor.execute(f"PRAGMA table_info({table_name})")
    return {row[1] for row in cursor.fetchall()}


def _add_column_if_missing(cursor, table_name, column_name, column_sql):
    if column_name not in _table_columns(cursor, table_name):
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_sql}")

def init_db():
    conn = get_connection()
    cursor = conn.cursor()

    # Sessions table — one row per user assessment session
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            resume_text TEXT,
            job_description TEXT,
            interview_date TEXT,
            status TEXT DEFAULT 'input'
            -- status flow: input → skills_extracted → assessment → plan_generated → done
        )
    """)

    # Skills table — extracted skills per session
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS skills (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER NOT NULL,
            skill_name TEXT NOT NULL,
            category TEXT NOT NULL,      -- 'present' or 'lacking'
            proficiency_score REAL,       -- 0.0 to 1.0, filled after assessment
            priority_weight REAL,         -- higher = needs more study time
            FOREIGN KEY (session_id) REFERENCES sessions(id)
        )
    """)

    # Assessment questions table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS questions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER NOT NULL,
            skill_id INTEGER NOT NULL,
            question_index INTEGER DEFAULT 1,
            difficulty_level INTEGER DEFAULT 1,
            question_text TEXT NOT NULL,
            options TEXT,
            correct_option_index INTEGER,
            ideal_answer TEXT,
            selected_option_index INTEGER,
            answer_text TEXT,             -- user's answer
            score REAL,                   -- AI-evaluated score 0.0 to 1.0
            feedback TEXT,                -- AI feedback on answer
            asked_at TIMESTAMP,
            answered_at TIMESTAMP,
            FOREIGN KEY (session_id) REFERENCES sessions(id),
            FOREIGN KEY (skill_id) REFERENCES skills(id)
        )
    """)

    # Study plan table — one plan per session
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS study_plans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER NOT NULL UNIQUE,
            total_days INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (session_id) REFERENCES sessions(id)
        )
    """)

    # Study plan topics — individual topics within a plan
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS study_topics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            plan_id INTEGER NOT NULL,
            skill_id INTEGER NOT NULL,
            topic_name TEXT NOT NULL,
            description TEXT,
            subtopics TEXT,               -- JSON string: ["topic a", "topic b"]
            resources TEXT,               -- JSON string: [{title, url, type}]
            estimated_hours REAL,
            day_number INTEGER,           -- which day to study this
            is_completed INTEGER DEFAULT 0,  -- 0 or 1 (checkbox)
            FOREIGN KEY (plan_id) REFERENCES study_plans(id),
            FOREIGN KEY (skill_id) REFERENCES skills(id)
        )
    """)

    # Backfill columns for databases created before Module 3.
    _add_column_if_missing(cursor, "questions", "question_index", "question_index INTEGER DEFAULT 1")
    _add_column_if_missing(cursor, "questions", "difficulty_level", "difficulty_level INTEGER DEFAULT 1")
    _add_column_if_missing(cursor, "questions", "options", "options TEXT")
    _add_column_if_missing(cursor, "questions", "correct_option_index", "correct_option_index INTEGER")
    _add_column_if_missing(cursor, "questions", "ideal_answer", "ideal_answer TEXT")
    _add_column_if_missing(cursor, "questions", "selected_option_index", "selected_option_index INTEGER")
    _add_column_if_missing(cursor, "study_topics", "subtopics", "subtopics TEXT")

    conn.commit()
    conn.close()
    print("✅ All tables created/verified")


def dict_from_row(row):
    """Convert sqlite3.Row to plain dict."""
    return dict(row) if row else None
