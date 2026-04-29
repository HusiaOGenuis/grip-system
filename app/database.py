import psycopg
import os

DATABASE_URL = os.getenv("DATABASE_URL")

def get_conn():
    return psycopg.connect(DATABASE_URL)

def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:

            # DATASETS TABLE
            cur.execute("""
            CREATE TABLE IF NOT EXISTS datasets (
                id TEXT PRIMARY KEY,
                filename TEXT,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """)

            # PAYMENTS TABLE (base)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS payments (
                dataset_id TEXT PRIMARY KEY
            );
            """)

            # 🔥 CRITICAL: ADD COLUMN IF MISSING
            cur.execute("""
            ALTER TABLE payments
            ADD COLUMN IF NOT EXISTS is_paid BOOLEAN DEFAULT FALSE;
            """)

            conn.commit()