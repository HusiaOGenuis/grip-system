from fastapi import FastAPI, UploadFile, File, Depends, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

import uuid
import csv
import io
import os

from psycopg.types.json import Json

from .db import get_conn
from .deps import get_current_user, get_dataset
from .models import DatasetOut, AnalysisOut
from .services import analyze_data
from .payments import initialize_payment, verify_payment
from .scoring import grip_score
from .reports import generate_pdf

# ✅ CREATE APP FIRST
app = FastAPI()
@app.get("/")
def serve_dashboard():
    return FileResponse("static/index.html")
app = FastAPI()

# -----------------------------
# ENV VALIDATION
# -----------------------------
REQUIRED_ENV = ["DATABASE_URL", "PAYSTACK_SECRET_KEY"]

def validate_env():
    missing = [k for k in REQUIRED_ENV if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing ENV: {missing}")

# -----------------------------
# DB INIT
# -----------------------------
def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:

            cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                api_key TEXT PRIMARY KEY,
                email TEXT UNIQUE,
                password TEXT,
                is_paid BOOLEAN DEFAULT FALSE
            )
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS datasets (
                id TEXT PRIMARY KEY,
                api_key TEXT,
                filename TEXT,
                parsed_json JSONB
            )
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS payments (
                id TEXT PRIMARY KEY,
                api_key TEXT,
                reference TEXT,
                status TEXT
            )
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS reports (
                id TEXT PRIMARY KEY,
                dataset_id TEXT,
                api_key TEXT,
                report_json JSONB
            )
            """)

@app.on_event("startup")
def startup():
    validate_env()
    init_db()

# -----------------------------
# VERSION
# -----------------------------
@app.get("/version")
def version():
    return {"version": "GRIP_STABLE_V1"}

# -----------------------------
# AUTH
# -----------------------------
@app.post("/register")
def register(email: str, password: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT api_key FROM users WHERE email=%s", (email,))
            if cur.fetchone():
                raise HTTPException(400, "USER_EXISTS")

            api_key = str(uuid.uuid4())

            cur.execute("""
                INSERT INTO users (api_key, email, password, is_paid)
                VALUES (%s, %s, %s, FALSE)
            """, (api_key, email, password))

            return {"api_key": api_key}

@app.post("/login")
def login(email: str, password: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT api_key FROM users
                WHERE email=%s AND password=%s
            """, (email, password))

            row = cur.fetchone()

            if not row:
                raise HTTPException(401, "INVALID_CREDENTIALS")

            return {"api_key": row[0]}

# -----------------------------
# DATASETS
# -----------------------------
@app.get("/datasets")
def list_datasets(api_key: str = Depends(get_current_user)):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, filename FROM datasets WHERE api_key=%s",
                (api_key,)
            )
            rows = cur.fetchall()

    return [{"dataset_id": r[0], "filename": r[1]} for r in rows]

# -----------------------------
# UPLOAD
# -----------------------------
@app.post("/upload", response_model=DatasetOut)
async def upload(
    file: UploadFile = File(...),
    api_key: str = Depends(get_current_user)
):
    content = await file.read()
    text = content.decode("utf-8")

    reader = csv.DictReader(io.StringIO(text))
    parsed = [row for row in reader]

    if not parsed:
        raise HTTPException(400, "EMPTY_CSV")

    dataset_id = str(uuid.uuid4())

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO datasets (id, api_key, filename, parsed_json)
                VALUES (%s, %s, %s, %s)
            """, (dataset_id, api_key, file.filename, Json(parsed)))

    return {"dataset_id": dataset_id}

# -----------------------------
# ANALYZE
# -----------------------------
@app.post("/analyze", response_model=AnalysisOut)
def analyze(dataset=Depends(get_dataset)):
    result = analyze_data(dataset["data"])
    return {"dataset_id": dataset["id"], **result}

# -----------------------------
# PAYMENT INIT
# -----------------------------
@app.post("/pay")
def pay(api_key: str = Depends(get_current_user)):
    result = initialize_payment(api_key, "user@email.com")

    if "error" in result:
        raise HTTPException(400, result["error"])

    reference = result.get("reference")

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO payments (id, api_key, reference, status)
                VALUES (%s, %s, %s, %s)
            """, (str(uuid.uuid4()), api_key, reference, "initialized"))

    return result

# -----------------------------
# PAYMENT VERIFY
# -----------------------------
@app.get("/verify-payment")
def verify(reference: str, api_key: str = Depends(get_current_user)):
    status = verify_payment(reference)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE payments SET status=%s WHERE reference=%s
            """, (status, reference))

            if status == "success":
                cur.execute("""
                    UPDATE users SET is_paid=TRUE WHERE api_key=%s
                """, (api_key,))

    return {"status": status}

# -----------------------------
# REPORT
# -----------------------------
@app.post("/report")
def report(dataset=Depends(get_dataset), api_key: str = Depends(get_current_user)):

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT is_paid FROM users WHERE api_key=%s", (api_key,))
            row = cur.fetchone()

            if not row or not row[0]:
                raise HTTPException(402, "PAYMENT_REQUIRED")

    analysis = analyze_data(dataset["data"])
    score = grip_score(dataset["data"])

    file_path = generate_pdf(dataset["id"], analysis, score)

    report_id = str(uuid.uuid4())

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO reports (id, dataset_id, api_key, report_json)
                VALUES (%s, %s, %s, %s)
            """, (
                report_id,
                dataset["id"],
                api_key,
                Json({
                    "analysis": analysis,
                    "score": score,
                    "file": file_path
                })
            ))

    return {
        "report_id": report_id,
        "analysis": analysis,
        "score": score,
        "report_file": file_path
    }

# -----------------------------
# REPORT DOWNLOAD
# -----------------------------
@app.get("/download-report/{report_id}")
def download_report(report_id: str, api_key: str = Depends(get_current_user)):

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT report_json FROM reports
                WHERE id=%s AND api_key=%s
            """, (report_id, api_key))

            row = cur.fetchone()

            if not row:
                raise HTTPException(404, "REPORT_NOT_FOUND")

            file_path = row[0]["file"]

    return FileResponse(file_path, filename="report.pdf")

# -----------------------------
# DASHBOARD API
# -----------------------------
@app.get("/dashboard")
def dashboard(api_key: str = Depends(get_current_user)):
    with get_conn() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT id, filename FROM datasets
                WHERE api_key=%s ORDER BY id DESC
            """, (api_key,))
            datasets = cur.fetchall()

            cur.execute("""
                SELECT reference, status FROM payments
                WHERE api_key=%s ORDER BY reference DESC
            """, (api_key,))
            payments = cur.fetchall()

            cur.execute("""
                SELECT id FROM reports
                WHERE api_key=%s ORDER BY id DESC
            """, (api_key,))
            reports = cur.fetchall()

    return {
        "datasets": [{"dataset_id": d[0], "filename": d[1]} for d in datasets],
        "payments": [{"reference": p[0], "status": p[1]} for p in payments],
        "reports": [{"report_id": r[0]} for r in reports]
    }

# -----------------------------
# STATIC FRONTEND
# -----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "..", "static")

app.mount("/static", StaticFiles(directory="static"), name="static")