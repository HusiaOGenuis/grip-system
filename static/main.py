from fastapi import FastAPI, UploadFile, File, Depends, HTTPException
from fastapi.staticfiles import StaticFiles

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

app = FastAPI()


# -----------------------------
# ENV CONTRACT (HARD ENFORCED)
# -----------------------------
REQUIRED_ENV = ["DATABASE_URL", "PAYSTACK_SECRET_KEY"]

def validate_env():
    missing = [k for k in REQUIRED_ENV if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing ENV: {missing}")


# -----------------------------
# INIT DB (SAFE)
# -----------------------------
def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:

            cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                api_key TEXT PRIMARY KEY,
                email TEXT UNIQUE,
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
# ROOT
# -----------------------------
@app.get("/")
def root():
    return {"status": "GRIP API running"}


@app.get("/version")
def version():
    return {"version": "CONTRACT_BUILD_FINAL"}


# -----------------------------
# CREATE USER
# -----------------------------
@app.post("/create-user")
def create_user(email: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT api_key FROM users WHERE email=%s", (email,))
            existing = cur.fetchone()

            if existing:
                return {"api_key": existing[0]}

            api_key = str(uuid.uuid4())

            cur.execute(
                "INSERT INTO users (api_key, email) VALUES (%s, %s)",
                (api_key, email)
            )

            return {"api_key": api_key}


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

            return [
                {"dataset_id": r[0], "filename": r[1]}
                for r in rows
            ]


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
        raise HTTPException(status_code=400, detail="EMPTY_CSV")

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

    return {
        "dataset_id": dataset["id"],
        **result
    }


# -----------------------------
# PAYMENT INIT
# -----------------------------
@app.post("/pay")
def pay(api_key: str = Depends(get_current_user)):

    result = initialize_payment(api_key, "user@email.com")

    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])

    reference = result["reference"]

    # STORE PAYMENT
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO payments (id, api_key, reference, status)
                VALUES (%s, %s, %s, %s)
            """, (
                str(uuid.uuid4()),
                api_key,
                reference,
                "initialized"
            ))

    return result
# -----------------------------
# PAYMENT VERIFY
# -----------------------------
@app.get("/verify-payment")
def verify(reference: str, api_key: str = Depends(get_current_user)):

    status = verify_payment(reference)

    with get_conn() as conn:
        with conn.cursor() as cur:

            # update payment status
            cur.execute("""
                UPDATE payments
                SET status=%s
                WHERE reference=%s
            """, (status, reference))

            # unlock user if successful
            if status == "success":
                cur.execute("""
                    UPDATE users
                    SET is_paid=TRUE
                    WHERE api_key=%s
                """, (api_key,))

    return {"status": status}

# -----------------------------
# REPORT (LOCKED)
# -----------------------------
@app.post("/report")
def report(
    dataset=Depends(get_dataset),
    api_key: str = Depends(get_current_user)
):

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT is_paid FROM users WHERE api_key=%s", (api_key,))
            row = cur.fetchone()

            if not row or not row[0]:
                raise HTTPException(status_code=402, detail="PAYMENT_REQUIRED")

    analysis = analyze_data(dataset["data"])
    score = grip_score(dataset["data"])

    file_path = generate_pdf(dataset["id"], analysis, score)

    return {
        "analysis": analysis,
        "score": score,
        "report_file": file_path
    }


# -----------------------------
# FRONTEND (ALWAYS LAST)
# -----------------------------
app.mount("/", StaticFiles(directory="static", html=True), name="static")
@app.get("/dashboard")
def dashboard(api_key: str = Depends(get_current_user)):

    with get_conn() as conn:
        with conn.cursor() as cur:

            # datasets
            cur.execute("""
                SELECT id, filename
                FROM datasets
                WHERE api_key=%s
                ORDER BY id DESC
            """, (api_key,))
            datasets = cur.fetchall()

            # payments
            cur.execute("""
                SELECT reference, status
                FROM payments
                WHERE api_key=%s
                ORDER BY reference DESC
            """, (api_key,))
            payments = cur.fetchall()

    return {
        "datasets": [
            {"dataset_id": d[0], "filename": d[1]}
            for d in datasets
        ],
        "payments": [
            {"reference": p[0], "status": p[1]}
            for p in payments
        ]
    }