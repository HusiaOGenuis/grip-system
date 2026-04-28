from fastapi import FastAPI, UploadFile, File, Depends
from fastapi.staticfiles import StaticFiles
import uuid, csv, io

from psycopg.types.json import Json

from .db import get_conn
from .deps import get_current_user, get_dataset
from .models import DatasetOut, AnalysisOut
from .services import analyze_data
from .payments import initialize_payment, verify_payment
app = FastAPI()

# -----------------------------
# INIT DB
# -----------------------------
with get_conn() as conn:
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            api_key TEXT PRIMARY KEY,
            email TEXT UNIQUE
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
CREATE TABLE IF NOT EXISTS payments (
    id TEXT PRIMARY KEY,
    api_key TEXT,
    reference TEXT,
    status TEXT
);

CREATE TABLE IF NOT EXISTS reports (
    id TEXT PRIMARY KEY,
    dataset_id TEXT,
    api_key TEXT,
    report_json JSONB
);
# -----------------------------
# ROOT
# -----------------------------
@app.get("/")
def root():
    return {"status": "GRIP API running"}

@app.get("/version")
def version():
    return {"version": "CONTRACT_BUILD_V1"}

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
# UPLOAD (STRICT)
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
        raise Exception("EMPTY_CSV")

    dataset_id = str(uuid.uuid4())

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO datasets (id, api_key, filename, parsed_json)
                VALUES (%s, %s, %s, %s)
            """, (dataset_id, api_key, file.filename, Json(parsed)))

    return {"dataset_id": dataset_id}

# -----------------------------
# ANALYZE (CONTRACT LOCKED)
# -----------------------------
@app.post("/analyze", response_model=AnalysisOut)
def analyze(dataset=Depends(get_dataset)):
    result = analyze_data(dataset["data"])

    return {
        "dataset_id": dataset["id"],
        **result
    }

# -----------------------------
# FRONTEND
# -----------------------------
app.mount("/", StaticFiles(directory="static", html=True), name="static")
# -----------------------------
# Payment Init
# -----------------------------
@app.post("/pay")
def pay(api_key: str = Depends(get_current_user)):
    url = initialize_payment(api_key, "user@email.com")
    return {"payment_url": url}
# -----------------------------
# Payment Verify
# -----------------------------
@app.get("/verify-payment")
def verify(reference: str):
    status = verify_payment(reference)
    return {"status": status}