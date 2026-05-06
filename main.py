# main.py

from dotenv import load_dotenv
load_dotenv(".env")

# -----------------------------------
# Preflight (FAIL FAST)
# -----------------------------------
from preflight import run_preflight
run_preflight()

# -----------------------------------
# OpenTelemetry
# -----------------------------------
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

trace.set_tracer_provider(TracerProvider())
trace.get_tracer_provider().add_span_processor(
    BatchSpanProcessor(ConsoleSpanExporter())
)
tracer = trace.get_tracer(__name__)

# -----------------------------------
# Framework imports
# -----------------------------------
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
import os
import re
import uuid
import time
import requests
from pathlib import Path
from typing import Dict, Tuple

# -----------------------------------
# Analysis imports (MATCHES analysis.py)
# -----------------------------------
from analysis import fetch_csv, analyze_dataframe

# -----------------------------------
# App init
# -----------------------------------
app = FastAPI()

# -----------------------------------
# Environment
# -----------------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY or not SUPABASE_BUCKET:
    raise RuntimeError("Critical env vars missing")

# -----------------------------------
# Policies
# -----------------------------------
CSV_FILENAME_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,60}\.csv$")
RATE_WINDOW = 60
RATE_MAX = 10
_rate_limit: Dict[Tuple[str, str], list] = {}

# -----------------------------------
# Models
# -----------------------------------
class SignRequest(BaseModel):
    user_id: str
    filename: str
    expires_in: int = 60

# -----------------------------------
# Helpers
# -----------------------------------
def headers():
    return {
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Content-Type": "application/json",
    }

def sanitize(filename: str) -> str:
    name = Path(filename).name.lower()
    if not CSV_FILENAME_RE.fullmatch(name):
        raise HTTPException(400, "Invalid CSV filename")
    return name

def rate_limit(user_id: str, ip: str):
    now = time.time()
    key = (user_id, ip)
    hits = [t for t in _rate_limit.get(key, []) if now - t < RATE_WINDOW]
    if len(hits) >= RATE_MAX:
        raise HTTPException(429, "Rate limit exceeded")
    hits.append(now)
    _rate_limit[key] = hits

# -----------------------------------
# Health
# -----------------------------------
@app.get("/health")
def health():
    return {"status": "ok"}

# -----------------------------------
# Signed upload
# -----------------------------------
@app.post("/sign-upload")
def sign_upload(req: SignRequest, request: Request):
    with tracer.start_as_current_span("sign_upload"):
        ip = request.client.host if request.client else "unknown"
        rate_limit(req.user_id, ip)

        filename = sanitize(req.filename)
        path = f"{SUPABASE_BUCKET}/{req.user_id}/{filename}"

        sign_url = f"{SUPABASE_URL}/storage/v1/object/upload/sign/{path}"

        resp = requests.post(
            sign_url,
            json={"expiresIn": req.expires_in},
            headers=headers(),
            timeout=5,
        )

        if resp.status_code != 200:
            raise HTTPException(500, resp.text)

        signed = resp.json().get("signedURL") or resp.json().get("url")
        if not signed:
            raise HTTPException(500, "Invalid signing response")

        return {
            "upload_url": f"{SUPABASE_URL}/storage/v1{signed}",
            "path": path,
        }

# -----------------------------------
# ✅ FINAL /analyze ENDPOINT (ERROR-FREE)
# -----------------------------------
@app.get("/analyze")
def analyze(path: str, user_id: str):
    try:
        df = fetch_csv(path)

        result = analyze_dataframe(
            df,
            user_id=user_id,
            object_path=path,
        )

        return {
            "status": "success",
            "analysis": result,
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "path": path
        }
@app.post("/ask")
def ask(question: str, user_id: str):
    """
    Semantic RAG over uploaded datasets.
    """
    query_vec = generate_real_embedding(question)

    matches = retrieve_similar_datasets(query_vec)

    context = [
        f"Dataset {m['dataset_hash']} (similarity {m['similarity']:.3f})"
        for m in matches
    ]

    answer = generate_answer(question, context)

    return {
        "question": question,
        "answer": answer,
        "sources": matches,
    }
