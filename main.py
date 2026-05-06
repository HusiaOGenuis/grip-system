# -----------------------------------
# Environment loading (local only)
# -----------------------------------
from dotenv import load_dotenv
load_dotenv(".env")

# -----------------------------------
# Preflight gate (FAIL FAST)
# -----------------------------------
from preflight import run_preflight
run_preflight()

# -----------------------------------
# OpenTelemetry (minimal, production-correct)
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
# Standard imports
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
# AI / Analysis imports
# -----------------------------------
from analysis import fetch_csv, analyze_dataframe

# -----------------------------------
# App init
# -----------------------------------
app = FastAPI()

# -----------------------------------
# Environment variables
# -----------------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY or not SUPABASE_BUCKET:
    raise RuntimeError("Critical env vars missing after preflight")

# -----------------------------------
# Policies & limits
# -----------------------------------

CSV_FILENAME_RE = re.compile(r"^[a-z0-9][a-z0-9_\-]{0,60}\.csv$")

RATE_LIMIT_WINDOW = 60      # seconds
RATE_LIMIT_MAX = 10         # requests per window

_rate_limit: Dict[Tuple[str, str], list] = {}

# -----------------------------------
# Models
# -----------------------------------
class SignRequest(BaseModel):
    user_id: str
    filename: str
    expires_in: int = 60

# -----------------------------------
# Utilities
# -----------------------------------
def build_headers():
    return {
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Content-Type": "application/json",
    }

def sanitize_filename(filename: str) -> str:
    name = Path(filename).name.lower()
    if not CSV_FILENAME_RE.fullmatch(name):
        raise HTTPException(
            status_code=400,
            detail="Invalid filename. Must be lowercase CSV with safe characters."
        )
    return name

def enforce_rate_limit(user_id: str, ip: str):
    now = time.time()
    key = (user_id, ip)
    timestamps = _rate_limit.get(key, [])
    timestamps = [t for t in timestamps if now - t < RATE_LIMIT_WINDOW]

    if len(timestamps) >= RATE_LIMIT_MAX:
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

    timestamps.append(now)
    _rate_limit[key] = timestamps

# -----------------------------------
# Health
# -----------------------------------
@app.get("/health")
def health():
    return {"status": "ok"}

# -----------------------------------
# SIGNED UPLOAD ENDPOINT
# -----------------------------------
@app.post("/sign-upload")
def sign_upload(req: SignRequest, request: Request):
    with tracer.start_as_current_span("sign_upload"):
        request_id = str(uuid.uuid4())
        client_ip = request.client.host if request.client else "unknown"

        # Rate limit
        enforce_rate_limit(req.user_id, client_ip)

        # Sanitize filename
        filename = sanitize_filename(req.filename)

        # Per-user path enforcement
        object_path = f"{SUPABASE_BUCKET}/{req.user_id}/{filename}"

        sign_url = (
            f"{SUPABASE_URL}/storage/v1/object/upload/sign/{object_path}"
        )

        payload = {"expiresIn": req.expires_in}

        try:
            resp = requests.post(
                sign_url,
                json=payload,
                headers=build_headers(),
                timeout=5,
            )
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail={"request_id": request_id, "error": str(e)},
            )

        if resp.status_code != 200:
            raise HTTPException(
                status_code=500,
                detail={"request_id": request_id, "error": resp.text},
            )

        data = resp.json()
        signed_path = data.get("signedURL") or data.get("url")

        if not signed_path:
            raise HTTPException(
                status_code=500,
                detail={"request_id": request_id, "error": "Invalid signing response"},
            )

        upload_url = f"{SUPABASE_URL}/storage/v1{signed_path}"

        if "/object/upload/sign/" not in upload_url:
            raise HTTPException(
                status_code=500,
                detail={"request_id": request_id, "error": "Invalid upload URL"},
            )

        return {
            "request_id": request_id,
            "upload_url": upload_url,
            "path": object_path,
            "expires_in": req.expires_in,
        }

# -----------------------------------
# ✅ FINAL /analyze ENDPOINT (LOCKED)
# -----------------------------------
@app.get("/analyze")
def analyze(path: str, user_id: str):
    """
    Full AI analysis pipeline:
    - Fetch CSV from Supabase
    - Run anomaly detection
    - Generate embeddings
    - Produce AI explanation
    - Persist lineage & audit
    - Emit OpenTelemetry spans
    """
    with tracer.start_as_current_span("analyze"):
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