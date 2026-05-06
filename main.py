print("=== RUNNING THIS FILE ===")
print("RUNNING CORRECT MAIN.PY")
print("=== THIS IS THE ACTIVE MAIN.PY ===")
import os
import requests
import tempfile
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv

# =========================
# LOAD ENV
# =========================
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY or not SUPABASE_BUCKET:
    raise RuntimeError("Missing required env vars")

app = FastAPI()

# =========================
# MODELS
# =========================

class SignUploadRequest(BaseModel):
    filename: str
    expires_in: int = 60

class AnalyzeRequest(BaseModel):
    filename: str  # e.g. "test.csv"

# =========================
# SIGNED UPLOAD
# =========================

import requests

@app.post("/sign-upload")
def sign_upload(req: SignRequest):
    object_path = f"{SUPABASE_BUCKET}/{req.filename}"

    url = f"{SUPABASE_URL}/storage/v1/object/sign/{object_path}"

    headers = {
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "expiresIn": req.expires_in
    }

    resp = requests.post(url, json=payload, headers=headers)

    if resp.status_code != 200:
        raise HTTPException(status_code=500, detail=resp.text)

    data = resp.json()

    upload_url = f"{SUPABASE_URL}/storage/v1{data['signedURL']}"

    return {
        "upload_url": upload_url,
        "path": object_path,
        "expires_in": req.expires_in
    }
# =========================
# ANALYZE FILE (AI STEP)
# =========================

@app.post("/analyze")
def analyze(req: AnalyzeRequest):
    object_path = f"{SUPABASE_BUCKET}/{req.filename}"

    # 🔐 Create signed DOWNLOAD URL
    sign_url = f"{SUPABASE_URL}/storage/v1/object/sign/{object_path}"

    headers = {
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
    }

    payload = {"expiresIn": 60}

    sign_resp = requests.post(sign_url, json=payload, headers=headers)

    if sign_resp.status_code != 200:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to sign download URL: {sign_resp.text}"
        )

    signed = sign_resp.json()
    download_url = f"{SUPABASE_URL}{signed['signedURL']}"

    # 📥 Download file
    file_resp = requests.get(download_url)

    if file_resp.status_code != 200:
        raise HTTPException(
            status_code=500,
            detail="Failed to download file"
        )

    # 💾 Save temp file
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        tmp.write(file_resp.content)
        tmp_path = tmp.name

    # 🧠 AI ANALYSIS (simple but powerful)
    try:
        df = pd.read_csv(tmp_path)

        insight = {
            "rows": int(df.shape[0]),
            "columns": list(df.columns),
            "column_count": int(df.shape[1]),
            "summary": df.describe(include="all").fillna("").to_dict()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")

    return {
        "file": req.filename,
        "insight": insight
    }

# =========================
# HEALTH
# =========================

@app.get("/health")
def health():
    return {"status": "ok"}