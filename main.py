import os
import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv

# =========================
# LOAD ENV FILE (CRITICAL)
# =========================
load_dotenv()

# =========================
# ENV VALIDATION (FAIL FAST)
# =========================

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET")

missing = []

if not SUPABASE_URL:
    missing.append("SUPABASE_URL")

if not SUPABASE_SERVICE_ROLE_KEY:
    missing.append("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_BUCKET:
    missing.append("SUPABASE_BUCKET")

if missing:
    raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

# =========================
# FASTAPI INIT
# =========================

app = FastAPI()

# =========================
# REQUEST MODEL
# =========================

class SignUploadRequest(BaseModel):
    filename: str
    expires_in: int = 60

# =========================
# SIGNED URL ENDPOINT
# =========================

@app.post("/sign-upload")
def sign_upload(req: SignUploadRequest):

    object_path = f"{SUPABASE_BUCKET}/{req.filename}"

    url = f"{SUPABASE_URL}/storage/v1/object/sign/{object_path}"

    headers = {
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
    }

    payload = {
        "expiresIn": req.expires_in
    }

    resp = requests.post(url, json=payload, headers=headers)

    if resp.status_code != 200:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to sign upload URL: {resp.text}"
        )

    data = resp.json()

    return {
        "upload_url": f"{SUPABASE_URL}{data['signedURL']}",
        "path": object_path,
        "expires_in": req.expires_in
    }

# =========================
# HEALTH CHECK
# =========================

@app.get("/health")
def health():
    return {"status": "ok"}