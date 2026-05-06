import os
import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY or not SUPABASE_BUCKET:
    raise RuntimeError("Missing required env vars")

app = FastAPI()

class SignUploadRequest(BaseModel):
    filename: str
    expires_in: int = 60

@app.post("/sign-upload")
def sign_upload(req: SignUploadRequest):

    object_path = f"{SUPABASE_BUCKET}/{req.filename}"

    # ✅ THIS IS THE CRITICAL FIX
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

@app.get("/health")
def health():
    return {"status": "ok"}