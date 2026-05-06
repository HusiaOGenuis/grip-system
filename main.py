from dotenv import load_dotenv
load_dotenv()

from preflight import run_preflight
run_preflight()
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os
import requests

print("=== RUNNING THIS FILE ===")
print("RUNNING CORRECT MAIN.PY")
print("=== THIS IS THE ACTIVE MAIN.PY ===")

app = FastAPI()
from dotenv import load_dotenv
load_dotenv()
# ---------- ENV VALIDATION ----------
REQUIRED_ENV = [
    "SUPABASE_URL",
    "SUPABASE_SERVICE_ROLE_KEY",
    "SUPABASE_BUCKET"
]

missing = [k for k in REQUIRED_ENV if not os.getenv(k)]
if missing:
    raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET")


# ---------- REQUEST MODEL ----------
class SignRequest(BaseModel):
    filename: str
    expires_in: int = 60


# ---------- HEALTH ----------
@app.get("/health")
def health():
    return {"status": "ok"}


# ---------- SIGN UPLOAD ----------
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