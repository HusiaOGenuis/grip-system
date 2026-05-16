import os
import requests
from fastapi import FastAPI, HTTPException, Body, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from typing import Dict, Any
from supabase import create_client, Client

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GRIP_API_KEY = os.getenv("GRIP_API_KEY")

# ✅ Hard fail early if missing
if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE configuration")

if not GRIP_API_KEY:
    raise RuntimeError("Missing GRIP_API_KEY")

# ✅ Single global client (clean + stable)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# -----------------------------
# App init
# -----------------------------
app = FastAPI(
    title="GRIP Systems",
    version="v1.0.0",
    description="Decision-grade governance engine"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# Security helpers
# -----------------------------
def require_api_key(x_api_key: str = Header(..., alias="X-API-Key")):
    if x_api_key != GRIP_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")

# -----------------------------
# Health
# -----------------------------
@app.get("/health")
def health():
    return {"status": "ok"}

# -----------------------------
# Login (Supabase Auth)
# -----------------------------
@app.post("/login")
def login(payload: dict = Body(...)):
    email = payload.get("email")
    password = payload.get("password")

    res = requests.post(
        f"{SUPABASE_URL}/auth/v1/token?grant_type=password",
        headers={
            "Content-Type": "application/json",
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
        },
        json={
            "email": email,
            "password": password,
        },
    )

    data = res.json()

    if res.status_code != 200:
        raise HTTPException(
            status_code=401,
            detail=data.get("error_description", "Invalid credentials"),
        )

    return {
        "access_token": data.get("access_token")
    }

# -----------------------------
# Logs
# -----------------------------
@app.get("/logs")
def get_logs():
    res = (
        supabase
        .table("decision_logs")
        .select("*")
        .order("created_at", desc=True)
        .execute()
    )
    return res.data
