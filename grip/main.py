import os
import requests
from typing import Dict, Any
from dotenv import load_dotenv

from fastapi import FastAPI, HTTPException, Body
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client, Client

# -----------------------------
# LOAD ENV
# -----------------------------
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing Supabase configuration")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# -----------------------------
# APP INIT
# -----------------------------
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# LOGIN (POST ONLY)
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
            detail=data.get("error_description", "Invalid credentials")
        )

    return {"access_token": data.get("access_token")}

# -----------------------------
# DECISION (TEST ENDPOINT)
# -----------------------------
@app.post("/decision")
def decision(payload: dict):
    return {
        "verdict": "APPROVED",
        "rationale": f"Score {payload.get('score')} accepted"
    }

# -----------------------------
# LANDING PAGE
# -----------------------------
@app.get("/", response_class=HTMLResponse)
def home():
    with open("index.html") as f:
        return f.read()
