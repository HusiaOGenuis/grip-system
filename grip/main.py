import os
import requests
from pathlib import Path
from fastapi import FastAPI, HTTPException, Body
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

# -----------------------------
# ENV CONFIGURATION
# -----------------------------
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing Supabase configuration")

# -----------------------------
# APP CONFIGURATION
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
# LANDING PAGE
# -----------------------------
@app.get("/", response_class=HTMLResponse)
def landing():
    return """
    <html>
    <body style="font-family: Arial; padding: 40px;">
        <h1>GRIP Systems</h1>
        <p>A governance and risk intelligence platform for decision evaluation.</p>
        <a href="/app">Open App</a>
        <hr>
        <h3>Company</h3>
        <p>GRIP Systems Ltd</p>
        <p>Email: support@grip-systems.com</p>
        <p>Location: Pretoria</p>
        <a href="/terms">Terms</a> | <a href="/privacy">Privacy</a>
    </body>
    </html>
    """

# -----------------------------
# SERVE FRONTEND (FIXED STATIC PATH)
# -----------------------------
@app.get("/app")
def app_page():
    path = Path(__file__).resolve().parent.parent / "static" / "index.html"
    return FileResponse(path)

# -----------------------------
# AUTHENTICATION (EMAIL/PASSWORD ONLY)
# -----------------------------
@app.post("/login")
def login(payload: dict = Body(...)):
    try:
        res = requests.post(
            f"{SUPABASE_URL}/auth/v1/token?grant_type=password",
            headers={
                "Content-Type": "application/json",
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
            },
            json={
                "email": payload.get("email"),
                "password": payload.get("password"),
            },
        )

        data = res.json()

        if res.status_code != 200:
            return {
                "error": True,
                "message": data.get("error_description", "Invalid credentials")
            }

        return {
            "error": False,
            "access_token": data.get("access_token")
        }

    except Exception as e:
        return {
            "error": True,
            "message": "Server error: " + str(e)
        }
# -----------------------------
# DECISION CORE ENGINE
# -----------------------------
@app.post("/decision")
def decision(payload: dict):
    try:
        score = int(payload.get("score", 0))
        return {
            "verdict": "APPROVED" if score > 50 else "REJECTED",
            "rationale": f"Score evaluated: {score}"
        }
    except Exception as e:
        return {
            "error": True,
            "message": str(e)
        }

# -----------------------------
# LEGAL POLICIES
# -----------------------------
@app.get("/terms", response_class=HTMLResponse)
def terms():
    return """
    <h1>Terms of Service</h1>
    <p>Use of this platform is subject to policy conditions.</p>
    """

@app.get("/privacy", response_class=HTMLResponse)
def privacy():
    return """
    <h1>Privacy Policy</h1>
    <p>We do not sell user data.</p>
    """

# -----------------------------
# SYSTEM INSPECTOR
# -----------------------------
@app.get("/__structure")
def show_structure():
    structure = []
    for root, dirs, files in os.walk(".", topdown=True):
        structure.append({
            "root": root,
            "dirs": dirs,
            "files": files
        })
    return {
        "cwd": os.getcwd(),
        "structure": structure
    }
