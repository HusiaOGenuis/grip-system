import os
import requests
from fastapi import FastAPI, HTTPException, Body
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

# -----------------------------
# ENV
# -----------------------------
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing Supabase configuration")

# -----------------------------
# APP
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

        <p>
        A governance and risk intelligence platform for decision evaluation.
        </p>

        /appOpen App</a>

        <hr>

        <h3>Company</h3>
        <p>GRIP Systems Ltd</p>
        <p>Email: support@grip-systems.com</p>
        <p>Location: Pretoria</p>

        /termsTerms</a> |
        /privacyPrivacy</a>
    </body>
    </html>
    """

# -----------------------------
# SERVE FRONTEND
# -----------------------------
@app.get("/app")
def app_page():
    return FileResponse("index.html")

# -----------------------------
# LOGIN
# -----------------------------
@app.post("/login")
def login(payload: dict = Body(...)):
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
        raise HTTPException(status_code=401, detail=data)

    return {"access_token": data.get("access_token")}

# -----------------------------
# DECISION
# -----------------------------
@app.post("/decision")
def decision(payload: dict):
    score = int(payload.get("score", 0))

    return {
        "verdict": "APPROVED" if score > 50 else "REJECTED",
        "rationale": f"Score evaluated: {score}"
    }

# -----------------------------
# LEGAL
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
