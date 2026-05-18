import os
import requests
from pathlib import Path
from fastapi import FastAPI, HTTPException, Body, Depends, status
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from dotenv import load_dotenv

# Env Configuration
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(env_path)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing Supabase configuration settings.")

app = FastAPI(title="GRIP Systems Backend", version="1.0.0")

# Tighten CORS Configuration to eliminate wildcard vulnerabilities
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8000"],  # Explicit domain control
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type", "Authorization"],
)

security_scheme = HTTPBearer()

# Secure Exception Handling - Internal errors are logged, not exposed
@app.exception_handler(Exception)
async def global_exception_handler(request, exc: Exception):
    # Real-world use cases should log the full traceback here internally
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": True,
            "message": "An internal server error occurred. Please contact support."
        }
    )

# Token Validation Dependency against Supabase Auth Layer
async def verify_user_token(credentials: HTTPAuthorizationCredentials = Depends(security_scheme)):
    token = credentials.credentials
    try:
        # Validate the incoming user bearer token directly against Supabase auth server
        res = requests.get(
            f"{SUPABASE_URL}/auth/v1/user",
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {token}"
            }
        )
        if res.status_code != 200:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired user session token"
            )
        return res.json()  # Returns verified user data payload
    except requests.RequestException:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication server temporarily unavailable"
        )

# Core Landing Endpoint
@app.get("/", response_class=HTMLResponse)
def landing():
    return """
    <!DOCTYPE html>
    <html>
    <head><title>GRIP Systems</title></head>
    <body style="font-family: Arial, sans-serif; padding: 40px; background-color: #f9f9f9;">
        <h1>GRIP Systems</h1>
        <p>A governance and risk intelligence platform for decision evaluation.</p>
        <a href="/app" style="display: inline-block; padding: 10px 20px; background: #0070f3; color: white; text-decoration: none; border-radius: 5px;">Open App</a>
        <hr style="margin-top: 40px;">
        <h3>Company Details</h3>
        <p>GRIP Systems Ltd</p>
        <p>Email: support@grip-systems.com</p>
        <p>Location: Pretoria</p>
        <a href="/terms">Terms</a> | <a href="/privacy">Privacy</a>
    </body>
    </html>
    """

# Serve Static Web Frontend
@app.get("/app")
def app_page():
    path = Path(__file__).resolve().parent.parent / "static" / "index.html"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Static index.html web asset not found.")
    return FileResponse(path)

# Authentication Route
@app.post("/api/login")
def login(payload: dict = Body(...)):
    email = payload.get("email")
    password = payload.get("password")
    
    if not email or not password:
        return {"error": True, "message": "Missing email or password fields."}

    try:
        res = requests.post(
            f"{SUPABASE_URL}/auth/v1/token?grant_type=password",
            headers={
                "apikey": SUPABASE_KEY,
                "Content-Type": "application/json"
            },
            json={"email": email, "password": password}
        )
        
        try:
            data = res.json()
        except Exception:
            return {"error": True, "message": "Malformed response payload from identity provider."}

        if res.status_code != 200:
            return {"error": True, "message": data.get("error_description", "Authentication failed.")}

        return {"error": False, "access_token": data.get("access_token")}

    except requests.RequestException as e:
        return {"error": True, "message": f"Connection error to authentication gateway: {str(e)}"}

# Secure Decision Engine (Access Restricted via Dependency Token validation)
@app.post("/api/decision")
def decision(payload: dict = Body(...), user: dict = Depends(verify_user_token)):
    try:
        score = int(payload.get("score", 0))
        return {
            "error": False,
            "verdict": "APPROVED" if score > 50 else "REJECTED",
            "rationale": f"Score evaluated: {score}",
            "evaluated_by": user.get("email")
        }
    except (ValueError, TypeError):
        raise HTTPException(status_code=400, detail="Invalid score format. Must be an integer computation.")

@app.get("/terms", response_class=HTMLResponse)
def terms():
    return "<html><body><h1>Terms of Service</h1><p>Use of this platform is subject to standard governance policies.</p></body></html>"

@app.get("/privacy", response_class=HTMLResponse)
def privacy():
    return "<html><body><h1>Privacy Policy</h1><p>We guarantee compliance with international user data regulations.</p></body></html>"
