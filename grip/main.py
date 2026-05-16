import os
from typing import Dict, Any
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Depends, Body
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import requests
from supabase import create_client, Client

# GRIP Core Engine & Policy Imports
from grip.grip_intelligence import evaluate_grip_policy
from grip.authz import require_access
from grip.auth import get_current_user
from grip.engine.config_loader import load_all_configuration
from grip.engine.decision_engine import make_decision, DecisionPause
from grip.engine.decision_result import DecisionResult
from grip.engine.confidence_model import compute_confidence
from grip.engine.decision_envelope import build_envelope
from grip.engine.record_writer import write_decision_record
from grip.engine.override_policy import validate_override, OverrideNotPermitted
from grip.engine.confidence_override_guard import (
    enforce_confidence_override,
    ConfidenceOverrideNotPermitted,
)
from grip.engine.override_writer import write_override_record
from grip.engine.resolution_engine import resolve_effective_verdict

# -----------------------------------------------------------------------------
# ENVIRONMENT INITIALIZATION & ERROR HANDLING
# -----------------------------------------------------------------------------
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GRIP_API_KEY = os.getenv("GRIP_API_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE configuration")
if not GRIP_API_KEY:
    raise RuntimeError("Missing GRIP_API_KEY")

# Global clients and constants
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
APP_ISSUER = "://governance-risk.com"
APP_VERSION = "v1.0.0"

# -----------------------------------------------------------------------------
# FASTAPI APPLICATION SETUP
# -----------------------------------------------------------------------------
app = FastAPI(
    title="GRIP Systems",
    version=APP_VERSION,
    description="Decision-grade governance, risk, and integrity engine",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
def activate_configuration():
    app.state.config = load_all_configuration()
    print("GRIP configuration loaded successfully")

# -----------------------------------------------------------------------------
# SECURITY HELPERS / DEPENDENCIES
# -----------------------------------------------------------------------------
def require_api_key(x_api_key: str = Header(..., alias="X-API-Key")):
    if x_api_key != GRIP_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")

# -----------------------------------------------------------------------------
# SYSTEM ENDPOINTS (Health, Auth, Logging)
# -----------------------------------------------------------------------------
@app.get("/health")
def health_check():
    return {
        "status": "ok",
        "issuer": APP_ISSUER,
        "version": APP_VERSION,
    }

@app.get("/whoami")
def whoami(user=Depends(get_current_user)):
    return {
        "sub": user.get("sub"),
        "email": user.get("email"),
        "role": user.get("role"),
    }

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
    
@app.post("/debug-create-user")
def debug_create_user():
    res = requests.post(
        f"{SUPABASE_URL}/auth/v1/signup",
        headers={
            "Content-Type": "application/json",
            "apikey": SUPABASE_KEY,
        },
        json={
            "email": "testuser123@email.com",
            "password": "StrongPassword123!"
        },
    )

    return res.json()
   

@app.post("/request-password-reset")
def request_reset(payload: dict):
    res = requests.post(
        f"{SUPABASE_URL}/auth/v1/recover",
        headers={
            "Content-Type": "application/json",
            "apikey": SUPABASE_KEY,
        },
        json={"email": payload.get("email")},
    )
    return res.json()

@app.post("/update-password")
def update_password(payload: dict):
    access_token = payload.get("access_token")
    new_password = payload.get("new_password")
    
    res = requests.put(
        f"{SUPABASE_URL}/auth/v1/user",
        headers={
            "Authorization": f"Bearer {access_token}",
            "apikey": SUPABASE_KEY,
            "Content-Type": "application/json",
        },
        json={"password": new_password},
    )
    return res.json()

@app.get("/reset", response_class=HTMLResponse)
def reset_page():
    return """
    <html>
    <body style="font-family: Arial; padding: 40px;">
        <h2>Reset Your Password</h2>

        <input id="password" type="password" placeholder="New password" />
        <button onclick="resetPassword()">Update Password</button>

        <script>
        const hash = window.location.hash.substring(1);
        const params = new URLSearchParams(hash);
        const access_token = params.get("access_token");

        function resetPassword() {
            const newPassword = document.getElementById("password").value;

            fetch("/update-password", {
                method: "POST",
                headers: {
                    "Content-Type": "application/json"
                },
                body: JSON.stringify({
                    access_token: access_token,
                    new_password: newPassword
                })
            })
            .then(res => res.json())
            .then(data => {
                alert("Password updated successfully!");
                window.location.href = "/";
            });
        }
        </script>
    </body>
    </html>
    """

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

# -----------------------------------------------------------------------------
# CORE ENGINE ENDPOINTS (Decisions & Overrides)
# -----------------------------------------------------------------------------
@app.post("/decision")
def decide(request: Dict[str, Any], identity: dict = Depends(require_access)):
    grip_decision = evaluate_grip_policy(
        identity=identity,
        action="decision:create",
        context=request,
    )
    if not grip_decision.allow:
        raise HTTPException(
            status_code=403,
            detail=f"Denied by GRIP intelligence: {grip_decision.reason}",
        )
    try:
        base_result = make_decision(request=request, config=app.state.config)
        confidence = compute_confidence(
            score=request.get("score", 0),
            verdict=base_result.verdict,
            request=request,
        )
        envelope = build_envelope(confidence)
        
        final_result = DecisionResult(
            trace_id=base_result.trace_id,
            verdict=base_result.verdict,
            rationale=base_result.rationale,
            reremediation=base_result.reremediation if hasattr(base_result, 'reremediation') else getattr(base_result, 'reremediation', None),
            remediation=base_result.reremediation if hasattr(base_result, 'reremediation') else getattr(base_result, 'reremediation', None) if not hasattr(base_result, 'reremediation') else base_result.reremediation, 
            confidence=confidence,
            envelope=envelope,
        )
        
        if hasattr(base_result, 'remediation') and not hasattr(final_result, 'reremediation'):
            final_result.reremediation = base_result.reremediation

        write_decision_record(
            request=request,
            result=final_result.to_record(),
            config_snapshot=app.state.config,
            engine_version=APP_VERSION,
        )
        return final_result.to_record()
        
    except DecisionPause as pause:
        paused_result = DecisionResult.new(verdict="ESCALATE", rationale=str(pause))
        write_decision_record(
            request=request,
            result=paused_result.to_record(),
            config_snapshot=app.state.config,
            engine_version=APP_VERSION,
        )
        return paused_result.to_record()

@app.post("/override")
def override_decision(payload: Dict[str, Any], _: None = Depends(require_api_key)):
    try:
        trace_id = payload["trace_id"]
        attestation = payload["attestation"]
        override_verdict = payload["override_verdict"]
        
        effective = resolve_effective_verdict(trace_id)
        
        validate_override(
            original_verdict=effective["effective_verdict"],
            override_verdict=override_verdict,
            attestation=attestation,
            policy=app.state.config["override_policy"],
        )
        enforce_confidence_override(
            confidence=payload["confidence"],
            envelope=payload["envelope"],
            override_verdict=override_verdict,
            attestation=attestation,
            policy=app.state.config["confidence_override_policy"],
        )
        override_id = write_override_record(
            trace_id=trace_id,
            original_verdict=effective["effective_verdict"],
            override_verdict=override_verdict,
            attestation=attestation,
            config_snapshot=app.state.config,
            engine_version=APP_VERSION,
        )
        return {
            "override_id": override_id,
            "trace_id": trace_id,
            "status": "OVERRIDE_RECORDED",
        }
    except (OverrideNotPermitted, ConfidenceOverrideNotPermitted) as e:
        return {"status": "OVERRIDE_REJECTED", "reason": str(e)}

@app.get("/decision/{trace_id}/effective")
def get_effective_decision(trace_id: str, _: None = Depends(require_api_key)):
    return resolve_effective_verdict(trace_id)
    
    
