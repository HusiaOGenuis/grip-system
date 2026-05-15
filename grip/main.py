from supabase import create_client
from fastapi import FastAPI, Header, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from grip.grip_intelligence import evaluate_grip_policy
from grip.authz import require_access
from grip.auth import get_current_user

import os
from typing import Dict, Any
from dotenv import load_dotenv

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


load_dotenv()

APP_ISSUER = "governance-risk.com/grip"
APP_VERSION = "v1.0.0"

app = FastAPI(
    title="GRIP Systems",
    version=APP_VERSION,
    description="Decision-grade governance, risk, and integrity engine",
)

# ✅ CORS — required for Carrd (FULLY DECLARED, not patched in later)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # production: narrow this later
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# ---------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------

@app.on_event("startup")
def activate_configuration():
    app.state.config = load_all_configuration()
    print("GRIP configuration loaded successfully")

# ---------------------------------------------------------------------
# API Key Enforcement (SINGLE, CORRECT DEFINITION)
# ---------------------------------------------------------------------

def require_api_key(x_api_key: str = Header(..., alias="X-API-Key")):
    expected = os.environ.get("GRIP_API_KEY")
    if not expected or x_api_key != expected:
        raise HTTPException(
            status_code=403,
            detail="Invalid API key"
        )

# ---------------------------------------------------------------------
# Health Check (INTENTIONALLY UNPROTECTED)
# ---------------------------------------------------------------------

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
# ---------------------------------------------------------------------
# Decision Lifecycle (PROTECTED)
# ---------------------------------------------------------------------
@app.post("/decision")
def decide(
    request: Dict[str, Any],
    identity: dict = Depends(require_access),
):
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    # ---------------------------------------------------------------
    # STEP 4 — GRIP INTELLIGENCE GATE
    # ---------------------------------------------------------------
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

    # ---------------------------------------------------------------
    # EXISTING GRIP DECISION LOGIC (UNCHANGED)
    # ---------------------------------------------------------------
    try:
        base_result = make_decision(
            request=request,
            config=app.state.config,
        )

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
            remediation=base_result.remediation,
            confidence=confidence,
            envelope=envelope,
        )

        write_decision_record(
            request=request,
            result=final_result.to_record(),
            config_snapshot=app.state.config,
            engine_version=APP_VERSION,
        )

        return final_result.to_record()

    except DecisionPause as pause:
        paused_result = DecisionResult.new(
            verdict="ESCALATE",
            rationale=str(pause),
        )

        write_decision_record(
            request=request,
            result=paused_result.to_record(),
            config_snapshot=app.state.config,
            engine_version=APP_VERSION,
        )

        return paused_result.to_record()
# ---------------------------------------------------------------------
# Override Lifecycle (PROTECTED)
# ---------------------------------------------------------------------

@app.post("/override")
def override_decision(
    payload: Dict[str, Any],
    _: None = Depends(require_api_key),
):
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
        return {
            "status": "OVERRIDE_REJECTED",
            "reason": str(e),
        }

# ---------------------------------------------------------------------
# Effective Decision Resolution (PROTECTED)
# ---------------------------------------------------------------------

@app.get("/decision/{trace_id}/effective")
def get_effective_decision(
    trace_id: str,
    _: None = Depends(require_api_key),
):

# ---------------------------------------------------------------------
# login endpoint 
# ---------------------------------------------------------------------
from fastapi import HTTPException

@app.post("/login")
def login(payload: dict):
    email = payload.get("email")
    password = payload.get("password")

    SUPABASE_URL = "https://lxldqhgevpssgkqtosnz.supabase.co"
    SUPABASE_KEY = "SUPABASE_ANON_KEY =eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imx4bGRxaGdldnBzc2drcXRvc256Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3Nzc1MDEwOTYsImV4cCI6MjA5MzA3NzA5Nn0.Gngx4y6VMrOBISyICnA-pcHpp3NIWq_VITKOTskg7MQ"

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

    print("LOGIN RESPONSE:", data)  # ✅ DEBUG

    if res.status_code != 200:
        raise HTTPException(
            status_code=401,
            detail=data.get("error_description", "Invalid credentials")
        )

    return {
        "access_token": data.get("access_token")
    }
    
@app.get("/logs")
def get_logs():
    res = supabase.table("decision_logs").select("*").order("created_at", desc=True).execute()
    return res.data