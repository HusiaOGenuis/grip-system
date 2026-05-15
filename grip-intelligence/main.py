from fastapi import FastAPI, HTTPException
from typing import Dict, Any

from policies import evaluate_policy
import requests
from fastapi import Body
app = FastAPI(title="GRIP Intelligence Service")


@app.post("/evaluate")
def evaluate(payload: Dict[str, Any]):
    try:
        return evaluate_policy(
            identity=payload["identity"],
            action=payload["action"],
            context=payload["context"],
        )
    except KeyError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Missing required field: {str(e)}",
        )

@app.post("/login")
def login(payload: dict = Body(...)):
    email = payload.get("email")
    password = payload.get("password")

    SUPABASE_URL = "https://lxldqhgevpssgkqtosnz.supabase.co"
    SUPABASE_KEY = "YOUR_ANON_KEY"

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

    if res.status_code != 200:
        return {"detail": "Invalid credentials"}

    return {
        "access_token": res.json().get("access_token")
    }
