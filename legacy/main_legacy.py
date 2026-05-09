from fastapi import FastAPI
from dotenv import load_dotenv
import os

load_dotenv()

APP_ISSUER = "governance-risk.com/grip"
APP_VERSION = "v0.1.0"

app = FastAPI(
    title="GRIP Systems",
    version=APP_VERSION,
    description="Decision-grade governance, risk, and integrity engine",
)

@app.get("/health")
def health_check():
    return {
        "status": "ok",
        "issuer": APP_ISSUER,
        "version": APP_VERSION,
    }
