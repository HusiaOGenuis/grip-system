import numpy as np
# main.py

from dotenv import load_dotenv
load_dotenv(".env")

# -----------------------------------
# Preflight (FAIL FAST)
# -----------------------------------
from preflight import run_preflight
run_preflight()

# -----------------------------------
# OpenTelemetry
# -----------------------------------
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

trace.set_tracer_provider(TracerProvider())
trace.get_tracer_provider().add_span_processor(
    BatchSpanProcessor(ConsoleSpanExporter())
)
tracer = trace.get_tracer(__name__)

# -----------------------------------
# Framework imports
# -----------------------------------
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
import os
import re
import uuid
import time
import requests
from pathlib import Path
from typing import Dict, Tuple

# -----------------------------------
# Analysis imports (MATCHES analysis.py)
# -----------------------------------
from analysis import fetch_csv, analyze_dataframe, dataset_hash
import analysis
print("🔥 USING ANALYSIS FILE:", analysis.__file__)

from analysis import (
    fetch_csv,
    analyze_dataframe,
    dataset_hash,
    impact_analysis
)

# -----------------------------------
# App init
# -----------------------------------
app = FastAPI()

# -----------------------------------
# Environment
# -----------------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY or not SUPABASE_BUCKET:
    raise RuntimeError("Critical env vars missing")

# -----------------------------------
# Policies
# -----------------------------------
CSV_FILENAME_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,60}\.csv$")
RATE_WINDOW = 60
RATE_MAX = 10
_rate_limit: Dict[Tuple[str, str], list] = {}

# -----------------------------------
# Models
# -----------------------------------
class SignRequest(BaseModel):
    user_id: str
    filename: str
    expires_in: int = 60

# -----------------------------------
# Helpers
# -----------------------------------
def headers():
    return {
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Content-Type": "application/json",
    }

def sanitize(filename: str) -> str:
    name = Path(filename).name.lower()
    if not CSV_FILENAME_RE.fullmatch(name):
        raise HTTPException(400, "Invalid CSV filename")
    return name

def rate_limit(user_id: str, ip: str):
    now = time.time()
    key = (user_id, ip)
    hits = [t for t in _rate_limit.get(key, []) if now - t < RATE_WINDOW]
    if len(hits) >= RATE_MAX:
        raise HTTPException(429, "Rate limit exceeded")
    hits.append(now)
    _rate_limit[key] = hits

# -----------------------------------
# Health
# -----------------------------------
@app.get("/health")
def health():
    return {"status": "ok"}

# -----------------------------------
# Signed upload
# -----------------------------------
@app.post("/sign-upload")
def sign_upload(req: SignRequest, request: Request):
    with tracer.start_as_current_span("sign_upload"):
        ip = request.client.host if request.client else "unknown"
        rate_limit(req.user_id, ip)

        filename = sanitize(req.filename)
        path = f"{SUPABASE_BUCKET}/{req.user_id}/{filename}"

        sign_url = f"{SUPABASE_URL}/storage/v1/object/upload/sign/{path}"

        resp = requests.post(
            sign_url,
            json={"expiresIn": req.expires_in},
            headers=headers(),
            timeout=5,
        )

        if resp.status_code != 200:
            raise HTTPException(500, resp.text)

        signed = resp.json().get("signedURL") or resp.json().get("url")
        if not signed:
            raise HTTPException(500, "Invalid signing response")

        return {
            "upload_url": f"{SUPABASE_URL}/storage/v1{signed}",
            "path": path,
        }

# -----------------------------------
# ✅ FINAL /analyze ENDPOINT (ERROR-FREE)
# -----------------------------------
@app.get("/analyze")
def analyze(path: str, user_id: str):
    try:
        df = fetch_csv(path)

        result = analyze_dataframe(
            df,
            user_id=user_id,
            object_path=path,
        )

        return {
            "status": "success",
            "analysis": result,
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "path": path
        }
@app.post("/ask")
def ask(question: str, user_id: str):
    """
    Semantic RAG over uploaded datasets.
    """
    query_vec = generate_real_embedding(question)

    matches = retrieve_similar_datasets(query_vec)

    context = [
        f"Dataset {m['dataset_hash']} (similarity {m['similarity']:.3f})"
        for m in matches
    ]

    answer = generate_answer(question, context)

    return {
        "question": question,
        "answer": answer,
        "sources": matches,
    }
@app.get("/compare")
def compare(path1: str, path2: str, user_id: str):

    try:
        df1 = fetch_csv(path1)
        df2 = fetch_csv(path2)

        rows1, rows2 = len(df1), len(df2)
        cols1, cols2 = set(df1.columns), set(df2.columns)

        common_cols = sorted(list(cols1 & cols2))

        df1c = df1[common_cols].copy().reset_index(drop=True).fillna("")
        df2c = df2[common_cols].copy().reset_index(drop=True).fillna("")

        # Hash
        h1 = dataset_hash(df1c)
        h2 = dataset_hash(df2c)
        identical = h1 == h2

        # Row diff
        try:
            s1 = set(df1c.astype(str).agg("|".join, axis=1))
            s2 = set(df2c.astype(str).agg("|".join, axis=1))
            rows_added = len(s2 - s1)
            rows_removed = len(s1 - s2)
        except Exception:
            rows_added = None
            rows_removed = None

        # Cell diff
        cell_changes = {}
        if df1c.shape == df2c.shape:
            diff_mask = (df1c != df2c)
            for col in common_cols:
                changed = int(diff_mask[col].sum())
                if changed > 0:
                    cell_changes[col] = changed

        # Drift
        drift = {}
        numeric_cols = df1c.select_dtypes(include=[np.number]).columns

        for col in numeric_cols:
            try:
                mean1 = df1c[col].mean()
                mean2 = df2c[col].mean()

                if pd.notna(mean1) and pd.notna(mean2):
                    diff = abs(mean1 - mean2)

                    drift[col] = {
                        "mean_1": float(mean1),
                        "mean_2": float(mean2),
                        "difference": float(diff),
                        "drift": diff > (0.1 * abs(mean1)) if mean1 != 0 else diff > 0
                    }
            except Exception:
                continue

        # Impact analysis
        try:
            analysis1 = analyze_dataframe(df1, user_id=user_id, object_path=path1)
            analysis2 = analyze_dataframe(df2, user_id=user_id, object_path=path2)

            impact = impact_analysis(analysis1, analysis2)

            if not impact:
                impact = {"note": "Impact analysis returned empty result"}

        except Exception as e:
            impact = {"error": str(e)}

        return {
            "status": "success",
            "comparison": {
                "identical": identical,
                "dataset_1_rows": rows1,
                "dataset_2_rows": rows2,
                "row_difference": rows1 - rows2,
                "rows_added": rows_added,
                "rows_removed": rows_removed,
                "columns_only_in_1": list(cols1 - cols2),
                "columns_only_in_2": list(cols2 - cols1),
                "common_columns": common_cols,
                "cell_changes": cell_changes,
                "drift": drift,
                "impact": impact,
                "hash_1": h1,
                "hash_2": h2
            }
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }