# analysis.py

import os
import time
import uuid
import hashlib
import requests
import pandas as pd
import numpy as np
from io import StringIO
from typing import Dict, Any

# ---- Optional OpenTelemetry (safe) ----
try:
    from opentelemetry import trace
    from opentelemetry.trace import Status, StatusCode
    tracer = trace.get_tracer(__name__)
except ImportError:
    from contextlib import nullcontext
    class _NoopTracer:
        def start_as_current_span(self, name):
            return nullcontext()
    tracer = _NoopTracer()
    Status = None
    StatusCode = None

# ---- Env ----
SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

HEADERS = {
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Content-Type": "application/json",
}

# -------------------------
# Embeddings (placeholder)
# -------------------------
def generate_real_embedding(text: str) -> list[float]:
    # Placeholder until real model is plugged in
    return [0.0] * 1536

# -------------------------
# Storage fetch
# -------------------------
def fetch_csv(object_path: str) -> pd.DataFrame:
    with tracer.start_as_current_span("fetch_csv"):
        url = f"{SUPABASE_URL}/storage/v1/object/{object_path}"
        resp = requests.get(url, headers=HEADERS, timeout=5)
        if resp.status_code != 200:
            raise RuntimeError(f"Failed to fetch CSV: {resp.text}")
        return pd.read_csv(StringIO(resp.text))

# -------------------------
# Analysis helpers
# -------------------------
def dataset_hash(df: pd.DataFrame) -> str:
    return hashlib.sha256(
        pd.util.hash_pandas_object(df, index=True).values
    ).hexdigest()

def detect_anomalies(df: pd.DataFrame) -> Dict[str, Any]:
    numeric = df.select_dtypes(include=[np.number])
    anomalies = {}
    for col in numeric.columns:
        s = numeric[col].dropna()
        if s.empty:
            continue
        z = (s - s.mean()) / (s.std() or 1)
        outliers = s[abs(z) > 3]
        if not outliers.empty:
            anomalies[col] = {
                "count": len(outliers),
                "rows": outliers.index.tolist(),
            }
    return anomalies

def explain(df: pd.DataFrame, anomalies: Dict[str, Any]) -> str:
    if anomalies:
        return f"Dataset has {len(df)} rows with anomalies in {list(anomalies.keys())}."
    return f"Dataset has {len(df)} rows with no significant anomalies."

# -------------------------
# Persistence
# -------------------------
def insert_row(table: str, payload: dict):
    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers=HEADERS,
        json=payload,
        timeout=5,
    )
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"Insert into {table} failed: {resp.text}")

# -------------------------
# Main analysis
# -------------------------
def analyze_dataframe(
    df: pd.DataFrame,
    *,
    user_id: str,
    object_path: str,
) -> Dict[str, Any]:

    with tracer.start_as_current_span("analyze_dataframe"):
        request_id = str(uuid.uuid4())
        #ts = int(time.time())

        h = dataset_hash(df)
        anomalies = detect_anomalies(df)
        explanation = explain(df, anomalies)

        # Dataset registry
        insert_row("datasets", {
            "dataset_hash": h,
            "user_id": user_id,
            "object_path": object_path,
            "created_at": ts,
        })

        # Analysis record
        insert_row("analyses", {
            "request_id": request_id,
            "dataset_hash": h,
            "user_id": user_id,
            "created_at": ts,
            "anomalies": anomalies,
            "explanation": explanation,
        })

        # Embedding
        vec = generate_real_embedding(" ".join(df.columns))
        insert_row("embeddings", {
            "dataset_hash": h,
            "kind": "dataset",
            "key": "dataset",
            "embedding_vec": vec,
        })

        return {
            "request_id": request_id,
            "dataset_hash": h,
            "anomalies": anomalies,
            "explanation": explanation,
        }