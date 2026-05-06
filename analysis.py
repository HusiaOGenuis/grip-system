import os
import uuid
import hashlib
import requests
import pandas as pd
import numpy as np
from io import StringIO
from typing import Dict, Any

# -------------------------
# ENV
# -------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

HEADERS = {
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Content-Type": "application/json",
}

# -------------------------
# FETCH CSV
# -------------------------
def fetch_csv(object_path: str) -> pd.DataFrame:
    url = f"{SUPABASE_URL}/storage/v1/object/{object_path}"
    resp = requests.get(url, headers=HEADERS, timeout=5)

    if resp.status_code != 200:
        raise RuntimeError(f"Failed to fetch CSV: {resp.text}")

    return pd.read_csv(StringIO(resp.text))


# -------------------------
# HASH DATASET
# -------------------------
def dataset_hash(df: pd.DataFrame) -> str:
    return hashlib.sha256(
        pd.util.hash_pandas_object(df, index=True).values
    ).hexdigest()


# -------------------------
# ANOMALY DETECTION
# -------------------------
def detect_anomalies(df: pd.DataFrame) -> Dict[str, Any]:
    numeric = df.select_dtypes(include=[np.number])
    anomalies = {}

    for col in numeric.columns:
        s = numeric[col].dropna()
        if s.empty:
            continue

        std = s.std()
        if std == 0:
            continue

        z = (s - s.mean()) / std
        outliers = s[abs(z) > 3]

        if not outliers.empty:
            anomalies[col] = {
                "count": int(len(outliers)),
                "rows": outliers.index.tolist(),
            }

    return anomalies


# -------------------------
# EXPLANATION ENGINE
# -------------------------
def explain(df: pd.DataFrame, anomalies: Dict[str, Any]) -> str:
    if anomalies:
        cols = list(anomalies.keys())
        return f"Dataset has {len(df)} rows with anomalies in columns: {cols}."
    return f"Dataset has {len(df)} rows with no significant anomalies."


# -------------------------
# MAIN ANALYSIS
# -------------------------
def analyze_dataframe(
    df: pd.DataFrame,
    *,
    user_id: str,
    object_path: str,
) -> Dict[str, Any]:

    request_id = str(uuid.uuid4())

    h = dataset_hash(df)
    anomalies = detect_anomalies(df)
    explanation = explain(df, anomalies)

    return {
        "request_id": request_id,
        "dataset_hash": h,
        "anomalies": anomalies,
        "explanation": explanation,
    }