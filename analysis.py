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
# HASH
# -------------------------
def dataset_hash(df: pd.DataFrame) -> str:
    return hashlib.sha256(
        pd.util.hash_pandas_object(df, index=True).values
    ).hexdigest()


# -------------------------
# COLUMN PROFILING
# -------------------------
def profile_columns(df: pd.DataFrame) -> Dict[str, Any]:
    profile = {}

    for col in df.columns:
        s = df[col]

        col_info = {
            "dtype": str(s.dtype),
            "null_ratio": float(s.isna().mean()),
            "unique": int(s.nunique()),
        }

        if pd.api.types.is_numeric_dtype(s):
            col_info.update({
                "type": "numeric",
                "mean": float(s.mean()) if not s.dropna().empty else None,
                "std": float(s.std()) if not s.dropna().empty else None,
                "min": float(s.min()) if not s.dropna().empty else None,
                "max": float(s.max()) if not s.dropna().empty else None,
                "skew": float(s.skew()) if not s.dropna().empty else None,
            })

        elif pd.api.types.is_datetime64_any_dtype(s):
            col_info["type"] = "datetime"

        else:
            col_info["type"] = "categorical"

        profile[col] = col_info

    return profile


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
# CORRELATION DETECTION
# -------------------------
def detect_correlations(df: pd.DataFrame) -> Dict[str, float]:
    numeric = df.select_dtypes(include=[np.number])

    if numeric.shape[1] < 2:
        return {}

    corr = numeric.corr()
    pairs = {}

    for i in corr.columns:
        for j in corr.columns:
            if i >= j:
                continue

            val = corr.loc[i, j]

            if abs(val) > 0.7:
                key = f"{i} ↔ {j}"
                pairs[key] = float(val)

    return pairs


# -------------------------
# EXPLANATION ENGINE
# -------------------------
def explain(df, profile, anomalies, correlations) -> str:
    parts = []

    parts.append(f"Dataset has {len(df)} rows and {len(df.columns)} columns.")

    # anomalies
    if anomalies:
        cols = list(anomalies.keys())
        parts.append(f"Anomalies detected in columns: {cols}.")
    else:
        parts.append("No significant anomalies detected.")

    # skew detection
    skewed = [
        col for col, info in profile.items()
        if info.get("type") == "numeric" and info.get("skew") and abs(info["skew"]) > 1
    ]
    if skewed:
        parts.append(f"Skewed distributions observed in: {skewed}.")

    # correlations
    if correlations:
        top = list(correlations.keys())[:3]
        parts.append(f"Strong correlations found: {top}.")

    return " ".join(parts)


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

    profile = profile_columns(df)
    anomalies = detect_anomalies(df)
    correlations = detect_correlations(df)
    explanation = explain(df, profile, anomalies, correlations)

    return {
        "request_id": request_id,
        "dataset_hash": h,
        "profile": profile,
        "anomalies": anomalies,
        "correlations": correlations,
        "explanation": explanation,
    }