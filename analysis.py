import os
import uuid
import hashlib
import requests
import pandas as pd
import numpy as np
from io import StringIO
from typing import Dict, Any

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

HEADERS = {
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Content-Type": "application/json",
}

# -------------------------
# FETCH
# -------------------------
def fetch_csv(object_path: str) -> pd.DataFrame:
    url = f"{SUPABASE_URL}/storage/v1/object/{object_path}"
    resp = requests.get(url, headers=HEADERS, timeout=5)

    if resp.status_code != 200:
        raise RuntimeError(f"Failed to fetch CSV: {resp.text}")

    return pd.read_csv(StringIO(resp.text))


# -------------------------
# SMART TYPE INFERENCE (FIXED)
# -------------------------
def normalize_types(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        s = df[col]

        if s.dtype == object:

            # 🔹 Attempt direct numeric conversion first
            direct = pd.to_numeric(s, errors="coerce")

            direct_ratio = direct.notna().mean()

            # 🔹 If direct works → use it
            if direct_ratio > 0.5:
                df[col] = direct
                continue

            # 🔹 Otherwise clean then retry
            cleaned = (
                s.astype(str)
                .str.replace(",", "")
                .str.replace("R", "", regex=False)
                .str.replace(" ", "")
            )

            converted = pd.to_numeric(cleaned, errors="coerce")
            cleaned_ratio = converted.notna().mean()

            if cleaned_ratio > 0.3:
                df[col] = converted

    return df


# -------------------------
# HASH
# -------------------------
def dataset_hash(df: pd.DataFrame) -> str:
    return hashlib.sha256(
        pd.util.hash_pandas_object(df, index=True).values
    ).hexdigest()


# -------------------------
# PROFILE
# -------------------------
def profile_columns(df: pd.DataFrame) -> Dict[str, Any]:
    profile = {}

    for col in df.columns:
        s = df[col]

        info = {
            "dtype": str(s.dtype),
            "null_ratio": float(s.isna().mean()),
            "unique": int(s.nunique()),
        }

        if pd.api.types.is_numeric_dtype(s):
            info.update({
                "type": "numeric",
                "mean": float(s.mean()) if not s.dropna().empty else None,
                "std": float(s.std()) if not s.dropna().empty else None,
                "min": float(s.min()) if not s.dropna().empty else None,
                "max": float(s.max()) if not s.dropna().empty else None,
                "skew": float(s.skew()) if not s.dropna().empty else None,
            })
        else:
            info["type"] = "categorical"

        profile[col] = info

    return profile


# -------------------------
# ANOMALIES
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
# CORRELATION
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
                pairs[f"{i} ↔ {j}"] = float(val)

    return pairs


# -------------------------
# EXPLAIN
# -------------------------
def explain(df, profile, anomalies, correlations):
    parts = []

    parts.append(f"Dataset has {len(df)} rows and {len(df.columns)} columns.")

    if anomalies:
        parts.append(f"Anomalies in: {list(anomalies.keys())}.")
    else:
        parts.append("No significant anomalies detected.")

    numeric_cols = [
        col for col, p in profile.items()
        if p["type"] == "numeric"
    ]

    if numeric_cols:
        parts.append(f"Numeric columns detected: {numeric_cols}.")

    if correlations:
        parts.append(f"Strong correlations: {list(correlations.keys())[:3]}.")

    return " ".join(parts)


# -------------------------
# MAIN
# -------------------------
def analyze_dataframe(
    df: pd.DataFrame,
    *,
    user_id: str,
    object_path: str,
) -> Dict[str, Any]:

    df = normalize_types(df)

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