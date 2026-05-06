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
# TYPE NORMALIZATION (SAFE)
# -------------------------
def normalize_types(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        s = df[col]
        if s.dtype == object:
            direct = pd.to_numeric(s, errors="coerce")
            if direct.notna().mean() > 0.5:
                df[col] = direct
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
# MIXED TYPE DETECTION
# -------------------------
def detect_mixed_types(df: pd.DataFrame) -> Dict[str, Any]:
    issues = {}

    for col in df.columns:
        s = df[col]

        if s.dtype == object:
            numeric_attempt = pd.to_numeric(s, errors="coerce")
            numeric_ratio = numeric_attempt.notna().mean()

            if 0.1 < numeric_ratio < 0.9:
                issues[col] = {
                    "type": "mixed",
                    "numeric_ratio": float(numeric_ratio),
                    "note": "Column contains both numeric-like and text values"
                }

    return issues


# -------------------------
# DATE VALIDATION
# -------------------------
def detect_date_issues(df: pd.DataFrame) -> Dict[str, Any]:
    issues = {}

    for col in df.columns:
        s = df[col]

        if s.dtype == object:
            parsed = pd.to_datetime(s, errors="coerce")
            ratio = parsed.notna().mean()

            if ratio > 0.5 and ratio < 1.0:
                issues[col] = {
                    "type": "partial_datetime",
                    "parse_ratio": float(ratio),
                    "note": "Some values look like dates but others do not"
                }

    return issues


# -------------------------
# MISSINGNESS
# -------------------------
def detect_missingness(df: pd.DataFrame) -> Dict[str, Any]:
    issues = {}

    for col in df.columns:
        null_ratio = df[col].isna().mean()

        if null_ratio > 0.3:
            issues[col] = {
                "type": "missing_values",
                "null_ratio": float(null_ratio),
                "note": "High proportion of missing values"
            }

    return issues


# -------------------------
# DATA QUALITY ENGINE
# -------------------------
def data_quality_report(df: pd.DataFrame) -> Dict[str, Any]:
    mixed = detect_mixed_types(df)
    dates = detect_date_issues(df)
    missing = detect_missingness(df)

    column_issues = {}

    for col in df.columns:
        col_flags = []

        if col in mixed:
            col_flags.append(mixed[col])

        if col in dates:
            col_flags.append(dates[col])

        if col in missing:
            col_flags.append(missing[col])

        if col_flags:
            column_issues[col] = col_flags

    total_cols = len(df.columns)
    problematic = len(column_issues)

    health_score = 1 - (problematic / total_cols if total_cols else 0)

    return {
        "column_issues": column_issues,
        "summary": {
            "total_columns": total_cols,
            "columns_with_issues": problematic,
            "health_score": round(health_score, 3)
        }
    }


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
def explain(df, profile, anomalies, correlations, quality):
    parts = []

    parts.append(f"Dataset has {len(df)} rows and {len(df.columns)} columns.")

    if anomalies:
        parts.append(f"Anomalies in: {list(anomalies.keys())}.")
    else:
        parts.append("No significant anomalies detected.")

    if quality["summary"]["columns_with_issues"] > 0:
        parts.append(
            f"{quality['summary']['columns_with_issues']} columns have data quality issues."
        )

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
    quality = data_quality_report(df)

    explanation = explain(df, profile, anomalies, correlations, quality)

    return {
        "request_id": request_id,
        "dataset_hash": h,
        "profile": profile,
        "data_quality": quality,
        "anomalies": anomalies,
        "correlations": correlations,
        "explanation": explanation,
    }