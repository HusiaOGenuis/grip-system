print("🔥 NEW ANALYSIS MODULE LOADED")
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
# NORMALIZE
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
            info["type"] = "numeric"
        else:
            info["type"] = "categorical"

        profile[col] = info
    return profile


# -------------------------
# DATA QUALITY
# -------------------------
def data_quality_report(df: pd.DataFrame) -> Dict[str, Any]:
    issues = {}

    for col in df.columns:
        null_ratio = df[col].isna().mean()

        if null_ratio > 0.3:
            issues[col] = {
                "type": "missing_values",
                "null_ratio": float(null_ratio)
            }

    total = len(df.columns)
    bad = len(issues)

    return {
        "column_issues": issues,
        "summary": {
            "total_columns": total,
            "columns_with_issues": bad,
            "health_score": round(1 - (bad / total if total else 0), 3)
        }
    }


# -------------------------
# ROLE INFERENCE
# -------------------------
def infer_roles(df: pd.DataFrame) -> Dict[str, str]:
    roles = {}

    for col in df.columns:
        name = col.lower()

        if "id" in name:
            roles[col] = "identifier"
        elif "date" in name or "time" in name:
            roles[col] = "datetime"
        elif "amount" in name or "price" in name or "total" in name:
            roles[col] = "financial"
        elif "status" in name:
            roles[col] = "status"
        else:
            roles[col] = "category"

    return roles


# -------------------------
# SEMANTIC DIAGNOSIS
# -------------------------
def semantic_diagnosis(df: pd.DataFrame, profile, quality, roles) -> Dict[str, Any]:
    findings = []

    for col in df.columns:
        role = roles[col]
        p = profile[col]
        q = quality["column_issues"].get(col)

        # Financial column check
        if role == "financial" and p["type"] != "numeric":
            findings.append({
                "column": col,
                "issue": "Expected numeric financial values but found non-numeric data",
                "severity": "high",
                "suggestion": "Clean currency symbols and enforce numeric format"
            })

        # Date column check
        if role == "datetime":
            parsed = pd.to_datetime(df[col], errors="coerce")
            ratio = parsed.notna().mean()

            if ratio < 0.8:
                findings.append({
                    "column": col,
                    "issue": "Column appears to be date but is not consistently parseable",
                    "severity": "medium",
                    "suggestion": "Standardize date format (e.g. YYYY-MM-DD)"
                })

        # Identifier check
        if role == "identifier":
            if p["unique"] < len(df):
                findings.append({
                    "column": col,
                    "issue": "Identifier column contains duplicates",
                    "severity": "high",
                    "suggestion": "Ensure unique keys per record"
                })

        # Missing values
        if q:
            findings.append({
                "column": col,
                "issue": "High missing values",
                "severity": "medium",
                "suggestion": "Investigate data collection or fill strategy"
            })

    return {
        "roles": roles,
        "findings": findings
    }


# -------------------------
# EXPLANATION
# -------------------------
def explain(df, quality, diagnosis):
    parts = []

    parts.append(f"Dataset has {len(df)} rows and {len(df.columns)} columns.")

    if quality["summary"]["columns_with_issues"] > 0:
        parts.append(
            f"{quality['summary']['columns_with_issues']} columns show data quality concerns."
        )

    if diagnosis["findings"]:
        parts.append(f"{len(diagnosis['findings'])} semantic issues detected.")

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
    quality = data_quality_report(df)
    roles = infer_roles(df)
    diagnosis = semantic_diagnosis(df, profile, quality, roles)

    explanation = explain(df, quality, diagnosis)

    return {
        "request_id": request_id,
        "dataset_hash": h,
        "profile": profile,
        "data_quality": quality,
        "semantic": diagnosis,
        "explanation": explanation,
    }