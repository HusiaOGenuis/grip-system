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
        profile[col] = {
            "dtype": str(s.dtype),
            "null_ratio": float(s.isna().mean()),
            "unique": int(s.nunique()),
            "type": "numeric" if pd.api.types.is_numeric_dtype(s) else "categorical"
        }
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
        elif "date" in name:
            roles[col] = "datetime"
        elif "amount" in name or "price" in name:
            roles[col] = "financial"
        elif "status" in name:
            roles[col] = "status"
        else:
            roles[col] = "category"
    return roles


# -------------------------
# SEMANTIC DIAGNOSIS
# -------------------------
def semantic_diagnosis(df, profile, quality, roles):
    findings = []

    for col in df.columns:
        role = roles[col]
        p = profile[col]

        if role == "financial" and p["type"] != "numeric":
            findings.append({
                "column": col,
                "issue": "Financial column is not numeric",
                "severity": "high",
                "suggestion": "Convert to numeric and clean values"
            })

        if role == "identifier" and p["unique"] < len(df):
            findings.append({
                "column": col,
                "issue": "Identifier contains duplicates",
                "severity": "high",
                "suggestion": "Ensure unique identifiers"
            })

        if role == "datetime":
            parsed = pd.to_datetime(df[col], errors="coerce")
            if parsed.notna().mean() < 0.8:
                findings.append({
                    "column": col,
                    "issue": "Date column not consistently parseable",
                    "severity": "medium",
                    "suggestion": "Standardize date format"
                })

        if col in quality["column_issues"]:
            findings.append({
                "column": col,
                "issue": "High missing values",
                "severity": "medium",
                "suggestion": "Investigate or impute missing data"
            })

    return findings


# -------------------------
# DECISION ENGINE
# -------------------------
def decision_engine(findings, quality):
    high = [f for f in findings if f["severity"] == "high"]
    medium = [f for f in findings if f["severity"] == "medium"]

    health = quality["summary"]["health_score"]

    # Verdict logic
    if high or health < 0.5:
        verdict = "UNRELIABLE"
        risk = "HIGH"
    elif medium:
        verdict = "WARNING"
        risk = "MEDIUM"
    else:
        verdict = "RELIABLE"
        risk = "LOW"

    # Priority actions (deduplicated)
    actions = []
    for f in high + medium:
        if f["suggestion"] not in actions:
            actions.append(f["suggestion"])

    return {
        "verdict": verdict,
        "risk_level": risk,
        "priority_actions": actions[:5]
    }


# -------------------------
# EXPLAIN
# -------------------------
def explain(df, decision):
    return (
        f"Dataset evaluated as {decision['verdict']} "
        f"with {decision['risk_level']} risk. "
        f"{len(decision['priority_actions'])} priority actions recommended."
    )


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
    findings = semantic_diagnosis(df, profile, quality, roles)
    decision = decision_engine(findings, quality)

    explanation = explain(df, decision)

    return {
        "request_id": request_id,
        "dataset_hash": h,
        "profile": profile,
        "data_quality": quality,
        "semantic_findings": findings,
        "decision": decision,
        "explanation": explanation,
    }