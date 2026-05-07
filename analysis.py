import os
import uuid
import hashlib
import requests
import pandas as pd
import numpy as np

from io import StringIO
from typing import Dict, Any
from dotenv import load_dotenv

# --------------------------------------------------
# ENV
# --------------------------------------------------

load_dotenv(".env")

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

HEADERS = {
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
}

# --------------------------------------------------
# FETCH CSV
# --------------------------------------------------

def fetch_csv(object_path: str) -> pd.DataFrame:

    url = f"{SUPABASE_URL}/storage/v1/object/{object_path}"

    response = requests.get(
        url,
        headers=HEADERS,
        timeout=10
    )

    if response.status_code != 200:
        raise RuntimeError(
            f"Failed to fetch CSV: {response.text}"
        )

    return pd.read_csv(StringIO(response.text))


# --------------------------------------------------
# NORMALIZE TYPES
# --------------------------------------------------

def normalize_types(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()

    for col in df.columns:

        series = df[col]

        if series.dtype == object:

            numeric = pd.to_numeric(
                series,
                errors="coerce"
            )

            if numeric.notna().mean() > 0.5:
                df[col] = numeric

    return df


# --------------------------------------------------
# HASH
# --------------------------------------------------

def dataset_hash(df: pd.DataFrame) -> str:

    return hashlib.sha256(
        pd.util.hash_pandas_object(
            df,
            index=True
        ).values
    ).hexdigest()


# --------------------------------------------------
# PROFILE
# --------------------------------------------------

def profile_columns(df: pd.DataFrame) -> Dict[str, Any]:

    profile = {}

    for col in df.columns:

        series = df[col]

        profile[col] = {
            "dtype": str(series.dtype),
            "null_ratio": float(series.isna().mean()),
            "unique": int(series.nunique()),
            "type": (
                "numeric"
                if pd.api.types.is_numeric_dtype(series)
                else "categorical"
            )
        }

    return profile


# --------------------------------------------------
# DATA QUALITY
# --------------------------------------------------

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

    health_score = round(
        1 - (bad / total if total else 0),
        3
    )

    return {
        "column_issues": issues,
        "summary": {
            "total_columns": total,
            "columns_with_issues": bad,
            "health_score": health_score
        }
    }


# --------------------------------------------------
# ROLE INFERENCE
# --------------------------------------------------

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


# --------------------------------------------------
# SEMANTIC DIAGNOSIS
# --------------------------------------------------

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

        if role == "identifier":

            if p["unique"] < len(df):

                findings.append({
                    "column": col,
                    "issue": "Identifier contains duplicates",
                    "severity": "high",
                    "suggestion": "Ensure unique identifiers"
                })

        if role == "datetime":

            parsed = pd.to_datetime(
                df[col],
                errors="coerce"
            )

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


# --------------------------------------------------
# DECISION ENGINE
# --------------------------------------------------

def decision_engine(findings, quality):

    high = [
        f for f in findings
        if f["severity"] == "high"
    ]

    medium = [
        f for f in findings
        if f["severity"] == "medium"
    ]

    health = quality["summary"]["health_score"]

    if high or health < 0.5:

        verdict = "UNRELIABLE"
        risk = "HIGH"

    elif medium:

        verdict = "WARNING"
        risk = "MEDIUM"

    else:

        verdict = "RELIABLE"
        risk = "LOW"

    actions = []

    for finding in high + medium:

        suggestion = finding["suggestion"]

        if suggestion not in actions:
            actions.append(suggestion)

    return {
        "verdict": verdict,
        "risk_level": risk,
        "priority_actions": actions[:5]
    }


# --------------------------------------------------
# IMPACT ANALYSIS
# --------------------------------------------------

def impact_analysis(old_analysis, new_analysis):

    old_score = (
        old_analysis
        .get("data_quality", {})
        .get("summary", {})
        .get("health_score", 0)
    )

    new_score = (
        new_analysis
        .get("data_quality", {})
        .get("summary", {})
        .get("health_score", 0)
    )

    delta = round(new_score - old_score, 3)

    if delta > 0:
        direction = "improved"

    elif delta < 0:
        direction = "degraded"

    else:
        direction = "unchanged"

    return {
        "previous_health": old_score,
        "current_health": new_score,
        "change": delta,
        "direction": direction
    }


# --------------------------------------------------
# AUTO FIXES
# --------------------------------------------------

def apply_auto_fixes(df):

    df = df.copy()

    for col in df.columns:

        name = col.lower()

        if "amount" in name or "price" in name:

            df[col] = (
                df[col]
                .astype(str)
                .str.replace(
                    r"[^\d\.\-]",
                    "",
                    regex=True
                )
            )

            df[col] = pd.to_numeric(
                df[col],
                errors="coerce"
            )

        if "date" in name:

            df[col] = pd.to_datetime(
                df[col],
                errors="coerce"
            )

        if df[col].isna().mean() > 0:

            if pd.api.types.is_numeric_dtype(df[col]):

                df[col] = df[col].fillna(
                    df[col].median()
                )

            else:

                df[col] = df[col].fillna(
                    "UNKNOWN"
                )

    return df


# --------------------------------------------------
# MAIN ANALYSIS
# --------------------------------------------------

def analyze_dataframe(
    df,
    *,
    user_id,
    object_path
):

    df = normalize_types(df)

    request_id = str(uuid.uuid4())

    h = dataset_hash(df)

    profile = profile_columns(df)

    quality = data_quality_report(df)

    roles = infer_roles(df)

    findings = semantic_diagnosis(
        df,
        profile,
        quality,
        roles
    )

    decision = decision_engine(
        findings,
        quality
    )

    result = {
        "request_id": request_id,
        "dataset_hash": h,
        "profile": profile,
        "data_quality": quality,
        "semantic_findings": findings,
        "decision": decision
    }

    # ----------------------------------------------
    # SAVE TO SUPABASE REST API
    # ----------------------------------------------

    try:

        payload = {
            "user_id": user_id,
            "object_path": object_path,
            "dataset_hash": h,
            "verdict": decision["verdict"],
            "risk_level": decision["risk_level"],
            "analysis": result
        }

        insert_url = f"{SUPABASE_URL}/rest/v1/datasets"

        response = requests.post(
            insert_url,
            headers={
                "apikey": SUPABASE_SERVICE_ROLE_KEY,
                "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
                "Content-Type": "application/json",
                "Prefer": "return=representation"
            },
            json=payload,
            timeout=10
        )

        print("\n========== DATASET SAVED ==========")
        print(response.status_code)
        print(response.text)
        print("===================================\n")

    except Exception as e:

        print("\n========== INSERT FAILED ==========")
        print(str(e))
        print("===================================\n")

    return result