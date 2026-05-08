import os
import io
import json
import hashlib
import requests
import pandas as pd

from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET")


def fetch_csv(path: str) -> pd.DataFrame:

    url = (
        f"{SUPABASE_URL}/storage/v1/object/public/"
        f"{SUPABASE_BUCKET}/{path}"
    )

    response = requests.get(url)

    response.raise_for_status()

    return pd.read_csv(
        io.StringIO(response.text)
    )


def dataset_hash(df: pd.DataFrame) -> str:

    content = df.to_csv(index=False)

    return hashlib.sha256(
        content.encode()
    ).hexdigest()


def profile_dataframe(df: pd.DataFrame) -> dict:

    profile = {}

    for column in df.columns:

        series = df[column]

        profile[column] = {
            "dtype": str(series.dtype),
            "null_ratio": float(series.isnull().mean()),
            "unique": int(series.nunique()),
            "type": "categorical"
        }

    return profile


def quality_analysis(df: pd.DataFrame) -> dict:

    issues = {}

    for column in df.columns:

        ratio = float(df[column].isnull().mean())

        if ratio > 0.2:

            issues[column] = {
                "type": "missing_values",
                "null_ratio": ratio
            }

    score = 1 - (
        len(issues) / max(len(df.columns), 1)
    )

    return {
        "column_issues": issues,
        "summary": {
            "total_columns": len(df.columns),
            "columns_with_issues": len(issues),
            "health_score": round(score, 3)
        }
    }


def semantic_analysis(df: pd.DataFrame) -> list:

    findings = []

    for column in df.columns:

        lower = column.lower()

        if "id" in lower:

            duplicates = df[column].duplicated().sum()

            if duplicates > 0:

                findings.append({
                    "column": column,
                    "issue": "Identifier contains duplicates",
                    "severity": "high",
                    "suggestion": "Ensure unique identifiers"
                })

        if "date" in lower:

            findings.append({
                "column": column,
                "issue": "Date column not consistently parseable",
                "severity": "medium",
                "suggestion": "Standardize date format"
            })

        if "amount" in lower:

            findings.append({
                "column": column,
                "issue": "Financial column is not numeric",
                "severity": "high",
                "suggestion": "Convert to numeric and clean values"
            })

        ratio = float(df[column].isnull().mean())

        if ratio > 0.2:

            findings.append({
                "column": column,
                "issue": "High missing values",
                "severity": "medium",
                "suggestion": "Investigate or impute missing data"
            })

    return findings


def decision_engine(findings: list) -> dict:

    verdict = "RELIABLE"
    risk = "LOW"

    actions = []

    for item in findings:

        severity = item.get("severity")
        suggestion = item.get("suggestion")

        if suggestion not in actions:
            actions.append(suggestion)

        if severity == "high":
            verdict = "UNRELIABLE"
            risk = "HIGH"

    if verdict != "UNRELIABLE":

        medium_count = len([
            x for x in findings
            if x.get("severity") == "medium"
        ])

        if medium_count > 0:
            verdict = "WARNING"
            risk = "MEDIUM"

    return {
        "verdict": verdict,
        "risk_level": risk,
        "priority_actions": actions
    }


def analyze_dataframe(
    df: pd.DataFrame,
    user_id: str,
    object_path: str
) -> dict:

    profile = profile_dataframe(df)

    quality = quality_analysis(df)

    findings = semantic_analysis(df)

    decision = decision_engine(findings)

    return {
        "request_id": hashlib.md5(
            os.urandom(32)
        ).hexdigest(),

        "dataset_hash": dataset_hash(df),

        "profile": profile,

        "data_quality": quality,

        "semantic_findings": findings,

        "decision": decision
    }
