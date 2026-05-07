# main.py

from fastapi import FastAPI, HTTPException, Header, Depends, Request
from dotenv import load_dotenv
import os
import time
import numpy as np
import pandas as pd

# -------------------------
# ENV
# -------------------------
load_dotenv(".env")

API_KEY = "your-secret-key"

# -------------------------
# SECURITY
# -------------------------
def verify_key(x_api_key: str = Header(...)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")

# -------------------------
# RATE LIMIT
# -------------------------
REQUEST_LOG = {}

def rate_limit(user_id):
    now = time.time()
    REQUEST_LOG.setdefault(user_id, [])
    REQUEST_LOG[user_id] = [t for t in REQUEST_LOG[user_id] if now - t < 60]

    if len(REQUEST_LOG[user_id]) > 10:
        raise HTTPException(429, "Too many requests")

    REQUEST_LOG[user_id].append(now)

# -------------------------
# IMPORTS (CLEAN)
# -------------------------
from analysis import (
    fetch_csv,
    analyze_dataframe,
    dataset_hash,
    impact_analysis,
    apply_auto_fixes
)

# -------------------------
# APP
# -------------------------
app = FastAPI()

# -------------------------
# HELPERS
# -------------------------
def validate_path(path: str, user_id: str):
    if not path.startswith(f"reports/{user_id}/"):
        raise HTTPException(400, "Invalid path access")

# -------------------------
# ROOT
# -------------------------
@app.get("/")
def home():
    return {
        "service": "GripSystem",
        "status": "live",
        "capabilities": ["analyze", "compare", "fix"]
    }

# -------------------------
# ANALYZE
# -------------------------
@app.get("/analyze", dependencies=[Depends(verify_key)])
def analyze(path: str, user_id: str):
    try:
        validate_path(path, user_id)
        rate_limit(user_id)

        df = fetch_csv(path)

        result = analyze_dataframe(
            df,
            user_id=user_id,
            object_path=path
        )

        return {
            "status": "success",
            "analysis": result
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }

# -------------------------
# COMPARE
# -------------------------
@app.get("/compare", dependencies=[Depends(verify_key)])
def compare(path1: str, path2: str, user_id: str):
    try:
        validate_path(path1, user_id)
        validate_path(path2, user_id)
        rate_limit(user_id)

        df1 = fetch_csv(path1)
        df2 = fetch_csv(path2)

        cols = sorted(list(set(df1.columns) & set(df2.columns)))

        df1c = df1[cols].fillna("").astype(str).reset_index(drop=True)
        df2c = df2[cols].fillna("").astype(str).reset_index(drop=True)

        h1 = dataset_hash(df1c)
        h2 = dataset_hash(df2c)

        identical = h1 == h2

        rows_added = len(set(df2c.values.flatten()) - set(df1c.values.flatten()))
        rows_removed = len(set(df1c.values.flatten()) - set(df2c.values.flatten()))

        # impact
        a1 = analyze_dataframe(df1, user_id=user_id, object_path=path1)
        a2 = analyze_dataframe(df2, user_id=user_id, object_path=path2)
        impact = impact_analysis(a1, a2)

        return {
            "status": "success",
            "comparison": {
                "identical": identical,
                "rows_1": len(df1),
                "rows_2": len(df2),
                "rows_added": rows_added,
                "rows_removed": rows_removed,
                "hash_1": h1,
                "hash_2": h2,
                "impact": impact
            }
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }

# -------------------------
# FIX
# -------------------------
@app.get("/fix", dependencies=[Depends(verify_key)])
def fix(path: str, user_id: str):
    try:
        validate_path(path, user_id)
        rate_limit(user_id)

        df = fetch_csv(path)

        before = analyze_dataframe(df, user_id=user_id, object_path=path)

        fixed_df = apply_auto_fixes(df)

        after = analyze_dataframe(fixed_df, user_id=user_id, object_path=path)

        impact = impact_analysis(before, after)

        return {
            "status": "success",
            "before": before["decision"],
            "after": after["decision"],
            "impact": impact
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }