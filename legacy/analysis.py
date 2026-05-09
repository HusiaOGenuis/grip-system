# analysis.py
import hashlib
import pandas as pd
import numpy as np
from typing import Dict, Any


def dataset_hash(df: pd.DataFrame) -> str:
    return hashlib.sha256(
        pd.util.hash_pandas_object(df, index=True).values
    ).hexdigest()


def profile_dataframe(df: pd.DataFrame) -> Dict[str, Any]:
    profile = {}

    for col in df.columns:
        series = df[col]
        null_ratio = float(series.isna().mean())
        unique = int(series.nunique(dropna=True))
        dtype = str(series.dtype)

        inferred_type = "categorical"
        if pd.api.types.is_numeric_dtype(series):
            inferred_type = "numeric"
        elif pd.api.types.is_datetime64_any_dtype(series):
            inferred_type = "datetime"

        profile[col] = {
            "dtype": dtype,
            "null_ratio": null_ratio,
            "unique": unique,
            "type": inferred_type,
        }

    return profile


def detect_anomalies(df: pd.DataFrame) -> Dict[str, Any]:
    anomalies = {}
    numeric = df.select_dtypes(include=[np.number])

    for col in numeric.columns:
        s = numeric[col].dropna()
        if s.empty:
            continue

        z = (s - s.mean()) / (s.std() or 1)
        outliers = s[abs(z) > 3]

        if not outliers.empty:
            anomalies[col] = {
                "count": int(len(outliers)),
                "rows": outliers.index.tolist(),
            }

    return anomalies


def analyze_dataframe(df: pd.DataFrame) -> Dict[str, Any]:
    return {
        "dataset_hash": dataset_hash(df),
        "profile": profile_dataframe(df),
        "anomalies": detect_anomalies(df),
    }
