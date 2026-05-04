from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from supabase import create_client, Client

import pandas as pd
import uuid
import os
import io
from datetime import datetime

# =========================
# ENV VALIDATION (HARD FAIL)
# =========================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("FATAL: Missing Supabase credentials")

# =========================
# CLIENT
# =========================
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
BUCKET = "reports"

# =========================
# APP
# =========================
app = FastAPI(title="GRIP CORE API", version="1.0.0")
app.mount("/static", StaticFiles(directory="static"), name="static")

# =========================
# CONTRACT
# =========================
class AnalysisContract(BaseModel):
    file_name: str
    timestamp: str
    summary: dict
    quality: dict
    fraud: dict
    signals: list
    signal_counts: dict

# =========================
# VALIDATION
# =========================
def validate_input(df: pd.DataFrame):
    required = ["transaction_id", "amount"]
    for col in required:
        if col not in df.columns:
            raise HTTPException(400, f"Missing column: {col}")

# =========================
# ANALYZER
# =========================
class Analyzer:

    def __init__(self, df):
        self.df = df

    def run(self):
        rows = len(self.df)

        completeness = 100
        uniqueness = round((1 - self.df.duplicated().sum() / rows) * 100, 2) if rows else 0
        quality_score = round((completeness * 0.6 + uniqueness * 0.4), 2)

        signals = []
        signal_counts = {}

        if "date" in self.df.columns:
            invalid_dates = self.df["date"].astype(str).str.contains("[a-zA-Z]")
            count = int(invalid_dates.sum())
            if count > 0:
                signals.append("INVALID_DATE")
                signal_counts["INVALID_DATE"] = count

        if "amount" in self.df.columns:
            non_numeric = pd.to_numeric(self.df["amount"], errors="coerce").isna()
            count = int(non_numeric.sum())
            if count > 0:
                signals.append("NON_NUMERIC_AMOUNT")
                signal_counts["NON_NUMERIC_AMOUNT"] = count

        duplicates = int(self.df.duplicated().sum())
        if duplicates > 0:
            signals.append("DUPLICATE_TRANSACTION")
            signal_counts["DUPLICATE_TRANSACTION"] = duplicates

        fraud_score = len(signals) * 10
        risk = "HIGH" if fraud_score > 20 else "LOW"

        return {
            "summary": {"rows": rows, "columns": list(self.df.columns)},
            "quality": {
                "completeness": completeness,
                "uniqueness": uniqueness,
                "score": quality_score
            },
            "fraud": {"score": fraud_score, "penalty": 100, "risk": risk},
            "signals": signals,
            "signal_counts": signal_counts
        }

# =========================
# ROUTES
# =========================
@app.get("/")
def root():
    return {"status": "GRIP LIVE"}

@app.get("/health")
def health():
    return {"status": "OK"}

@app.post("/upload")
async def upload(file: UploadFile = File(...)):
    contents = await file.read()
    filename = f"{uuid.uuid4()}.csv"

    supabase.storage.from_(BUCKET).upload(
        path=filename,
        file=contents,
        file_options={"content-type": "text/csv"}
    )

    return {"file": filename}

@app.get("/files")
def list_files():
    result = supabase.storage.from_(BUCKET).list()
    return JSONResponse(content={"files": [f["name"] for f in result]})

@app.post("/analyze", response_model=AnalysisContract)
def analyze(filename: str):

    file_data = supabase.storage.from_(BUCKET).download(filename)
    df = pd.read_csv(io.BytesIO(file_data))

    validate_input(df)

    result = Analyzer(df).run()

    payload = {
        "file_name": filename,
        "timestamp": datetime.utcnow().isoformat(),
        **result
    }

    if "vendor_ref" in df.columns:
        for vendor in df["vendor_ref"].dropna():

            existing = supabase.table("vendor_risk") \
                .select("*") \
                .eq("vendor", vendor) \
                .execute()

            if existing.data:
                record = existing.data[0]

                total = record["total_occurrences"] + 1
                hits = record["high_risk_hits"]

                if payload["fraud"]["risk"] == "HIGH":
                    hits += 1

                score = hits / total

                supabase.table("vendor_risk").update({
                    "total_occurrences": total,
                    "high_risk_hits": hits,
                    "risk_score": score
                }).eq("vendor", vendor).execute()

            else:
                supabase.table("vendor_risk").insert({
                    "vendor": vendor,
                    "total_occurrences": 1,
                    "high_risk_hits": 1 if payload["fraud"]["risk"] == "HIGH" else 0,
                    "risk_score": 1.0 if payload["fraud"]["risk"] == "HIGH" else 0.0
                }).execute()

    supabase.table("reports_analysis").insert({
        "file_name": filename,
        "analysis": payload
    }).execute()

    return payload

@app.get("/reports")
def reports():
    data = supabase.table("reports_analysis").select("*").execute().data
    return JSONResponse(content=data)

@app.get("/vendor-risk")
def vendor_risk():
    data = supabase.table("vendor_risk").select("*").execute().data
    return JSONResponse(content=data)