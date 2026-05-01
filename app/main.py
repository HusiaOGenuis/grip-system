from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from supabase import create_client, Client
import uuid
import os
import requests
import pandas as pd
import io

app = FastAPI()

# =========================
# ENV CONTRACT
# =========================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
PAYSTACK_SECRET = os.getenv("PAYSTACK_SECRET")
BUCKET = "reports"

if not SUPABASE_URL or not SUPABASE_KEY:
    raise Exception("Supabase credentials missing")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# =========================
# STATIC UI
# =========================
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/", response_class=HTMLResponse)
def root():
    with open("static/index.html") as f:
        return f.read()

# =========================
# SCORING ENGINE (INLINE)
# =========================
def grip_score(data):
    total = len(data)
    if total == 0:
        return {"score": 0, "risk": "INVALID"}

    missing = 0
    duplicates = 0
    seen = set()

    for row in data:
        for v in row.values():
            if v in (None, "", "null"):
                missing += 1

        key = tuple(row.items())
        if key in seen:
            duplicates += 1
        else:
            seen.add(key)

    completeness = 1 - (missing / (total * len(data[0])))
    uniqueness = 1 - (duplicates / total)

    score = (completeness * 0.6 + uniqueness * 0.4) * 100

    if score < 50:
        risk = "HIGH"
    elif score < 75:
        risk = "MEDIUM"
    else:
        risk = "LOW"

    return {
        "completeness": round(completeness * 100, 2),
        "uniqueness": round(uniqueness * 100, 2),
        "score": round(score, 2),
        "risk": risk
    }

# =========================
# PAYSTACK VERIFY
# =========================
def verify_payment(reference: str):
    url = f"https://api.paystack.co/transaction/verify/{reference}"

    headers = {
        "Authorization": f"Bearer {PAYSTACK_SECRET}"
    }

    response = requests.get(url, headers=headers).json()

    if response.get("data", {}).get("status") == "success":
        return True

    raise HTTPException(400, "Payment not verified")

# =========================
# UPLOAD
# =========================
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

# =========================
# LIST FILES
# =========================
@app.get("/files")
def list_files():
    result = supabase.storage.from_(BUCKET).list()
    files = [item["name"] for item in result if "name" in item]
    return {"files": files}

# =========================
# ANALYZE + STORE
# =========================
@app.post("/analyze")
def analyze(file_name: str):

    signed = supabase.storage.from_(BUCKET).create_signed_url(file_name, 60)
    response = requests.get(signed["signedURL"])

    df = pd.read_csv(io.StringIO(response.text))
    data = df.to_dict(orient="records")

    score_result = grip_score(data)

    result = {
        "file_name": file_name,
        "rows": len(df),
        "columns": list(df.columns),
        "score": score_result
    }

    # STORE RESULT
    supabase.table("reports_analysis").insert({
        "file_name": file_name,
        "rows": len(df),
        "columns": list(df.columns)
    }).execute()

    return result

# =========================
# DOWNLOAD (PAYMENT LOCK)
# =========================
@app.get("/download/{file_name}")
def download(file_name: str, reference: str):
    verify_payment(reference)

    signed = supabase.storage.from_(BUCKET).create_signed_url(file_name, 60)
    return {"url": signed["signedURL"]}