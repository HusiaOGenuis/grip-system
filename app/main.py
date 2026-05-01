from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from supabase import create_client, Client

import uuid
import os
import io
import requests
import pandas as pd

# =========================
# APP INIT
# =========================
app = FastAPI()

# =========================
# CONTRACT: ENVIRONMENT PRECONDITIONS
# =========================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise Exception("❌ SUPABASE ENV VARIABLES NOT SET")

# MUST match Supabase EXACTLY (case-sensitive)
BUCKET_NAME = "reports"

# Single client instance (NO duplication)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# =========================
# STATIC FRONTEND
# =========================
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/", response_class=HTMLResponse)
def read_root():
    with open("static/index.html", "r") as f:
        return f.read()


# =========================
# CONTRACT 1: UPLOAD
# Precondition: file provided
# Postcondition: file stored in Supabase
# =========================
@app.post("/upload")
async def upload(file: UploadFile = File(...)):
    try:
        if not file:
            raise HTTPException(status_code=400, detail="No file provided")

        contents = await file.read()

        if not contents:
            raise HTTPException(status_code=400, detail="Empty file")

        file_id = str(uuid.uuid4())
        filename = f"{file_id}.csv"

        res = supabase.storage.from_(BUCKET_NAME).upload(filename, contents)

        print("UPLOAD RESULT:", res)

        return {
            "file_id": file_id,
            "filename": filename,
            "status": "uploaded"
        }

    except Exception as e:
        print("UPLOAD ERROR:", str(e))
        raise HTTPException(status_code=500, detail=str(e))


# =========================
# CONTRACT 2: LIST FILES
# Precondition: bucket exists
# Postcondition: returns stored files
# =========================
@app.get("/files")
def list_files():
    try:
        files = supabase.storage.from_(BUCKET_NAME).list()

        # Normalize output
        return [f["name"] for f in files]

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =========================
# CONTRACT 3: ANALYZE
# Precondition: file exists in Supabase
# Postcondition: returns dataset summary
# =========================
@app.post("/analyze")
def analyze(file_name: str):
    try:
        if not file_name:
            raise HTTPException(status_code=400, detail="file_name required")

        signed = supabase.storage.from_(BUCKET_NAME).create_signed_url(file_name, 60)

        if "signedURL" not in signed:
            raise HTTPException(status_code=500, detail="Failed to generate signed URL")

        response = requests.get(signed["signedURL"])

        if response.status_code != 200:
            raise HTTPException(status_code=500, detail="Failed to fetch file")

        df = pd.read_csv(io.StringIO(response.text))

        return {
            "rows": len(df),
            "columns": list(df.columns)
        }

    except Exception as e:
        print("ANALYZE ERROR:", str(e))
        raise HTTPException(status_code=500, detail=str(e))


# =========================
# CONTRACT 4: DOWNLOAD (SECURE)
# Precondition: payment verified
# Postcondition: signed URL returned
# =========================
@app.get("/download/{file_id}")
def download(file_id: str):
    try:
        if not file_id:
            raise HTTPException(status_code=400, detail="file_id required")

        res = supabase.table("reports").select("*").eq("file_id", file_id).execute()

        if not res.data:
            raise HTTPException(status_code=404, detail="Report not found")

        record = res.data[0]

        if not record.get("is_paid"):
            raise HTTPException(status_code=402, detail="Payment required")

        filename = f"{file_id}.csv"

        signed = supabase.storage.from_(BUCKET_NAME).create_signed_url(filename, 60)

        if "signedURL" not in signed:
            raise HTTPException(status_code=500, detail="Failed to generate download URL")

        return {"url": signed["signedURL"]}

    except Exception as e:
        print("DOWNLOAD ERROR:", str(e))
        raise HTTPException(status_code=500, detail=str(e))