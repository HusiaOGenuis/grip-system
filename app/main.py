from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from supabase import create_client, Client

import uuid
import os
import requests
import pandas as pd
import io

# =========================
# CONTRACT: PRECONDITIONS
# =========================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
BUCKET_NAME = "reports"

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Supabase credentials not set")

# =========================
# INITIALIZATION
# =========================
app = FastAPI()
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
# CONTRACT MODEL
# =========================
class FileRequest(BaseModel):
    file_name: str


# =========================
# UPLOAD → SUPABASE
# =========================
@app.post("/upload")
async def upload(file: UploadFile = File(...)):
    contents = await file.read()

    if not contents:
        raise HTTPException(status_code=400, detail="Empty file")

    filename = f"{uuid.uuid4()}.csv"

    res = supabase.storage.from_(BUCKET_NAME).upload(filename, contents)

    if hasattr(res, "error") and res.error:
        raise HTTPException(status_code=500, detail=str(res.error))

    return {"file": filename}


# =========================
# LIST FILES FROM SUPABASE
# =========================
@app.get("/files")
def list_files():
    res = supabase.storage.from_(BUCKET_NAME).list()

    files = []
    for f in res:
        if "name" in f:
            files.append(f["name"])

    return {"files": files}


# =========================
# ANALYZE EXISTING FILE
# =========================
@app.post("/analyze")
def analyze(req: FileRequest):
    file_name = req.file_name

    signed = supabase.storage.from_(BUCKET_NAME).create_signed_url(file_name, 60)

    if "signedURL" not in signed:
        raise HTTPException(status_code=500, detail="Failed to create signed URL")

    response = requests.get(signed["signedURL"])

    if response.status_code != 200:
        raise HTTPException(status_code=500, detail="Failed to fetch file")

    df = pd.read_csv(io.StringIO(response.text))

    return {
        "rows": len(df),
        "columns": list(df.columns)
    }


# =========================
# DOWNLOAD (SECURE CONTRACT)
# =========================
@app.get("/download/{file_name}")
def download(file_name: str):
    signed = supabase.storage.from_(BUCKET_NAME).create_signed_url(file_name, 60)

    if "signedURL" not in signed:
        raise HTTPException(status_code=500, detail="Download failed")

    return {"download_url": signed["signedURL"]}