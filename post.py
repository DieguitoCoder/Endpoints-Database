import os
import hashlib
from fastapi import FastAPI, UploadFile, File, HTTPException
from supabase import create_client, Client

# --------------------------------------------------
# ENV (Render / Prod compatible)
# --------------------------------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    raise RuntimeError("Supabase credentials not set")

supabase: Client = create_client(
    SUPABASE_URL,
    SUPABASE_SERVICE_KEY
)

# --------------------------------------------------
# APP
# --------------------------------------------------
app = FastAPI(title="PDF Storage Service")

# --------------------------------------------------
# CONSTANTS
# --------------------------------------------------
MAX_PDF_SIZE = 10 * 1024 * 1024  # 10 MB

# --------------------------------------------------
# HELPERS
# --------------------------------------------------
def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

# --------------------------------------------------
# POST: Upload PDF (inmutable)
# --------------------------------------------------
@app.post("/pdf")
async def upload_pdf(
    interview_id: str,
    coder_id: str,
    file: UploadFile = File(...)
):
    buffer = await file.read()

    if file.content_type != "application/pdf":
        raise HTTPException(status_code=400, detail="Only PDF files are allowed")

    if len(buffer) > MAX_PDF_SIZE:
        raise HTTPException(status_code=400, detail="PDF exceeds max size (10MB)")

    file_hash = sha256(buffer)

    storage_path = f"{coder_id}/{interview_id}.pdf"

    # Upload to Supabase Storage (NO upsert → immutable PDFs)
    try:
        supabase.storage.from_("pdfs").upload(
            storage_path,
            buffer,
            file_options={
                "content-type": "application/pdf",
                "upsert": False
            }
        )
    except Exception as e:
        # 409 = already exists → business rule
        if "409" in str(e):
            raise HTTPException(
                status_code=409,
                detail="PDF already exists for this interview"
            )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to upload PDF: {str(e)}"
        )

    # Save metadata
    supabase.table("pdf_documents").insert({
        "interview_id": interview_id,
        "coder_id": coder_id,
        "storage_path": storage_path,
        "sha256": file_hash,
        "size_bytes": len(buffer),
        "content_type": "application/pdf"
    }).execute()

    return {
        "status": "stored",
        "interview_id": interview_id,
        "coder_id": coder_id
    }

# --------------------------------------------------
# GET: Signed URL for frontend
# --------------------------------------------------
@app.get("/pdf/{interview_id}/signed")
async def get_signed_pdf(interview_id: str):
    result = (
        supabase
        .table("pdf_documents")
        .select("storage_path")
        .eq("interview_id", interview_id)
        .limit(1)
        .execute()
    )

    if not result.data:
        raise HTTPException(status_code=404, detail="PDF not found")

    storage_path = result.data[0]["storage_path"]

    try:
        signed = supabase.storage.from_("pdfs").create_signed_url(
            storage_path,
            expires_in=300  # 5 minutes
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate signed URL: {str(e)}"
        )

    return {
        "url": signed["signedURL"],
        "expires_in": 300
    }
