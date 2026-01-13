import os
import hashlib
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Supabase credentials not set")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --------------------------------------------------
# APP
# --------------------------------------------------
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
# POST: upload PDF
# --------------------------------------------------
@app.post("/pdf")
async def upload_pdf(
    interview_id: str,
    coder_id: str,
    file: UploadFile = File(...)
):
    buffer = await file.read()

    if file.content_type != "application/pdf":
        raise HTTPException(400, "Only PDF allowed")

    if len(buffer) > MAX_PDF_SIZE:
        raise HTTPException(400, "PDF exceeds max size (10MB)")

    file_hash = sha256(buffer)
    storage_path = f"{coder_id}/{interview_id}.pdf"

    try:
        supabase.storage.from_("pdfs").upload(
            storage_path,
            buffer,
            file_options={
                "content-type": "application/pdf",
                "upsert": False
            }
        )
    except Exception:
        raise HTTPException(409, "PDF already exists")

    supabase.table("pdf_documents").insert({
        "interview_id": interview_id,
        "coder_id": coder_id,
        "storage_path": storage_path,
        "sha256": file_hash,
        "size_bytes": len(buffer),
        "content_type": "application/pdf"
    }).execute()

    return {"status": "stored", "interview_id": interview_id}

# --------------------------------------------------
# GET: signed URL for frontend
# --------------------------------------------------
@app.get("/pdf/{interview_id}/signed")
async def get_pdf_for_front(interview_id: str):
    result = (
        supabase
        .table("pdf_documents")
        .select("storage_path")
        .eq("interview_id", interview_id)
        .limit(1)
        .execute()
    )

    if not result.data:
        raise HTTPException(404, "PDF not found")

    signed = supabase.storage.from_("pdfs").create_signed_url(
        result.data[0]["storage_path"],
        expires_in=120
    )

    return {
        "url": signed["signedURL"],
        "expires_in": 120
    }
