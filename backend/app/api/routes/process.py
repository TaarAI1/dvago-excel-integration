import asyncio
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from bson import ObjectId

from app.core.security import get_current_user
from app.db.mongodb import get_db
from app.jobs.ftp_job import poll_ftp_and_ingest
from app.jobs.api_job import process_pending_docs
from app.models.activity_log import write_log

router = APIRouter(prefix="/api/process", tags=["process"])


@router.post("/trigger")
async def manual_trigger(
    background_tasks: BackgroundTasks,
    _: str = Depends(get_current_user),
):
    """Manually trigger an immediate FTP poll and ingest cycle."""
    background_tasks.add_task(poll_ftp_and_ingest)
    return {"message": "FTP poll triggered. Check activity logs for progress."}


@router.post("/trigger-api")
async def manual_api_trigger(
    background_tasks: BackgroundTasks,
    _: str = Depends(get_current_user),
):
    """Manually trigger an immediate RetailPro API processing cycle."""
    background_tasks.add_task(process_pending_docs)
    return {"message": "API processing triggered. Check activity logs for progress."}


@router.post("/retry/{document_id}")
async def retry_document(document_id: str, _: str = Depends(get_current_user)):
    """
    Reset a failed document so it will be picked up by the next api_job run.
    Clears has_error and error_message.
    """
    db = get_db()
    try:
        oid = ObjectId(document_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid document ID.")

    doc = await db.documents.find_one({"_id": oid})
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found.")

    if doc.get("posted"):
        raise HTTPException(status_code=400, detail="Document already posted successfully.")

    from datetime import datetime
    await db.documents.update_one(
        {"_id": oid},
        {"$set": {"has_error": False, "error_message": None, "updated_at": datetime.utcnow()}},
    )
    await write_log(
        db,
        activity_type="manual_trigger",
        document_id=document_id,
        document_type=doc.get("document_type"),
        status="pending",
        details="Document manually queued for retry.",
    )
    return {"message": "Document queued for retry.", "document_id": document_id}


@router.post("/batch-retry")
async def batch_retry(_: str = Depends(get_current_user)):
    """Reset all failed (has_error=True, posted=False) documents for retry."""
    db = get_db()
    from datetime import datetime
    result = await db.documents.update_many(
        {"has_error": True, "posted": False},
        {"$set": {"has_error": False, "error_message": None, "updated_at": datetime.utcnow()}},
    )
    await write_log(
        db,
        activity_type="manual_trigger",
        status="pending",
        details=f"Batch retry queued {result.modified_count} documents.",
    )
    return {"message": f"Queued {result.modified_count} documents for retry."}
