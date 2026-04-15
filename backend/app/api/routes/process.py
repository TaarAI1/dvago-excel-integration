import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy import update, and_

from app.core.security import get_current_user
from app.db.postgres import get_session
from app.models.document import Document
from app.models.activity_log import write_log
from app.jobs.ftp_job import poll_ftp_and_ingest
from app.jobs.api_job import process_pending_docs

router = APIRouter(prefix="/api/process", tags=["process"])


@router.post("/trigger")
async def manual_trigger(background_tasks: BackgroundTasks, _: str = Depends(get_current_user)):
    background_tasks.add_task(poll_ftp_and_ingest)
    return {"message": "FTP poll triggered. Check activity logs for progress."}


@router.post("/trigger-api")
async def manual_api_trigger(background_tasks: BackgroundTasks, _: str = Depends(get_current_user)):
    background_tasks.add_task(process_pending_docs)
    return {"message": "API processing triggered. Check activity logs for progress."}


@router.post("/retry/{document_id}")
async def retry_document(document_id: str, _: str = Depends(get_current_user)):
    try:
        oid = uuid.UUID(document_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid document ID.")

    async with get_session() as session:
        doc = await session.get(Document, oid)
        if not doc:
            raise HTTPException(status_code=404, detail="Document not found.")
        if doc.posted:
            raise HTTPException(status_code=400, detail="Document already posted successfully.")

    async with get_session() as session:
        async with session.begin():
            await session.execute(
                update(Document).where(Document.id == oid).values(
                    has_error=False, error_message=None, updated_at=datetime.utcnow()
                )
            )
            await write_log(session, activity_type="manual_trigger", document_id=document_id,
                            document_type=doc.document_type, status="pending",
                            details="Document manually queued for retry.")

    return {"message": "Document queued for retry.", "document_id": document_id}


@router.post("/batch-retry")
async def batch_retry(_: str = Depends(get_current_user)):
    async with get_session() as session:
        async with session.begin():
            result = await session.execute(
                update(Document)
                .where(and_(Document.has_error == True, Document.posted == False))
                .values(has_error=False, error_message=None, updated_at=datetime.utcnow())
                .returning(Document.id)
            )
            count = len(result.fetchall())
            await write_log(session, activity_type="manual_trigger", status="pending",
                            details=f"Batch retry queued {count} documents.")

    return {"message": f"Queued {count} documents for retry."}
