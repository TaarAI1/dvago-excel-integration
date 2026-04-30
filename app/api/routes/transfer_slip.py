"""
Transfer Slip routes.

GET  /api/transfer-slip/status    – whether an import is currently running
POST /api/transfer-slip/kill      – cancel the running import after current note-group
GET  /api/transfer-slip/batches   – one row per source_file with counts
GET  /api/transfer-slip/docs      – paginated list of transfer slip documents
GET  /api/transfer-slip/docs/{id} – single document detail (full API traces)
POST /api/transfer-slip/import    – manual CSV upload trigger
"""
import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File
from sqlalchemy import select, func, case

from app.core.security import get_current_user
from app.db.postgres import get_session
from app.models.transfer_slip_doc import TransferSlipDoc, transfer_slip_doc_to_response

router = APIRouter(prefix="/api/transfer-slip", tags=["transfer-slip"])
logger = logging.getLogger(__name__)


@router.get("/status")
async def transfer_slip_status(_: str = Depends(get_current_user)):
    """Return whether a Transfer Slip import is currently running."""
    from app.services.transfer_slip_service import get_active_import_id
    active = get_active_import_id()
    return {"running": active is not None, "import_id": active}


@router.post("/kill")
async def transfer_slip_kill(_: str = Depends(get_current_user)):
    """Cancel the running Transfer Slip import after the current note-group finishes."""
    from app.services.transfer_slip_service import request_cancel_import, get_active_import_id
    active = get_active_import_id()
    if not active:
        return {"cancelled": False, "message": "No Transfer Slip import is currently running."}
    request_cancel_import()
    return {
        "cancelled": True,
        "import_id": active,
        "message": "Stop signal sent — will halt after the current note-group completes.",
    }


@router.get("/batches")
async def list_transfer_slip_batches(_: str = Depends(get_current_user)):
    """Return one row per source_file with aggregated counts."""
    async with get_session() as session:
        q = (
            select(
                TransferSlipDoc.source_file,
                func.count(TransferSlipDoc.id).label("doc_count"),
                func.sum(TransferSlipDoc.item_count).label("total_items"),
                func.sum(TransferSlipDoc.posted_count).label("posted_items"),
                func.sum(TransferSlipDoc.error_count).label("error_items"),
                func.max(TransferSlipDoc.created_at).label("latest"),
                func.sum(
                    case((TransferSlipDoc.status == "posted", 1), else_=0)
                ).label("posted_docs"),
                func.sum(
                    case((TransferSlipDoc.status == "error", 1), else_=0)
                ).label("error_docs"),
            )
            .group_by(TransferSlipDoc.source_file)
            .order_by(func.max(TransferSlipDoc.created_at).desc())
        )
        result = await session.execute(q)
        rows = result.all()

    return [
        {
            "source_file":  r.source_file or "(unknown)",
            "doc_count":    r.doc_count,
            "total_items":  int(r.total_items  or 0),
            "posted_items": int(r.posted_items or 0),
            "error_items":  int(r.error_items  or 0),
            "latest":       r.latest.isoformat() if r.latest else None,
            "posted_docs":  int(r.posted_docs  or 0),
            "error_docs":   int(r.error_docs   or 0),
        }
        for r in rows
    ]


@router.get("/docs")
async def list_transfer_slip_docs(
    source_file: Optional[str] = Query(None),
    status:      Optional[str] = Query(None),
    limit:  int = Query(100, ge=1, le=500),
    offset: int = Query(0,   ge=0),
    _: str = Depends(get_current_user),
):
    filters = []
    if source_file:
        filters.append(TransferSlipDoc.source_file == source_file)
    if status:
        filters.append(TransferSlipDoc.status == status)

    async with get_session() as session:
        total = await session.scalar(
            select(func.count()).select_from(TransferSlipDoc).where(*filters)
        )
        result = await session.execute(
            select(TransferSlipDoc).where(*filters)
            .order_by(TransferSlipDoc.created_at.desc())
            .offset(offset).limit(limit)
        )
        docs = result.scalars().all()

    return {
        "total":  total,
        "offset": offset,
        "limit":  limit,
        "items":  [transfer_slip_doc_to_response(d) for d in docs],
    }


@router.get("/docs/{doc_id}")
async def get_transfer_slip_doc(doc_id: str, _: str = Depends(get_current_user)):
    import uuid
    try:
        oid = uuid.UUID(doc_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid document ID.")

    async with get_session() as session:
        doc = await session.get(TransferSlipDoc, oid)

    if not doc:
        raise HTTPException(status_code=404, detail="Document not found.")
    return transfer_slip_doc_to_response(doc)


@router.post("/import")
async def import_transfer_slip(
    file: UploadFile = File(...),
    _: str = Depends(get_current_user),
):
    """Manual CSV upload — runs the full pipeline immediately."""
    import asyncio
    from app.services.transfer_slip_service import process_transfer_slip_csv
    from app.services.email_service import send_batch_email
    from app.core.timezone import now_pkt

    raw       = await file.read()
    base_name = file.filename or "transfer_slip.csv"
    batch_key = f"{base_name}::{now_pkt().strftime('%Y%m%d_%H%M%S')}"

    try:
        result = await process_transfer_slip_csv(raw, source_file=batch_key)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception("Transfer slip import failed")
        raise HTTPException(status_code=500, detail=str(exc))

    asyncio.create_task(send_batch_email("transfer_slip", batch_key, result))
    return result
