"""
GRN (Goods Received Note) routes.

GET  /api/grn/status        – whether an import is currently running
POST /api/grn/kill          – cancel the running import after current note-group
POST /api/grn/import        – manual CSV upload trigger
GET  /api/grn/batches       – one row per source_file with aggregated counts
GET  /api/grn/docs          – paginated list of GRN documents
GET  /api/grn/docs/{id}     – single document detail (full API traces)
"""
import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File
from sqlalchemy import select, func, case

from app.core.security import get_current_user
from app.db.postgres import get_session
from app.models.grn_doc import GRNDoc, grn_doc_to_response

router = APIRouter(prefix="/api/grn", tags=["grn"])
logger = logging.getLogger(__name__)


@router.get("/status")
async def grn_status(_: str = Depends(get_current_user)):
    """Return whether a GRN import is currently running."""
    from app.services.grn_service import get_active_import_id
    active = get_active_import_id()
    return {"running": active is not None, "import_id": active}


@router.post("/kill")
async def grn_kill(_: str = Depends(get_current_user)):
    """Cancel the running GRN import after the current note-group finishes."""
    from app.services.grn_service import request_cancel_import, get_active_import_id
    active = get_active_import_id()
    if not active:
        return {"cancelled": False, "message": "No GRN import is currently running."}
    request_cancel_import()
    return {
        "cancelled": True,
        "import_id": active,
        "message": "Stop signal sent — will halt after the current note-group completes.",
    }


@router.post("/import")
async def import_grn(
    file: UploadFile = File(...),
    _: str = Depends(get_current_user),
):
    """Manual CSV upload — runs the full GRN pipeline immediately."""
    from app.services.grn_service import process_grn_csv
    from app.core.timezone import now_pkt

    raw       = await file.read()
    base_name = file.filename or "grn.csv"
    batch_key = f"{base_name}::{now_pkt().strftime('%Y%m%d_%H%M%S')}"

    try:
        result = await process_grn_csv(raw, source_file=batch_key)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception("GRN import failed")
        raise HTTPException(status_code=500, detail=str(exc))

    return result


@router.get("/batches")
async def list_grn_batches(_: str = Depends(get_current_user)):
    """Return one row per source_file with aggregated counts."""
    async with get_session() as session:
        q = (
            select(
                GRNDoc.source_file,
                func.count(GRNDoc.id).label("doc_count"),
                func.sum(GRNDoc.item_count).label("total_items"),
                func.sum(GRNDoc.posted_count).label("posted_items"),
                func.sum(GRNDoc.error_count).label("error_items"),
                func.max(GRNDoc.created_at).label("latest"),
                func.sum(
                    case((GRNDoc.status == "posted", 1), else_=0)
                ).label("posted_docs"),
                func.sum(
                    case((GRNDoc.status == "error", 1), else_=0)
                ).label("error_docs"),
            )
            .group_by(GRNDoc.source_file)
            .order_by(func.max(GRNDoc.created_at).desc())
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
async def list_grn_docs(
    source_file: Optional[str] = Query(None),
    status:      Optional[str] = Query(None),
    limit:  int = Query(100, ge=1, le=500),
    offset: int = Query(0,   ge=0),
    _: str = Depends(get_current_user),
):
    filters = []
    if source_file:
        filters.append(GRNDoc.source_file == source_file)
    if status:
        filters.append(GRNDoc.status == status)

    async with get_session() as session:
        total = await session.scalar(
            select(func.count()).select_from(GRNDoc).where(*filters)
        )
        result = await session.execute(
            select(GRNDoc).where(*filters)
            .order_by(GRNDoc.created_at.desc())
            .offset(offset).limit(limit)
        )
        docs = result.scalars().all()

    return {
        "total":  total,
        "offset": offset,
        "limit":  limit,
        "items":  [grn_doc_to_response(d) for d in docs],
    }


@router.get("/docs/{doc_id}")
async def get_grn_doc(doc_id: str, _: str = Depends(get_current_user)):
    import uuid
    try:
        oid = uuid.UUID(doc_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid document ID.")

    async with get_session() as session:
        doc = await session.get(GRNDoc, oid)

    if not doc:
        raise HTTPException(status_code=404, detail="Document not found.")
    return grn_doc_to_response(doc)
