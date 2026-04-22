"""
Price Adjustment routes.

GET  /api/price-adjustment/batches   – one row per source_file with counts
GET  /api/price-adjustment/docs      – paginated list of adjustment documents
GET  /api/price-adjustment/docs/{id} – single document detail
POST /api/price-adjustment/import    – manual CSV upload trigger
"""
import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File
from sqlalchemy import select, func, case

from app.core.security import get_current_user
from app.db.postgres import get_session
from app.models.price_adjustment_doc import PriceAdjustmentDoc, price_adj_doc_to_response

router = APIRouter(prefix="/api/price-adjustment", tags=["price-adjustment"])
logger = logging.getLogger(__name__)


@router.get("/batches")
async def list_price_adj_batches(_: str = Depends(get_current_user)):
    """Return one row per source_file with aggregated counts."""
    async with get_session() as session:
        q = (
            select(
                PriceAdjustmentDoc.source_file,
                func.count(PriceAdjustmentDoc.id).label("doc_count"),
                func.sum(PriceAdjustmentDoc.item_count).label("total_items"),
                func.sum(PriceAdjustmentDoc.posted_count).label("posted_items"),
                func.sum(PriceAdjustmentDoc.error_count).label("error_items"),
                func.max(PriceAdjustmentDoc.created_at).label("latest"),
                func.sum(
                    case((PriceAdjustmentDoc.status == "posted", 1), else_=0)
                ).label("posted_docs"),
                func.sum(
                    case((PriceAdjustmentDoc.status == "error", 1), else_=0)
                ).label("error_docs"),
            )
            .group_by(PriceAdjustmentDoc.source_file)
            .order_by(func.max(PriceAdjustmentDoc.created_at).desc())
        )
        result = await session.execute(q)
        rows = result.all()

    return [
        {
            "source_file": r.source_file or "(unknown)",
            "doc_count": r.doc_count,
            "total_items": int(r.total_items or 0),
            "posted_items": int(r.posted_items or 0),
            "error_items": int(r.error_items or 0),
            "latest": r.latest.isoformat() if r.latest else None,
            "posted_docs": int(r.posted_docs or 0),
            "error_docs": int(r.error_docs or 0),
        }
        for r in rows
    ]


@router.get("/docs")
async def list_price_adj_docs(
    source_file: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    _: str = Depends(get_current_user),
):
    filters = []
    if source_file:
        filters.append(PriceAdjustmentDoc.source_file == source_file)
    if status:
        filters.append(PriceAdjustmentDoc.status == status)

    async with get_session() as session:
        total = await session.scalar(
            select(func.count()).select_from(PriceAdjustmentDoc).where(*filters)
        )
        result = await session.execute(
            select(PriceAdjustmentDoc).where(*filters)
            .order_by(PriceAdjustmentDoc.created_at.desc())
            .offset(offset).limit(limit)
        )
        docs = result.scalars().all()

    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "items": [price_adj_doc_to_response(d) for d in docs],
    }


@router.get("/docs/{doc_id}")
async def get_price_adj_doc(doc_id: str, _: str = Depends(get_current_user)):
    import uuid
    try:
        oid = uuid.UUID(doc_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid document ID.")

    async with get_session() as session:
        doc = await session.get(PriceAdjustmentDoc, oid)

    if not doc:
        raise HTTPException(status_code=404, detail="Document not found.")
    return price_adj_doc_to_response(doc)


@router.post("/import")
async def import_price_adjustment(
    file: UploadFile = File(...),
    _: str = Depends(get_current_user),
):
    """Manual CSV upload — runs the full pipeline immediately."""
    from app.services.price_adjustment_service import process_price_adjustment_csv
    from app.core.timezone import now_pkt

    raw = await file.read()
    base_name = file.filename or "price_adjustment.csv"
    batch_key = f"{base_name}::{now_pkt().strftime('%Y%m%d_%H%M%S')}"

    try:
        result = await process_price_adjustment_csv(raw, source_file=batch_key)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception("Price adjustment import failed")
        raise HTTPException(status_code=500, detail=str(exc))

    return result
