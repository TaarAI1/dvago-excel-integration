import uuid
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.security import get_current_user
from app.db.postgres import get_session
from app.models.document import Document, document_to_response
from app.models.system_config import SystemConfig

router = APIRouter(prefix="/api/documents", tags=["documents"])


def _build_filters(document_type, status, date_from, date_to):
    filters = []
    if document_type:
        filters.append(Document.document_type == document_type)
    if status == "posted":
        filters.append(Document.posted == True)
    elif status == "error":
        filters.append(and_(Document.has_error == True, Document.posted == False))
    elif status == "pending":
        filters.append(and_(Document.posted == False, Document.has_error == False))
    if date_from:
        try:
            filters.append(Document.created_at >= datetime.fromisoformat(date_from))
        except ValueError:
            pass
    if date_to:
        try:
            filters.append(Document.created_at <= datetime.fromisoformat(date_to))
        except ValueError:
            pass
    return filters


@router.get("")
async def list_documents(
    document_type: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    _: str = Depends(get_current_user),
):
    filters = _build_filters(document_type, status, date_from, date_to)

    async with get_session() as session:
        total = await session.scalar(
            select(func.count()).select_from(Document).where(*filters)
        )
        result = await session.execute(
            select(Document).where(*filters)
            .order_by(Document.created_at.desc())
            .offset(offset).limit(limit)
        )
        docs = result.scalars().all()

    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "items": [document_to_response(d) for d in docs],
    }


@router.get("/stats")
async def get_stats(_: str = Depends(get_current_user)):
    from datetime import date
    today_start = datetime.combine(date.today(), datetime.min.time())

    async with get_session() as session:
        total = await session.scalar(select(func.count()).select_from(Document))
        posted = await session.scalar(select(func.count()).select_from(Document).where(Document.posted == True))
        errors = await session.scalar(select(func.count()).select_from(Document).where(
            and_(Document.has_error == True, Document.posted == False)))
        pending = await session.scalar(select(func.count()).select_from(Document).where(
            and_(Document.posted == False, Document.has_error == False)))
        total_today = await session.scalar(select(func.count()).select_from(Document).where(
            Document.created_at >= today_start))
        posted_today = await session.scalar(select(func.count()).select_from(Document).where(
            and_(Document.posted == True, Document.posted_at >= today_start)))

        last_poll = await session.get(SystemConfig, "last_ftp_poll_success")

        from app.models.activity_log import ActivityLog
        avg_result = await session.scalar(
            select(func.avg(ActivityLog.duration_ms)).where(
                and_(ActivityLog.activity_type == "api_call", ActivityLog.status == "success",
                     ActivityLog.duration_ms.isnot(None))
            )
        )

    return {
        "total": total or 0,
        "posted": posted or 0,
        "errors": errors or 0,
        "pending": pending or 0,
        "total_today": total_today or 0,
        "posted_today": posted_today or 0,
        "post_rate_pct": round((posted or 0) / total * 100, 1) if total else 0,
        "error_rate_pct": round((errors or 0) / total * 100, 1) if total else 0,
        "avg_api_response_ms": round(avg_result, 1) if avg_result else None,
        "last_poll_time": last_poll.value if last_poll else None,
    }


@router.get("/{document_id}")
async def get_document(document_id: str, _: str = Depends(get_current_user)):
    try:
        oid = uuid.UUID(document_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid document ID.")

    async with get_session() as session:
        doc = await session.get(Document, oid)

    if not doc:
        raise HTTPException(status_code=404, detail="Document not found.")
    return document_to_response(doc)
