import asyncio
import json
from datetime import datetime
from typing import Optional, AsyncGenerator

from fastapi import APIRouter, Query, HTTPException, status
from fastapi.responses import StreamingResponse
from jose import JWTError, jwt
from sqlalchemy import select, func, and_

from app.core.config import settings
from app.db.postgres import get_session
from app.models.document import Document
from app.models.activity_log import ActivityLog
from app.models.system_config import SystemConfig

router = APIRouter(prefix="/api/stream", tags=["stream"])


def _verify_sse_token(token: Optional[str]) -> str:
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing token")
    try:
        payload = jwt.decode(token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm])
        username = payload.get("sub")
        if not username:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
        return username
    except JWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")


async def _stats_generator() -> AsyncGenerator[str, None]:
    from datetime import date
    while True:
        try:
            today_start = datetime.combine(date.today(), datetime.min.time())
            async with get_session() as session:
                total = await session.scalar(select(func.count()).select_from(Document)) or 0
                posted = await session.scalar(select(func.count()).select_from(Document).where(Document.posted == True)) or 0
                errors = await session.scalar(select(func.count()).select_from(Document).where(
                    and_(Document.has_error == True, Document.posted == False))) or 0
                pending = await session.scalar(select(func.count()).select_from(Document).where(
                    and_(Document.posted == False, Document.has_error == False))) or 0
                total_today = await session.scalar(select(func.count()).select_from(Document).where(
                    Document.created_at >= today_start)) or 0
                posted_today = await session.scalar(select(func.count()).select_from(Document).where(
                    and_(Document.posted == True, Document.posted_at >= today_start))) or 0
                last_poll = await session.get(SystemConfig, "last_ftp_poll_success")

            payload = {
                "total": total,
                "posted": posted,
                "errors": errors,
                "pending": pending,
                "total_today": total_today,
                "posted_today": posted_today,
                "post_rate_pct": round(posted / total * 100, 1) if total else 0,
                "last_poll_time": last_poll.value if last_poll else None,
                "ts": datetime.utcnow().isoformat(),
            }
            yield f"data: {json.dumps(payload)}\n\n"
        except Exception:
            yield f"data: {json.dumps({'error': 'stats unavailable'})}\n\n"

        await asyncio.sleep(5)


async def _logs_generator() -> AsyncGenerator[str, None]:
    last_ts = datetime.utcnow()
    while True:
        try:
            async with get_session() as session:
                result = await session.execute(
                    select(ActivityLog)
                    .where(ActivityLog.timestamp > last_ts)
                    .order_by(ActivityLog.timestamp.asc())
                    .limit(50)
                )
                new_logs = result.scalars().all()

            for log in new_logs:
                log_data = {
                    "id": str(log.id),
                    "activity_type": log.activity_type,
                    "document_id": str(log.document_id) if log.document_id else None,
                    "document_type": log.document_type,
                    "timestamp": log.timestamp.isoformat() if log.timestamp else None,
                    "status": log.status,
                    "details": log.details,
                    "duration_ms": log.duration_ms,
                    "metadata": log.metadata_,
                }
                yield f"data: {json.dumps(log_data)}\n\n"

            if new_logs:
                last_ts = new_logs[-1].timestamp

        except Exception:
            pass

        await asyncio.sleep(3)


@router.get("/dashboard")
async def stream_dashboard(token: Optional[str] = Query(None)):
    _verify_sse_token(token)
    return StreamingResponse(
        _stats_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/logs")
async def stream_logs(token: Optional[str] = Query(None)):
    _verify_sse_token(token)
    return StreamingResponse(
        _logs_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
