import asyncio
import json
from datetime import datetime
from typing import AsyncGenerator, Optional

from fastapi import APIRouter, Depends, Query, HTTPException, status
from fastapi.responses import StreamingResponse
from jose import JWTError, jwt

from app.core.config import settings
from app.db.mongodb import get_db

router = APIRouter(prefix="/api/stream", tags=["stream"])


async def _stats_generator(db) -> AsyncGenerator[str, None]:
    """Yield dashboard stats as SSE events every 5 seconds."""
    while True:
        try:
            total = await db.documents.count_documents({})
            posted = await db.documents.count_documents({"posted": True})
            errors = await db.documents.count_documents({"has_error": True, "posted": False})
            pending = await db.documents.count_documents({"posted": False, "has_error": False})

            from datetime import date
            today_start = datetime.combine(date.today(), datetime.min.time())
            total_today = await db.documents.count_documents({"created_at": {"$gte": today_start}})
            posted_today = await db.documents.count_documents(
                {"posted": True, "posted_at": {"$gte": today_start}}
            )

            last_poll = await db.system_config.find_one({"key": "last_ftp_poll_success"})
            last_poll_time = last_poll["value"] if last_poll else None

            payload = {
                "total": total,
                "posted": posted,
                "errors": errors,
                "pending": pending,
                "total_today": total_today,
                "posted_today": posted_today,
                "post_rate_pct": round(posted / total * 100, 1) if total else 0,
                "last_poll_time": last_poll_time,
                "ts": datetime.utcnow().isoformat(),
            }
            yield f"data: {json.dumps(payload)}\n\n"
        except Exception:
            yield f"data: {json.dumps({'error': 'stats unavailable'})}\n\n"

        await asyncio.sleep(5)


async def _logs_generator(db) -> AsyncGenerator[str, None]:
    """
    Yield new activity log entries as SSE events every 3 seconds.
    Uses a cursor timestamp to only send entries newer than the last sent one.
    """
    last_ts = datetime.utcnow()

    while True:
        try:
            cursor = db.activity_logs.find(
                {"timestamp": {"$gt": last_ts}}
            ).sort("timestamp", 1)
            new_logs = await cursor.to_list(length=50)

            for log in new_logs:
                log_data = {}
                for k, v in log.items():
                    if k == "_id":
                        log_data["id"] = str(v)
                    elif isinstance(v, datetime):
                        log_data[k] = v.isoformat()
                    else:
                        log_data[k] = v
                if new_logs:
                    last_ts = new_logs[-1]["timestamp"]
                yield f"data: {json.dumps(log_data)}\n\n"

        except Exception:
            pass

        await asyncio.sleep(3)


def _verify_sse_token(token: Optional[str]) -> str:
    """Verify JWT token passed as query param for SSE endpoints."""
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


@router.get("/dashboard")
async def stream_dashboard(token: Optional[str] = Query(None)):
    """SSE stream: push dashboard statistics every 5 seconds."""
    _verify_sse_token(token)
    db = get_db()
    return StreamingResponse(
        _stats_generator(db),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/logs")
async def stream_logs(token: Optional[str] = Query(None)):
    """SSE stream: push new activity log entries as they arrive (polled every 3s)."""
    _verify_sse_token(token)
    db = get_db()
    return StreamingResponse(
        _logs_generator(db),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
