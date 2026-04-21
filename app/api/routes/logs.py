import csv
import io
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import select, func, and_

from app.core.security import get_current_user
from app.db.postgres import get_session
from app.models.activity_log import ActivityLog, log_to_response

router = APIRouter(prefix="/api/logs", tags=["logs"])


@router.get("")
async def list_logs(
    activity_type: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    _: str = Depends(get_current_user),
):
    filters = []
    if activity_type:
        filters.append(ActivityLog.activity_type == activity_type)
    if status:
        filters.append(ActivityLog.status == status)
    if date_from:
        try:
            filters.append(ActivityLog.timestamp >= datetime.fromisoformat(date_from))
        except ValueError:
            pass
    if date_to:
        try:
            filters.append(ActivityLog.timestamp <= datetime.fromisoformat(date_to))
        except ValueError:
            pass

    async with get_session() as session:
        total = await session.scalar(
            select(func.count()).select_from(ActivityLog).where(*filters)
        )
        result = await session.execute(
            select(ActivityLog).where(*filters)
            .order_by(ActivityLog.timestamp.desc())
            .offset(offset).limit(limit)
        )
        logs = result.scalars().all()

    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "items": [log_to_response(l) for l in logs],
    }


@router.get("/export")
async def export_logs(
    fmt: str = Query("csv", description="csv or json"),
    activity_type: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    _: str = Depends(get_current_user),
):
    filters = []
    if activity_type:
        filters.append(ActivityLog.activity_type == activity_type)
    if status:
        filters.append(ActivityLog.status == status)

    async with get_session() as session:
        result = await session.execute(
            select(ActivityLog).where(*filters).order_by(ActivityLog.timestamp.desc()).limit(10000)
        )
        logs = result.scalars().all()

    records = [log_to_response(l) for l in logs]

    if fmt == "json":
        import json
        content = json.dumps(records, indent=2, default=str)
        return StreamingResponse(
            io.BytesIO(content.encode()),
            media_type="application/json",
            headers={"Content-Disposition": "attachment; filename=activity_logs.json"},
        )

    if not records:
        return StreamingResponse(
            io.BytesIO(b"No data\n"),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=activity_logs.csv"},
        )

    fieldnames = list(records[0].keys())
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(records)
    output.seek(0)

    return StreamingResponse(
        io.BytesIO(output.read().encode()),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=activity_logs.csv"},
    )
