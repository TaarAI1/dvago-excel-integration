import csv
import io
from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from typing import Optional
from datetime import datetime

from app.core.security import get_current_user
from app.db.mongodb import get_db
from app.models.activity_log import log_to_response

router = APIRouter(prefix="/api/logs", tags=["logs"])


@router.get("")
async def list_logs(
    activity_type: Optional[str] = Query(None),
    status: Optional[str] = Query(None, description="success | failed | pending"),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    _: str = Depends(get_current_user),
):
    """Paginated activity log with optional filtering."""
    db = get_db()
    query = {}
    if activity_type:
        query["activity_type"] = activity_type
    if status:
        query["status"] = status

    date_filter = {}
    if date_from:
        try:
            date_filter["$gte"] = datetime.fromisoformat(date_from)
        except ValueError:
            pass
    if date_to:
        try:
            date_filter["$lte"] = datetime.fromisoformat(date_to)
        except ValueError:
            pass
    if date_filter:
        query["timestamp"] = date_filter

    total = await db.activity_logs.count_documents(query)
    cursor = db.activity_logs.find(query).sort("timestamp", -1).skip(offset).limit(limit)
    logs = await cursor.to_list(length=limit)

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
    """Export all logs as CSV or JSON file download."""
    db = get_db()
    query = {}
    if activity_type:
        query["activity_type"] = activity_type
    if status:
        query["status"] = status

    cursor = db.activity_logs.find(query).sort("timestamp", -1)
    logs = await cursor.to_list(length=10000)
    records = [log_to_response(l) for l in logs]

    if fmt == "json":
        import json
        content = json.dumps(records, indent=2, default=str)
        return StreamingResponse(
            io.BytesIO(content.encode()),
            media_type="application/json",
            headers={"Content-Disposition": "attachment; filename=activity_logs.json"},
        )

    # CSV export
    if not records:
        output = io.StringIO()
        output.write("No data\n")
        output.seek(0)
        return StreamingResponse(
            io.BytesIO(output.read().encode()),
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
