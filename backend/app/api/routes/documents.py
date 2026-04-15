from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional, List
from bson import ObjectId
from datetime import datetime

from app.core.security import get_current_user
from app.db.mongodb import get_db
from app.models.document import document_to_response

router = APIRouter(prefix="/api/documents", tags=["documents"])


def _build_filter(
    document_type: Optional[str],
    status: Optional[str],
    date_from: Optional[str],
    date_to: Optional[str],
) -> dict:
    query = {}
    if document_type:
        query["document_type"] = document_type
    if status == "posted":
        query["posted"] = True
    elif status == "error":
        query["has_error"] = True
    elif status == "pending":
        query["posted"] = False
        query["has_error"] = False

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
        query["created_at"] = date_filter

    return query


@router.get("")
async def list_documents(
    document_type: Optional[str] = Query(None),
    status: Optional[str] = Query(None, description="posted | error | pending"),
    date_from: Optional[str] = Query(None, description="ISO datetime"),
    date_to: Optional[str] = Query(None, description="ISO datetime"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    _: str = Depends(get_current_user),
):
    """List documents with optional filtering and pagination."""
    db = get_db()
    query = _build_filter(document_type, status, date_from, date_to)

    total = await db.documents.count_documents(query)
    cursor = db.documents.find(query).sort("created_at", -1).skip(offset).limit(limit)
    docs = await cursor.to_list(length=limit)

    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "items": [document_to_response(d) for d in docs],
    }


@router.get("/stats")
async def get_stats(_: str = Depends(get_current_user)):
    """Aggregated document statistics."""
    db = get_db()
    total = await db.documents.count_documents({})
    posted = await db.documents.count_documents({"posted": True})
    errors = await db.documents.count_documents({"has_error": True, "posted": False})
    pending = await db.documents.count_documents({"posted": False, "has_error": False})

    # Today's stats
    from datetime import date, timezone
    today_start = datetime.combine(date.today(), datetime.min.time())
    total_today = await db.documents.count_documents({"created_at": {"$gte": today_start}})
    posted_today = await db.documents.count_documents({"posted": True, "posted_at": {"$gte": today_start}})

    # Last poll info
    last_poll = await db.system_config.find_one({"key": "last_ftp_poll_success"})
    last_poll_time = last_poll["value"] if last_poll else None

    # Average API duration from recent logs
    pipeline = [
        {"$match": {"activity_type": "api_call", "status": "success", "duration_ms": {"$exists": True}}},
        {"$group": {"_id": None, "avg_ms": {"$avg": "$duration_ms"}}},
    ]
    avg_result = await db.activity_logs.aggregate(pipeline).to_list(length=1)
    avg_api_ms = round(avg_result[0]["avg_ms"], 1) if avg_result else None

    return {
        "total": total,
        "posted": posted,
        "errors": errors,
        "pending": pending,
        "total_today": total_today,
        "posted_today": posted_today,
        "post_rate_pct": round(posted / total * 100, 1) if total else 0,
        "error_rate_pct": round(errors / total * 100, 1) if total else 0,
        "avg_api_response_ms": avg_api_ms,
        "last_poll_time": last_poll_time,
    }


@router.get("/{document_id}")
async def get_document(document_id: str, _: str = Depends(get_current_user)):
    """Get a single document by ID."""
    db = get_db()
    try:
        oid = ObjectId(document_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid document ID.")

    doc = await db.documents.find_one({"_id": oid})
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found.")
    return document_to_response(doc)
