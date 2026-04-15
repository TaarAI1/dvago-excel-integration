from pydantic import BaseModel, Field
from typing import Optional, Any, Dict
from datetime import datetime
from bson import ObjectId


class ActivityLogModel(BaseModel):
    activity_type: str  # "ftp_poll" | "csv_parse" | "api_call" | "error" | "manual_trigger"
    document_id: Optional[str] = None
    document_type: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    status: str  # "success" | "failed" | "pending"
    details: Optional[str] = None
    duration_ms: Optional[float] = None
    metadata: Optional[Dict[str, Any]] = None


def log_to_response(log: dict) -> dict:
    """Convert MongoDB log document to JSON-serializable dict."""
    log = dict(log)
    if "_id" in log:
        log["id"] = str(log.pop("_id"))
    for key, val in log.items():
        if isinstance(val, ObjectId):
            log[key] = str(val)
        elif isinstance(val, datetime):
            log[key] = val.isoformat()
    return log


async def write_log(db, **kwargs) -> None:
    """Helper to insert an activity log entry."""
    entry = {
        "activity_type": kwargs.get("activity_type", "error"),
        "document_id": kwargs.get("document_id"),
        "document_type": kwargs.get("document_type"),
        "timestamp": datetime.utcnow(),
        "status": kwargs.get("status", "success"),
        "details": kwargs.get("details"),
        "duration_ms": kwargs.get("duration_ms"),
        "metadata": kwargs.get("metadata"),
    }
    await db.activity_logs.insert_one(entry)
