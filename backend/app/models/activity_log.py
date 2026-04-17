import uuid
from datetime import datetime
from typing import Any
from sqlalchemy import String, Text, Float, DateTime
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import UUID, JSONB
from app.db.postgres import Base
from app.core.timezone import now_pkt


class ActivityLog(Base):
    __tablename__ = "activity_logs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    activity_type: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    document_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    document_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, default=now_pkt, nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    details: Mapped[str | None] = mapped_column(Text, nullable=True)
    duration_ms: Mapped[float | None] = mapped_column(Float, nullable=True)
    metadata_: Mapped[dict | None] = mapped_column("metadata", JSONB, nullable=True)


def log_to_response(log: ActivityLog) -> dict:
    return {
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


async def write_log(session, **kwargs) -> None:
    """Insert an activity log entry within the provided session."""
    doc_id_raw = kwargs.get("document_id")
    doc_id = None
    if doc_id_raw:
        try:
            doc_id = uuid.UUID(str(doc_id_raw))
        except (ValueError, AttributeError):
            doc_id = None

    log = ActivityLog(
        activity_type=kwargs.get("activity_type", "error"),
        document_id=doc_id,
        document_type=kwargs.get("document_type"),
        timestamp=now_pkt(),
        status=kwargs.get("status", "success"),
        details=kwargs.get("details"),
        duration_ms=kwargs.get("duration_ms"),
        metadata_=kwargs.get("metadata"),
    )
    session.add(log)
    await session.flush()
