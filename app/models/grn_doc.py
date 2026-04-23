import uuid
from datetime import datetime
from sqlalchemy import String, Integer, Text, DateTime
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import UUID, JSONB
from app.db.postgres import Base


class GRNDoc(Base):
    __tablename__ = "grn_docs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source_file: Mapped[str | None] = mapped_column(String(500), nullable=True, index=True)
    note: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    store_code: Mapped[str | None] = mapped_column(String(100), nullable=True)
    store_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    storesid: Mapped[str | None] = mapped_column(String(255), nullable=True)
    sbssid: Mapped[str | None] = mapped_column(String(255), nullable=True)
    vendsid: Mapped[str | None] = mapped_column(String(255), nullable=True)
    vousid: Mapped[str | None] = mapped_column(String(255), nullable=True)
    item_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    posted_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    error_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    status: Mapped[str] = mapped_column(String(20), default="pending", nullable=False, index=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Full API call traces
    api_create_payload: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    api_create_response: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    api_get_rowversion_response: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    api_vendor_payload: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    api_vendor_response: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    api_items_payload: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    api_items_response: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    api_comment_payload: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    api_comment_response: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    api_get_rowversion2_response: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    api_finalize_payload: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    api_finalize_response: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    # Per-item detail: [{upc, qty, item_sid, ok, error}]
    items_data: Mapped[list | None] = mapped_column(JSONB, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    posted_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


def grn_doc_to_response(doc: GRNDoc) -> dict:
    return {
        "id": str(doc.id),
        "source_file": doc.source_file,
        "note": doc.note,
        "store_code": doc.store_code,
        "store_name": doc.store_name,
        "storesid": doc.storesid,
        "sbssid": doc.sbssid,
        "vendsid": doc.vendsid,
        "vousid": doc.vousid,
        "item_count": doc.item_count,
        "posted_count": doc.posted_count,
        "error_count": doc.error_count,
        "status": doc.status,
        "error_message": doc.error_message,
        "api_create_payload": doc.api_create_payload,
        "api_create_response": doc.api_create_response,
        "api_get_rowversion_response": doc.api_get_rowversion_response,
        "api_vendor_payload": doc.api_vendor_payload,
        "api_vendor_response": doc.api_vendor_response,
        "api_items_payload": doc.api_items_payload,
        "api_items_response": doc.api_items_response,
        "api_comment_payload": doc.api_comment_payload,
        "api_comment_response": doc.api_comment_response,
        "api_get_rowversion2_response": doc.api_get_rowversion2_response,
        "api_finalize_payload": doc.api_finalize_payload,
        "api_finalize_response": doc.api_finalize_response,
        "items_data": doc.items_data,
        "created_at": doc.created_at.isoformat() if doc.created_at else None,
        "posted_at": doc.posted_at.isoformat() if doc.posted_at else None,
    }
