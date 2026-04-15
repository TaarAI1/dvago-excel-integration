import uuid
from datetime import datetime
from sqlalchemy import String, Boolean, Text, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import UUID, JSONB
from app.db.postgres import Base


class Document(Base):
    __tablename__ = "documents"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    document_type: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    original_data: Mapped[dict] = mapped_column(JSONB, nullable=False)
    retailprosid: Mapped[str | None] = mapped_column(String(255), nullable=True)
    posted: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False, index=True)
    has_error: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False, index=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    posted_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    source_file: Mapped[str | None] = mapped_column(String(500), nullable=True)


def document_to_response(doc: Document) -> dict:
    return {
        "id": str(doc.id),
        "document_type": doc.document_type,
        "original_data": doc.original_data,
        "retailprosid": doc.retailprosid,
        "posted": doc.posted,
        "has_error": doc.has_error,
        "error_message": doc.error_message,
        "created_at": doc.created_at.isoformat() if doc.created_at else None,
        "updated_at": doc.updated_at.isoformat() if doc.updated_at else None,
        "posted_at": doc.posted_at.isoformat() if doc.posted_at else None,
        "source_file": doc.source_file,
    }
