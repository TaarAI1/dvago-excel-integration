import uuid
from datetime import datetime
from sqlalchemy import String, Integer, Float, Text, DateTime
from sqlalchemy.orm import Mapped, mapped_column
from app.db.postgres import Base


class SalesExportRun(Base):
    """One row per export session (scheduler fire or manual trigger)."""
    __tablename__ = "sales_export_runs"

    run_id:          Mapped[str]       = mapped_column(String(36), primary_key=True)
    label:           Mapped[str]       = mapped_column(String(200), nullable=False)  # "sales-export 2026-04-21 14:30:00"
    triggered_by:    Mapped[str]       = mapped_column(String(20), default="scheduler", nullable=False)  # scheduler | manual
    status:          Mapped[str]       = mapped_column(String(20), default="running", nullable=False, index=True)
    total_stores:    Mapped[int]       = mapped_column(Integer, default=0, nullable=False)
    processed_stores: Mapped[int]     = mapped_column(Integer, default=0, nullable=False)
    started_at:      Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    finished_at:     Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class SalesExportStore(Base):
    """One row per store within an export run."""
    __tablename__ = "sales_export_stores"

    id:           Mapped[uuid.UUID] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    run_id:       Mapped[str]       = mapped_column(String(36), nullable=False, index=True)
    store_no:     Mapped[int | None]= mapped_column(Integer, nullable=True)
    store_name:   Mapped[str | None]= mapped_column(String(255), nullable=True)
    query_rows:   Mapped[int]       = mapped_column(Integer, default=0, nullable=False)
    written_rows: Mapped[int]       = mapped_column(Integer, default=0, nullable=False)
    filename:     Mapped[str | None]= mapped_column(String(500), nullable=True)
    ftp_path:     Mapped[str | None]= mapped_column(String(500), nullable=True)
    status:       Mapped[str]       = mapped_column(String(20), default="pending", nullable=False)
    error_message:Mapped[str | None]= mapped_column(Text, nullable=True)
    duration_ms:  Mapped[float | None] = mapped_column(Float, nullable=True)
    created_at:   Mapped[datetime]  = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


def run_to_response(r: SalesExportRun) -> dict:
    return {
        "run_id":           r.run_id,
        "label":            r.label,
        "triggered_by":     r.triggered_by,
        "status":           r.status,
        "total_stores":     r.total_stores,
        "processed_stores": r.processed_stores,
        "started_at":       r.started_at.isoformat() if r.started_at else None,
        "finished_at":      r.finished_at.isoformat() if r.finished_at else None,
    }


def store_to_response(s: SalesExportStore) -> dict:
    return {
        "id":           str(s.id),
        "run_id":       s.run_id,
        "store_no":     s.store_no,
        "store_name":   s.store_name,
        "query_rows":   s.query_rows,
        "written_rows": s.written_rows,
        "filename":     s.filename,
        "ftp_path":     s.ftp_path,
        "status":       s.status,
        "error_message":s.error_message,
        "duration_ms":  s.duration_ms,
        "created_at":   s.created_at.isoformat() if s.created_at else None,
    }
