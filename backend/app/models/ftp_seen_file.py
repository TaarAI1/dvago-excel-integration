from datetime import datetime
from sqlalchemy import String, DateTime
from sqlalchemy.orm import Mapped, mapped_column
from app.db.postgres import Base


class FtpSeenFile(Base):
    __tablename__ = "ftp_seen_files"

    filename: Mapped[str] = mapped_column(String(500), primary_key=True)
    processed_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
