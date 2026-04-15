import os
import time
import logging
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.db.postgres import get_session
from app.models.document import Document
from app.models.activity_log import ActivityLog, write_log
from app.models.ftp_seen_file import FtpSeenFile
from app.models.system_config import SystemConfig
from app.services.ftp_service import list_csv_files, download_csv_file
from app.services.csv_processor import parse_csv, infer_document_type
from app.core.config import settings

logger = logging.getLogger(__name__)


async def poll_ftp_and_ingest():
    """
    APScheduler job: poll FTP for new CSV files, parse and insert into PostgreSQL.
    Skips files already tracked in ftp_seen_files table.
    """
    job_start = time.monotonic()
    logger.info("FTP poll job started.")

    async with get_session() as session:
        async with session.begin():
            await session.merge(SystemConfig(key="last_ftp_poll_start", value=datetime.utcnow().isoformat()))

    try:
        csv_files = list_csv_files()
    except Exception as exc:
        logger.error(f"FTP listing failed: {exc}")
        async with get_session() as session:
            async with session.begin():
                await write_log(session, activity_type="ftp_poll", status="failed", details=f"FTP listing error: {exc}")
        return

    if not csv_files:
        logger.info("No CSV files found on FTP.")
        async with get_session() as session:
            async with session.begin():
                await write_log(session, activity_type="ftp_poll", status="success", details="No CSV files found.")
        return

    field_maps = settings.get_document_type_field_maps()
    new_files_count = 0
    total_docs_inserted = 0

    for filename in csv_files:
        # Check if already processed
        async with get_session() as session:
            seen = await session.get(FtpSeenFile, filename)

        if seen:
            logger.debug(f"Skipping already-processed file: {filename}")
            continue

        new_files_count += 1
        tmp_path = None
        file_start = time.monotonic()

        try:
            tmp_path = download_csv_file(filename)
        except Exception as exc:
            logger.error(f"FTP download failed for {filename}: {exc}")
            async with get_session() as session:
                async with session.begin():
                    await write_log(session, activity_type="ftp_poll", status="failed",
                                    details=f"Download error: {exc}", metadata={"filename": filename})
            continue

        doc_type = infer_document_type(filename)
        field_map = field_maps.get(doc_type, {})

        try:
            records = parse_csv(tmp_path, doc_type, field_map)
        except Exception as exc:
            logger.error(f"CSV parse failed for {filename}: {exc}")
            async with get_session() as session:
                async with session.begin():
                    await write_log(session, activity_type="csv_parse", status="failed",
                                    details=f"Parse error: {exc}",
                                    metadata={"filename": filename, "document_type": doc_type})
            continue
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)

        if not records:
            logger.warning(f"No rows parsed from {filename}")
            async with get_session() as session:
                async with session.begin():
                    session.add(FtpSeenFile(filename=filename, processed_at=datetime.utcnow()))
            continue

        now = datetime.utcnow()
        try:
            async with get_session() as session:
                async with session.begin():
                    documents = [
                        Document(
                            document_type=doc_type,
                            original_data=row,
                            posted=False,
                            has_error=False,
                            created_at=now,
                            updated_at=now,
                            source_file=filename,
                        )
                        for row in records
                    ]
                    session.add_all(documents)
                    await session.flush()
                    inserted = len(documents)
                    total_docs_inserted += inserted

                    session.add(FtpSeenFile(filename=filename, processed_at=now))

                    file_duration = (time.monotonic() - file_start) * 1000
                    await write_log(session, activity_type="csv_parse", status="success",
                                    details=f"Inserted {inserted} documents from {filename}",
                                    duration_ms=round(file_duration, 2),
                                    metadata={"filename": filename, "document_type": doc_type, "row_count": inserted})

            logger.info(f"Inserted {inserted} docs from {filename}")

        except Exception as exc:
            logger.error(f"DB insert failed for {filename}: {exc}")
            async with get_session() as session:
                async with session.begin():
                    await write_log(session, activity_type="csv_parse", status="failed",
                                    details=f"DB insert error: {exc}", metadata={"filename": filename})

    total_duration = (time.monotonic() - job_start) * 1000
    summary = f"Processed {new_files_count} new files, {total_docs_inserted} documents inserted."
    logger.info(f"FTP poll job done. {summary}")

    async with get_session() as session:
        async with session.begin():
            await write_log(session, activity_type="ftp_poll", status="success",
                            details=summary, duration_ms=round(total_duration, 2),
                            metadata={"new_files": new_files_count, "docs_inserted": total_docs_inserted})
            await session.merge(SystemConfig(key="last_ftp_poll_success", value=datetime.utcnow().isoformat()))
