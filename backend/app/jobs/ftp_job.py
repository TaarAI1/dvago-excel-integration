import os
import time
import logging
from datetime import datetime

from sqlalchemy import select

from app.db.postgres import get_session
from app.db.settings_store import get_setting
from app.models.document import Document
from app.models.activity_log import ActivityLog, write_log
from app.models.ftp_seen_file import FtpSeenFile
from app.models.system_config import SystemConfig
from app.services.ftp_service import (
    list_csv_files, download_csv_file,
    list_excel_files, download_excel_file, move_ftp_file_to_processed,
)
from app.services.csv_processor import parse_csv, infer_document_type

logger = logging.getLogger(__name__)


async def poll_ftp_and_ingest():
    """
    APScheduler job: poll FTP for new CSV files, parse and insert into PostgreSQL.
    Skips files already tracked in ftp_seen_files table.
    """
    job_start = time.monotonic()
    logger.info("FTP poll job started.")

    # Load FTP settings from DB
    host = await get_setting("ftp_host", "localhost")
    port = int(await get_setting("ftp_port", "21") or "21")
    user = await get_setting("ftp_user", "anonymous")
    password = await get_setting("ftp_password", "") or ""
    import_path = await get_setting("ftp_import_path", "/") or "/"

    # Load field maps from DB
    import json
    field_maps_raw = await get_setting("document_type_field_maps", "{}")
    try:
        field_maps = json.loads(field_maps_raw or "{}")
    except Exception:
        field_maps = {}

    async with get_session() as session:
        async with session.begin():
            await session.merge(SystemConfig(key="last_ftp_poll_start", value=datetime.utcnow().isoformat()))

    try:
        csv_files = list_csv_files(host, port, user, password, import_path)
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

    new_files_count = 0
    total_docs_inserted = 0

    for filename in csv_files:
        async with get_session() as session:
            seen = await session.get(FtpSeenFile, filename)

        if seen:
            logger.debug(f"Skipping already-processed file: {filename}")
            continue

        new_files_count += 1
        tmp_path = None
        file_start = time.monotonic()

        try:
            tmp_path = download_csv_file(filename, host, port, user, password, import_path)
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
    summary = f"Processed {new_files_count} new CSV files, {total_docs_inserted} documents inserted."
    logger.info(f"FTP poll job done (CSV). {summary}")

    async with get_session() as session:
        async with session.begin():
            await write_log(session, activity_type="ftp_poll", status="success",
                            details=summary, duration_ms=round(total_duration, 2),
                            metadata={"new_files": new_files_count, "docs_inserted": total_docs_inserted})
            await session.merge(SystemConfig(key="last_ftp_poll_success", value=datetime.utcnow().isoformat()))

    # ── Item Master Excel processing ─────────────────────────────────────────
    await _poll_item_master_excel(host, port, user, password)


async def _poll_item_master_excel(
    ftp_host: str, ftp_port: int, ftp_user: str, ftp_password: str
) -> None:
    """
    Check the configured Item Master FTP path for new .xlsx files.
    For each unseen file: download → process via item_master_service
    → move to processed folder on FTP.
    """
    from app.services.item_master_service import process_excel_batch

    im_path       = (await get_setting("item_master_ftp_path"))       or ""
    im_proc_path  = (await get_setting("item_master_processed_path"))  or ""

    if not im_path:
        logger.debug("item_master_ftp_path not configured – skipping.")
        return

    try:
        excel_files = list_excel_files(ftp_host, ftp_port, ftp_user, ftp_password, im_path)
    except Exception as exc:
        logger.error(f"Item Master FTP listing failed: {exc}")
        return

    if not excel_files:
        logger.info("No new Item Master Excel files on FTP.")
        return

    for filename in excel_files:
        # Use ftp_seen_files to avoid re-processing
        seen_key = f"im:{im_path}/{filename}"
        async with get_session() as session:
            seen = await session.get(FtpSeenFile, seen_key)
        if seen:
            logger.debug(f"Skipping already-processed Item Master file: {filename}")
            continue

        logger.info(f"Processing Item Master file: {filename}")
        file_start = time.monotonic()

        try:
            file_bytes = download_excel_file(filename, ftp_host, ftp_port, ftp_user, ftp_password, im_path)
        except Exception as exc:
            logger.error(f"Download failed for Item Master {filename}: {exc}")
            continue

        try:
            result = await process_excel_batch(file_bytes, source_file=filename)
        except Exception as exc:
            logger.error(f"Item Master processing failed for {filename}: {exc}")
            async with get_session() as session:
                async with session.begin():
                    await write_log(session, activity_type="item_master", status="failed",
                                    details=str(exc), metadata={"filename": filename})
            continue

        # Mark as seen so we don't re-process on next run
        async with get_session() as session:
            async with session.begin():
                session.add(FtpSeenFile(filename=seen_key, processed_at=datetime.utcnow()))

        # Move file to processed folder on FTP
        if im_proc_path:
            try:
                move_ftp_file_to_processed(
                    filename, im_path, im_proc_path,
                    ftp_host, ftp_port, ftp_user, ftp_password,
                )
            except Exception as exc:
                logger.warning(f"Could not move {filename} to processed folder: {exc}")

        duration_ms = round((time.monotonic() - file_start) * 1000, 2)
        summary = (f"Item Master {filename}: total={result.get('total',0)} "
                   f"created={result.get('created',0)} updated={result.get('updated',0)} "
                   f"errors={result.get('errors',0)}")
        logger.info(summary)
        async with get_session() as session:
            async with session.begin():
                await write_log(session, activity_type="item_master", status="success",
                                details=summary, duration_ms=duration_ms,
                                metadata={"filename": filename, **{k: result.get(k) for k in
                                          ("total", "created", "updated", "errors")}})
