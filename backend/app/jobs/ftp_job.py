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
    list_all_files,
    download_csv_file,
    download_excel_file,
    move_ftp_file_to_processed,
)
from app.services.csv_processor import parse_csv, infer_document_type

logger = logging.getLogger(__name__)

# ── Module routing by filename keyword ──────────────────────────────────────────
# Add entries here as new Excel modules are introduced.
_EXCEL_MODULE_KEYWORDS: list[tuple[str, str]] = [
    ("item master",  "item_master"),
    ("item_master",  "item_master"),
]


def _detect_excel_module(filename: str) -> str | None:
    """Return the module key for an Excel file based on its name, or None if unknown."""
    lower = filename.lower()
    for keyword, module in _EXCEL_MODULE_KEYWORDS:
        if keyword in lower:
            return module
    return None


async def poll_ftp_and_ingest():
    """
    APScheduler job: poll FTP import path for new CSV and Excel files.

    Routing logic
    ─────────────
    • .csv  → csv_processor (existing doc-type inference by filename)
    • .xlsx → module detected by filename keyword:
              "item master" / "item_master"  → item_master_service

    After every file is processed (success or error) it is moved to
    {ftp_import_path}/processed/  (folder is created automatically on first use).
    Already-moved files are tracked in ftp_seen_files so they are never re-processed.
    """
    job_start = time.monotonic()
    logger.info("FTP poll job started.")

    # ── Load settings ────────────────────────────────────────────────────────
    host        = await get_setting("ftp_host",        "localhost")
    port        = int(await get_setting("ftp_port",    "21") or "21")
    user        = await get_setting("ftp_user",        "anonymous")
    password    = (await get_setting("ftp_password",   "")) or ""
    import_path = (await get_setting("ftp_import_path","/")) or "/"
    processed_path = import_path.rstrip("/") + "/processed"

    import json
    field_maps_raw = await get_setting("document_type_field_maps", "{}")
    try:
        field_maps = json.loads(field_maps_raw or "{}")
    except Exception:
        field_maps = {}

    async with get_session() as session:
        async with session.begin():
            await session.merge(
                SystemConfig(key="last_ftp_poll_start", value=datetime.utcnow().isoformat())
            )

    # ── List files ───────────────────────────────────────────────────────────
    try:
        all_files = list_all_files(host, port, user, password, import_path)
    except Exception as exc:
        logger.error(f"FTP listing failed: {exc}")
        async with get_session() as session:
            async with session.begin():
                await write_log(session, activity_type="ftp_poll", status="failed",
                                details=f"FTP listing error: {exc}")
        return

    # Filter out files already inside a 'processed' sub-path that might appear in nlst
    all_files = [f for f in all_files if "/processed" not in f.lower()]

    if not all_files:
        logger.info("No importable files found on FTP.")
        async with get_session() as session:
            async with session.begin():
                await write_log(session, activity_type="ftp_poll", status="success",
                                details="No importable files found.")
        return

    new_files      = 0
    docs_inserted  = 0
    im_processed   = 0

    for filename in all_files:
        # ── Skip seen files ──────────────────────────────────────────────────
        async with get_session() as session:
            seen = await session.get(FtpSeenFile, filename)
        if seen:
            logger.debug(f"Skipping already-processed file: {filename}")
            continue

        new_files += 1
        file_start = time.monotonic()
        lower_name = filename.lower()

        # ── Route: Excel ─────────────────────────────────────────────────────
        if lower_name.endswith(".xlsx"):
            module = _detect_excel_module(filename)
            if module is None:
                logger.warning(f"No module matched for Excel file: {filename} – skipping.")
                continue

            if module == "item_master":
                try:
                    file_bytes = download_excel_file(
                        filename, host, port, user, password, import_path
                    )
                except Exception as exc:
                    logger.error(f"Download failed for {filename}: {exc}")
                    continue

                try:
                    from app.services.item_master_service import process_excel_batch
                    result = await process_excel_batch(file_bytes, source_file=filename)
                    im_processed += result.get("total", 0)
                    summary = (
                        f"Item Master {filename}: "
                        f"total={result.get('total',0)} "
                        f"created={result.get('created',0)} "
                        f"updated={result.get('updated',0)} "
                        f"errors={result.get('errors',0)}"
                    )
                    logger.info(summary)
                    status_str = "success"
                except Exception as exc:
                    summary = f"Item Master processing failed for {filename}: {exc}"
                    logger.error(summary)
                    status_str = "failed"

                duration_ms = round((time.monotonic() - file_start) * 1000, 2)
                async with get_session() as session:
                    async with session.begin():
                        await write_log(
                            session, activity_type="item_master", status=status_str,
                            details=summary, duration_ms=duration_ms,
                            metadata={"filename": filename},
                        )

            # ── Mark seen + move to processed (all Excel modules) ────────────
            async with get_session() as session:
                async with session.begin():
                    session.add(FtpSeenFile(filename=filename, processed_at=datetime.utcnow()))

            try:
                move_ftp_file_to_processed(
                    filename, import_path, processed_path,
                    host, port, user, password,
                )
            except Exception as exc:
                logger.warning(f"Could not move {filename} to processed folder: {exc}")

            continue  # next file

        # ── Route: CSV ───────────────────────────────────────────────────────
        if lower_name.endswith(".csv"):
            # Filename matches an Excel-module keyword → must be uploaded as .xlsx
            if _detect_excel_module(filename):
                logger.warning(
                    f"Skipping '{filename}' — matches an Excel module keyword but is .csv. "
                    f"Upload as .xlsx to process via Item Master."
                )
                continue
            tmp_path = None
            try:
                tmp_path = download_csv_file(filename, host, port, user, password, import_path)
            except Exception as exc:
                logger.error(f"FTP download failed for {filename}: {exc}")
                async with get_session() as session:
                    async with session.begin():
                        await write_log(session, activity_type="ftp_poll", status="failed",
                                        details=f"Download error: {exc}",
                                        metadata={"filename": filename})
                continue

            doc_type  = infer_document_type(filename)
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

            now = datetime.utcnow()
            inserted = 0
            if records:
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
                            docs_inserted += inserted

                            session.add(FtpSeenFile(filename=filename, processed_at=now))

                            duration_ms = round((time.monotonic() - file_start) * 1000, 2)
                            await write_log(
                                session, activity_type="csv_parse", status="success",
                                details=f"Inserted {inserted} documents from {filename}",
                                duration_ms=duration_ms,
                                metadata={"filename": filename, "document_type": doc_type,
                                          "row_count": inserted},
                            )
                    logger.info(f"Inserted {inserted} docs from {filename}")
                except Exception as exc:
                    logger.error(f"DB insert failed for {filename}: {exc}")
                    async with get_session() as session:
                        async with session.begin():
                            await write_log(session, activity_type="csv_parse", status="failed",
                                            details=f"DB insert error: {exc}",
                                            metadata={"filename": filename})
                    continue
            else:
                logger.warning(f"No rows parsed from {filename}")
                async with get_session() as session:
                    async with session.begin():
                        session.add(FtpSeenFile(filename=filename, processed_at=now))

            # Move CSV to processed folder
            try:
                move_ftp_file_to_processed(
                    filename, import_path, processed_path,
                    host, port, user, password,
                )
            except Exception as exc:
                logger.warning(f"Could not move {filename} to processed folder: {exc}")

    # ── Final summary ────────────────────────────────────────────────────────
    total_duration = (time.monotonic() - job_start) * 1000
    summary = (
        f"Processed {new_files} new files — "
        f"{docs_inserted} CSV docs inserted, {im_processed} Item Master rows processed."
    )
    logger.info(f"FTP poll job done. {summary}")

    async with get_session() as session:
        async with session.begin():
            await write_log(
                session, activity_type="ftp_poll", status="success",
                details=summary, duration_ms=round(total_duration, 2),
                metadata={"new_files": new_files, "docs_inserted": docs_inserted,
                          "im_processed": im_processed},
            )
            await session.merge(
                SystemConfig(key="last_ftp_poll_success", value=datetime.utcnow().isoformat())
            )
