import time
import logging
from datetime import datetime

from app.db.postgres import get_session
from app.db.settings_store import get_setting
from app.models.activity_log import write_log
from app.models.ftp_seen_file import FtpSeenFile
from app.models.system_config import SystemConfig
from app.services.ftp_service import (
    list_all_files,
    download_excel_file,
    move_ftp_file_to_processed,
)

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


async def _poll_item_master(host: str, port: int, user: str, password: str) -> tuple[int, int]:
    """
    Poll the Item Master import path for new .xlsx files and process them.
    Returns (new_files_count, rows_processed_count).
    """
    import_path = (await get_setting("ftp_import_path", "/")) or "/"
    processed_path = import_path.rstrip("/") + "/processed"

    try:
        all_files = list_all_files(host, port, user, password, import_path)
    except Exception as exc:
        logger.error(f"[Item Master] FTP listing failed for path '{import_path}': {exc}")
        return 0, 0

    all_files = [f for f in all_files if "/processed" not in f.lower() and f.lower().endswith(".xlsx")]

    new_files = 0
    im_processed = 0

    for filename in all_files:
        async with get_session() as session:
            seen = await session.get(FtpSeenFile, filename)
        if seen:
            logger.debug(f"[Item Master] Skipping already-processed file: {filename}")
            continue

        module = _detect_excel_module(filename)
        if module != "item_master":
            logger.warning(f"[Item Master] File '{filename}' does not match item master keywords – skipping.")
            continue

        new_files += 1
        file_start = time.monotonic()

        try:
            file_bytes = download_excel_file(filename, host, port, user, password, import_path)
        except Exception as exc:
            logger.error(f"[Item Master] Download failed for {filename}: {exc}")
            continue

        try:
            from app.services.item_master_service import process_excel_batch
            result = await process_excel_batch(file_bytes, source_file=filename)
            im_processed += result.get("total", 0)
            summary = (
                f"Item Master {filename}: "
                f"total={result.get('total', 0)} "
                f"created={result.get('created', 0)} "
                f"updated={result.get('updated', 0)} "
                f"errors={result.get('errors', 0)}"
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
                session.add(FtpSeenFile(filename=filename, processed_at=datetime.utcnow()))

        try:
            move_ftp_file_to_processed(filename, import_path, processed_path, host, port, user, password)
        except Exception as exc:
            logger.warning(f"[Item Master] Could not move {filename} to processed: {exc}")

    return new_files, im_processed


async def poll_ftp_and_ingest():
    """
    APScheduler job: poll all configured FTP import paths for new files.

    Routing logic
    ─────────────
    • Item Master import path  → .xlsx files matching "item master" keyword
                                 → item_master_service
    • (CSV paths for qty adjust, price adjustment, transfers, GRN — future modules)

    After every file is processed it is moved to {path}/processed/ and tracked
    in ftp_seen_files so it is never re-processed.
    """
    job_start = time.monotonic()
    logger.info("FTP poll job started.")

    # ── Load FTP connection settings ─────────────────────────────────────────
    host     = await get_setting("ftp_host",     "localhost")
    port     = int(await get_setting("ftp_port", "21") or "21")
    user     = await get_setting("ftp_user",     "anonymous")
    password = (await get_setting("ftp_password", "")) or ""

    async with get_session() as session:
        async with session.begin():
            await session.merge(
                SystemConfig(key="last_ftp_poll_start", value=datetime.utcnow().isoformat())
            )

    new_files     = 0
    docs_inserted = 0
    im_processed  = 0

    # ── Poll Item Master import path ─────────────────────────────────────────
    im_new, im_rows = await _poll_item_master(host, port, user, password)
    new_files    += im_new
    im_processed += im_rows

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
