import time
import logging

from app.core.timezone import now_pkt
from app.db.postgres import get_session
from app.db.settings_store import get_setting
from app.models.activity_log import write_log
from app.models.system_config import SystemConfig
from app.services.ftp_service import (
    list_all_files,
    download_excel_file,
    move_ftp_file_to_processed,
)

logger = logging.getLogger(__name__)

_IM_EXTS = (".xlsx", ".csv")


async def _poll_item_master(host: str, port: int, user: str, password: str) -> tuple[int, int]:
    """
    Poll the Item Master import path and process every .xlsx and .csv file found.

    - Accepts both Excel (.xlsx) and CSV (.csv) — routed by file extension.
    - No filename keyword filtering: any supported file in the path is processed.
    - No seen-file deduplication: files are always picked up, so uploading a file
      with the same name as a previous batch will always be reprocessed.
    - After processing, the file is moved to {import_path}/processed/ with a timestamp
      suffix so the source folder stays clean for the next upload.

    Returns (files_processed_count, rows_processed_count).
    """
    import_path = (await get_setting("ftp_import_path", "/")) or "/"
    processed_path = import_path.rstrip("/") + "/processed"

    try:
        all_files = list_all_files(host, port, user, password, import_path)
    except Exception as exc:
        logger.error(f"[Item Master] FTP listing failed for path '{import_path}': {exc}")
        return 0, 0

    # Keep only supported extensions; exclude anything inside a processed sub-folder
    all_files = [
        f for f in all_files
        if f.lower().endswith(_IM_EXTS) and "processed" not in f.lower()
    ]

    if not all_files:
        logger.info("[Item Master] No .xlsx or .csv files found in import path.")
        return 0, 0

    files_processed = 0
    im_processed = 0

    for filename in all_files:
        files_processed += 1
        file_start = time.monotonic()

        try:
            file_bytes = download_excel_file(filename, host, port, user, password, import_path)
        except Exception as exc:
            logger.error(f"[Item Master] Download failed for {filename}: {exc}")
            continue

        try:
            from app.services.item_master_service import process_excel_batch, process_csv_batch
            # Unique batch key: filename + timestamp so re-uploads of the same
            # filename always produce a separate, distinct batch.
            batch_key = f"{filename}::{now_pkt().strftime('%Y%m%d_%H%M%S')}"
            if filename.lower().endswith(".csv"):
                result = await process_csv_batch(file_bytes, source_file=batch_key)
            else:
                result = await process_excel_batch(file_bytes, source_file=batch_key)

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

        try:
            move_ftp_file_to_processed(filename, import_path, processed_path, host, port, user, password)
        except Exception as exc:
            logger.warning(f"[Item Master] Could not move {filename} to processed: {exc}")

    return files_processed, im_processed


async def _poll_qty_adjustment(host: str, port: int, user: str, password: str) -> tuple[int, int]:
    """
    Poll the Qty Adjustment import path and process every .csv file found.
    Returns (files_processed_count, docs_processed_count).
    """
    import_path = (await get_setting("ftp_qty_adjust_import_path", "/")) or "/"
    processed_path = import_path.rstrip("/") + "/processed"

    try:
        all_files = list_all_files(host, port, user, password, import_path)
    except Exception as exc:
        logger.error(f"[QtyAdj] FTP listing failed for path '{import_path}': {exc}")
        return 0, 0

    all_files = [
        f for f in all_files
        if f.lower().endswith(".csv") and "processed" not in f.lower()
    ]

    if not all_files:
        logger.info("[QtyAdj] No .csv files found in qty adjustment import path.")
        return 0, 0

    files_processed = 0
    docs_processed = 0

    for filename in all_files:
        files_processed += 1
        file_start = time.monotonic()
        status_str = "failed"
        summary = ""

        try:
            file_bytes = download_excel_file(filename, host, port, user, password, import_path)
        except Exception as exc:
            logger.error(f"[QtyAdj] Download failed for {filename}: {exc}")
            continue

        try:
            from app.services.qty_adjustment_service import process_qty_adjustment_csv
            batch_key = f"{filename}::{now_pkt().strftime('%Y%m%d_%H%M%S')}"
            result = await process_qty_adjustment_csv(file_bytes, source_file=batch_key)
            docs_processed += result.get("total_docs", 0)
            summary = (
                f"QtyAdj {filename}: "
                f"docs={result.get('total_docs', 0)} "
                f"posted={result.get('posted_docs', 0)} "
                f"errors={result.get('error_docs', 0)} "
                f"items={result.get('total_items', 0)}"
            )
            logger.info(summary)
            status_str = "success"
        except Exception as exc:
            summary = f"QtyAdj processing failed for {filename}: {exc}"
            logger.error(summary)

        duration_ms = round((time.monotonic() - file_start) * 1000, 2)
        async with get_session() as session:
            async with session.begin():
                await write_log(
                    session, activity_type="qty_adjustment", status=status_str,
                    details=summary, duration_ms=duration_ms,
                    metadata={"filename": filename},
                )

        try:
            move_ftp_file_to_processed(filename, import_path, processed_path, host, port, user, password)
        except Exception as exc:
            logger.warning(f"[QtyAdj] Could not move {filename} to processed: {exc}")

    return files_processed, docs_processed


async def poll_ftp_and_ingest():
    """
    APScheduler job: poll all configured FTP import paths for new files.

    Routing logic
    ─────────────
    • Item Master import path     → .xlsx / .csv → item_master_service
    • Qty Adjustment import path  → .csv          → qty_adjustment_service
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
                SystemConfig(key="last_ftp_poll_start", value=now_pkt().isoformat())
            )

    new_files     = 0
    docs_inserted = 0
    im_processed  = 0

    # ── Poll Item Master import path ─────────────────────────────────────────
    im_new, im_rows = await _poll_item_master(host, port, user, password)
    new_files    += im_new
    im_processed += im_rows

    # ── Poll Qty Adjustment import path ──────────────────────────────────────
    qa_new, qa_docs = await _poll_qty_adjustment(host, port, user, password)
    new_files    += qa_new
    docs_inserted += qa_docs

    # ── Final summary ────────────────────────────────────────────────────────
    total_duration = (time.monotonic() - job_start) * 1000
    summary = (
        f"Processed {new_files} new files — "
        f"{docs_inserted} QtyAdj docs, {im_processed} Item Master rows processed."
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
                SystemConfig(key="last_ftp_poll_success", value=now_pkt().isoformat())
            )
