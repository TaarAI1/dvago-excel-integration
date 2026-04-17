"""
Sales Export Job: Query Oracle DB → Polars DataFrame → CSV → Upload to FTP.
"""
import asyncio
import io
import logging
import time

from app.core.timezone import now_pkt
from app.db.postgres import get_session
from app.db.settings_store import get_setting
from app.models.activity_log import write_log
from app.services.oracle_service import run_query
from app.services.ftp_service import upload_file

logger = logging.getLogger(__name__)


async def run_sales_export():
    """
    APScheduler job: run Oracle SQL query, export to CSV, upload to FTP.
    """
    job_start = time.monotonic()
    logger.info("Sales export job started.")

    # Load all required settings
    oracle_host = await get_setting("oracle_host", "")
    oracle_port = int(await get_setting("oracle_port", "1521") or "1521")
    oracle_service = await get_setting("oracle_service_name", "")
    oracle_user = await get_setting("oracle_username", "")
    oracle_password = await get_setting("oracle_password", "") or ""
    sql = await get_setting("sales_export_sql", "")
    ftp_host = await get_setting("ftp_host", "localhost")
    ftp_port = int(await get_setting("ftp_port", "21") or "21")
    ftp_user = await get_setting("ftp_user", "anonymous")
    ftp_password = await get_setting("ftp_password", "") or ""
    export_path = await get_setting("ftp_export_path", "/exports") or "/exports"
    prefix = await get_setting("sales_export_filename_prefix", "sales_export") or "sales_export"

    if not oracle_host or not oracle_service or not sql:
        msg = "Sales export skipped: Oracle host, service name, or SQL query not configured."
        logger.warning(msg)
        async with get_session() as session:
            async with session.begin():
                await write_log(session, activity_type="sales_export", status="skipped", details=msg)
        return

    # Run Oracle query
    try:
        df = await run_query(oracle_host, oracle_port, oracle_service, oracle_user, oracle_password, sql)
        row_count = len(df)
        logger.info(f"Oracle query returned {row_count} rows.")
    except Exception as exc:
        logger.error(f"Oracle query failed: {exc}")
        async with get_session() as session:
            async with session.begin():
                await write_log(session, activity_type="sales_export", status="failed",
                                details=f"Oracle query error: {exc}")
        return

    # Convert to CSV bytes
    try:
        csv_bytes = df.write_csv().encode("utf-8")
    except Exception as exc:
        logger.error(f"CSV generation failed: {exc}")
        async with get_session() as session:
            async with session.begin():
                await write_log(session, activity_type="sales_export", status="failed",
                                details=f"CSV generation error: {exc}")
        return

    # Upload to FTP
    filename = f"{prefix}_{now_pkt().strftime('%Y%m%d_%H%M%S')}.csv"
    try:
        await asyncio.to_thread(
            upload_file, csv_bytes, filename, ftp_host, ftp_port, ftp_user, ftp_password, export_path
        )
        logger.info(f"Uploaded {filename} to FTP {export_path}")
    except Exception as exc:
        logger.error(f"FTP upload failed: {exc}")
        async with get_session() as session:
            async with session.begin():
                await write_log(session, activity_type="sales_export", status="failed",
                                details=f"FTP upload error: {exc}",
                                metadata={"filename": filename, "row_count": row_count})
        return

    duration_ms = round((time.monotonic() - job_start) * 1000, 2)
    summary = f"Exported {row_count} rows to {filename}"
    logger.info(f"Sales export done. {summary}")

    async with get_session() as session:
        async with session.begin():
            await write_log(session, activity_type="sales_export", status="success",
                            details=summary, duration_ms=duration_ms,
                            metadata={"filename": filename, "row_count": row_count, "ftp_path": export_path})
