"""
Sales Export Job: Query Oracle DB per store → CSV per store → Upload to FTP.

Pipeline per run
────────────────
1. Run  SELECT DISTINCT store_no FROM rps.store  → list of store numbers
2. For each store_no:
   a. Substitute {store_no} placeholder (or --Store Filter comment) in the
      SQL template loaded from settings.
   b. Execute the query against the Sales Export Oracle connection.
   c. Write results to a CSV file named  <prefix>_<storeno>_YYYYMMDD_HHMMSS.csv
   d. Upload the CSV to the configured FTP export path.
3. Log a single activity_log entry with summary of all stores.
"""
import asyncio
import io
import logging
import re
import time

from app.core.timezone import now_pkt
from app.db.postgres import get_session
from app.db.settings_store import get_setting
from app.models.activity_log import write_log
from app.services.oracle_service import run_query
from app.services.ftp_service import upload_file

logger = logging.getLogger(__name__)

# Regex that matches the store-filter line added by the user, e.g.:
#   AND s.STORE_no = 6  --Store Filter
_STORE_FILTER_RE = re.compile(
    r'AND\s+[sS]\s*\.\s*STORE_no\s*=\s*\d+\s*--\s*Store\s*Filter',
    re.IGNORECASE,
)


def _inject_store(sql: str, store_no: int) -> str:
    """
    Substitute the store_no into the SQL template.

    Priority:
      1. If the template contains '{store_no}' → Python str.format substitution.
      2. If it contains a  --Store Filter  comment → regex replacement.
      3. Otherwise → return the SQL unchanged (caller logs a warning).
    """
    if '{store_no}' in sql:
        return sql.replace('{store_no}', str(store_no))

    if _STORE_FILTER_RE.search(sql):
        return _STORE_FILTER_RE.sub(
            f'AND s.STORE_no = {store_no}  --Store Filter',
            sql,
        )

    logger.warning(
        "sales_export_sql has no {store_no} placeholder or --Store Filter comment. "
        "Running query without store substitution (store=%s).", store_no
    )
    return sql


async def _load_oracle_settings() -> dict:
    """
    Load the Sales Export Oracle connection — dedicated settings first,
    fall back to the shared Oracle DB settings.
    """
    host    = await get_setting("sales_oracle_host", "")    or await get_setting("oracle_host", "")
    port    = int(await get_setting("sales_oracle_port", "1521") or "1521")
    service = await get_setting("sales_oracle_service_name", "") or await get_setting("oracle_service_name", "")
    user    = await get_setting("sales_oracle_username", "")    or await get_setting("oracle_username", "")
    pwd     = (await get_setting("sales_oracle_password", "") or await get_setting("oracle_password", "") or "")
    return {"host": host, "port": port, "service": service, "user": user, "pwd": pwd}


async def run_sales_export():
    """
    APScheduler job (also callable from the manual trigger endpoint):
    Export sales data per store to FTP.
    """
    job_start = time.monotonic()
    logger.info("Sales export job started.")

    oc     = await _load_oracle_settings()
    sql_template = (await get_setting("sales_export_sql", "")) or ""
    ftp_host     = await get_setting("ftp_host",     "localhost")
    ftp_port     = int(await get_setting("ftp_port", "21") or "21")
    ftp_user     = await get_setting("ftp_user",     "anonymous")
    ftp_password = (await get_setting("ftp_password", "") or "")
    export_path  = (await get_setting("ftp_export_path", "/exports") or "/exports")
    prefix       = (await get_setting("sales_export_filename_prefix", "sales_export") or "sales_export")

    if not oc["host"] or not oc["service"] or not sql_template:
        msg = "Sales export skipped: Oracle host, service name, or SQL query not configured."
        logger.warning(msg)
        async with get_session() as session:
            async with session.begin():
                await write_log(session, activity_type="sales_export", status="skipped", details=msg)
        return

    # ── Step 1: Get distinct store numbers ───────────────────────────────────
    try:
        stores_df = await run_query(
            oc["host"], oc["port"], oc["service"], oc["user"], oc["pwd"],
            "SELECT DISTINCT store_no FROM rps.store ORDER BY store_no",
        )
        if stores_df is None or stores_df.is_empty():
            msg = "Sales export skipped: no stores found in rps.store."
            logger.warning(msg)
            async with get_session() as session:
                async with session.begin():
                    await write_log(session, activity_type="sales_export", status="skipped", details=msg)
            return
        store_numbers: list[int] = [int(row[0]) for row in stores_df.rows()]
        logger.info("Sales export: found %d stores → %s", len(store_numbers), store_numbers)
    except Exception as exc:
        logger.error("Sales export: failed to fetch store list: %s", exc)
        async with get_session() as session:
            async with session.begin():
                await write_log(session, activity_type="sales_export", status="failed",
                                details=f"Store list query failed: {exc}")
        return

    # ── Step 2: Per-store export (one activity log entry per store) ───────────
    timestamp  = now_pkt().strftime('%Y%m%d_%H%M%S')
    results    = []   # [(store_no, filename, row_count, status, error)]

    for store_no in store_numbers:
        store_start = time.monotonic()
        store_sql   = _inject_store(sql_template, store_no)
        filename    = f"{prefix}_{store_no}_{timestamp}.csv"

        # ── Run Oracle query ─────────────────────────────────────────────────
        try:
            df = await run_query(
                oc["host"], oc["port"], oc["service"], oc["user"], oc["pwd"],
                store_sql,
            )
            row_count = len(df) if df is not None else 0
        except Exception as exc:
            msg = f"Sales Export Store {store_no}: query failed — {exc}"
            logger.error(msg)
            store_ms = round((time.monotonic() - store_start) * 1000, 2)
            async with get_session() as session:
                async with session.begin():
                    await write_log(session, activity_type="sales_export", status="failed",
                                    details=msg, duration_ms=store_ms,
                                    metadata={"store_no": store_no, "error": str(exc)})
            results.append((store_no, filename, 0, "failed", str(exc)))
            continue

        # ── No data ──────────────────────────────────────────────────────────
        if df is None or df.is_empty():
            msg = f"Sales Export Store {store_no}: no data returned by query — skipped"
            logger.info(msg)
            store_ms = round((time.monotonic() - store_start) * 1000, 2)
            async with get_session() as session:
                async with session.begin():
                    await write_log(session, activity_type="sales_export", status="skipped",
                                    details=msg, duration_ms=store_ms,
                                    metadata={"store_no": store_no, "rows": 0})
            results.append((store_no, filename, 0, "skipped", "no rows"))
            continue

        # ── Convert to CSV ───────────────────────────────────────────────────
        try:
            csv_bytes = df.write_csv().encode("utf-8")
        except Exception as exc:
            msg = f"Sales Export Store {store_no}: CSV generation failed — {exc}"
            logger.error(msg)
            store_ms = round((time.monotonic() - store_start) * 1000, 2)
            async with get_session() as session:
                async with session.begin():
                    await write_log(session, activity_type="sales_export", status="failed",
                                    details=msg, duration_ms=store_ms,
                                    metadata={"store_no": store_no, "rows": row_count, "error": str(exc)})
            results.append((store_no, filename, row_count, "failed", str(exc)))
            continue

        # ── Upload to FTP ────────────────────────────────────────────────────
        try:
            await asyncio.to_thread(
                upload_file, csv_bytes, filename,
                ftp_host, ftp_port, ftp_user, ftp_password, export_path,
            )
            msg = (
                f"Sales Export Store {store_no}: query returned {row_count} rows — "
                f"wrote {row_count} rows to CSV — uploaded as {filename}"
            )
            logger.info(msg)
            store_ms = round((time.monotonic() - store_start) * 1000, 2)
            async with get_session() as session:
                async with session.begin():
                    await write_log(session, activity_type="sales_export", status="success",
                                    details=msg, duration_ms=store_ms,
                                    metadata={"store_no": store_no, "rows": row_count,
                                              "filename": filename, "ftp_path": export_path})
            results.append((store_no, filename, row_count, "success", None))
        except Exception as exc:
            msg = f"Sales Export Store {store_no}: FTP upload failed — {exc}"
            logger.error(msg)
            store_ms = round((time.monotonic() - store_start) * 1000, 2)
            async with get_session() as session:
                async with session.begin():
                    await write_log(session, activity_type="sales_export", status="failed",
                                    details=msg, duration_ms=store_ms,
                                    metadata={"store_no": store_no, "rows": row_count,
                                              "filename": filename, "error": str(exc)})
            results.append((store_no, filename, row_count, "failed", str(exc)))

    # ── Step 3: Overall summary log ───────────────────────────────────────────
    ok_count   = sum(1 for r in results if r[3] == "success")
    skip_count = sum(1 for r in results if r[3] == "skipped")
    fail_count = sum(1 for r in results if r[3] == "failed")
    total_rows = sum(r[2] for r in results)
    duration_ms = round((time.monotonic() - job_start) * 1000, 2)

    summary = (
        f"Sales export complete — {len(store_numbers)} stores: "
        f"{ok_count} uploaded, {skip_count} empty, {fail_count} failed, "
        f"{total_rows} total rows."
    )
    logger.info(summary)

    overall_status = "success" if fail_count == 0 else ("partial" if ok_count > 0 else "failed")

    async with get_session() as session:
        async with session.begin():
            await write_log(
                session, activity_type="sales_export",
                status=overall_status,
                details=summary,
                duration_ms=duration_ms,
                metadata={
                    "summary": True,
                    "stores": len(store_numbers),
                    "uploaded": ok_count,
                    "skipped": skip_count,
                    "failed": fail_count,
                    "total_rows": total_rows,
                },
            )
