"""
Sales Export Job: Query Oracle DB per store → CSV per store → Upload to FTP.

Per-run tracking
────────────────
Each execution (scheduler or manual) creates a SalesExportRun row and one
SalesExportStore row per store.  An in-memory cancellation flag allows the
/api/sales-export/kill endpoint to abort between store iterations.
"""
import asyncio
import io
import logging
import re
import time
import uuid as _uuid
from datetime import datetime

from app.core.timezone import now_pkt
from app.db.postgres import get_session
from app.db.settings_store import get_setting
from app.models.activity_log import write_log
from app.models.sales_export_run import SalesExportRun, SalesExportStore
from app.services.oracle_service import run_query
from app.services.ftp_service import upload_file

logger = logging.getLogger(__name__)

# ── In-memory state shared with the route layer ───────────────────────────────

# run_id of the currently executing export (None when idle)
_active_run_id: str | None = None

# Set of run_ids that have been asked to cancel
_cancel_requests: set[str] = set()

# Live progress: {run_id: {total, done, current_store, status}}
_progress: dict = {}


def get_active_run_id() -> str | None:
    return _active_run_id


def get_progress() -> dict:
    return dict(_progress)


def request_cancel(run_id: str | None = None) -> bool:
    """
    Signal the running export to stop after the current store finishes.
    If run_id is None, cancels whatever is currently active.
    Returns True if there was an active run to cancel.
    """
    global _active_run_id
    target = run_id or _active_run_id
    if not target:
        return False
    _cancel_requests.add(target)
    logger.info("Cancel requested for export run %s", target)
    return True


def _is_cancelled(run_id: str) -> bool:
    return run_id in _cancel_requests


# ── SQL helpers ───────────────────────────────────────────────────────────────

_STORE_FILTER_RE = re.compile(
    r'AND\s+[sS]\s*\.\s*STORE_no\s*=\s*\d+\s*--\s*Store\s*Filter',
    re.IGNORECASE,
)


def _inject_store(sql: str, store_no: int) -> str:
    if '{store_no}' in sql:
        return sql.replace('{store_no}', str(store_no))
    if _STORE_FILTER_RE.search(sql):
        return _STORE_FILTER_RE.sub(f'AND s.STORE_no = {store_no}  --Store Filter', sql)
    logger.warning("No store placeholder found in SQL — running without store filter (store=%s).", store_no)
    return sql


async def _load_oracle_settings() -> dict:
    host    = await get_setting("sales_oracle_host", "")    or await get_setting("oracle_host", "")
    port    = int(await get_setting("sales_oracle_port", "1521") or "1521")
    service = await get_setting("sales_oracle_service_name", "") or await get_setting("oracle_service_name", "")
    user    = await get_setting("sales_oracle_username", "")    or await get_setting("oracle_username", "")
    pwd     = (await get_setting("sales_oracle_password", "") or await get_setting("oracle_password", "") or "")
    return {"host": host, "port": port, "service": service, "user": user, "pwd": pwd}


# ── DB helpers ────────────────────────────────────────────────────────────────

async def _save_run(run: SalesExportRun) -> None:
    async with get_session() as session:
        async with session.begin():
            await session.merge(run)


async def _save_store(store: SalesExportStore) -> None:
    async with get_session() as session:
        async with session.begin():
            session.add(store)


async def _update_run(run_id: str, **kwargs) -> None:
    async with get_session() as session:
        async with session.begin():
            run = await session.get(SalesExportRun, run_id)
            if run:
                for k, v in kwargs.items():
                    setattr(run, k, v)


# ── Main job ──────────────────────────────────────────────────────────────────

async def run_sales_export(triggered_by: str = "scheduler") -> dict:
    """
    APScheduler job (also callable from the manual trigger endpoint).
    Returns a summary dict.
    """
    global _active_run_id, _progress

    run_id     = str(_uuid.uuid4())
    started_at = now_pkt()
    label      = f"sales-export {started_at.strftime('%Y-%m-%d %H:%M:%S')}"
    job_start  = time.monotonic()

    _active_run_id = run_id
    _progress[run_id] = {"total": 0, "done": 0, "current_store": None, "status": "starting"}

    logger.info("Sales export job started. run_id=%s triggered_by=%s", run_id, triggered_by)

    oc           = await _load_oracle_settings()
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
        _active_run_id = None
        _progress.pop(run_id, None)
        async with get_session() as session:
            async with session.begin():
                await write_log(session, activity_type="sales_export", status="skipped", details=msg)
        return {"run_id": run_id, "status": "skipped", "message": msg}

    # ── Create run record ────────────────────────────────────────────────────
    run = SalesExportRun(
        run_id=run_id, label=label,
        triggered_by=triggered_by,
        status="running",
        started_at=started_at,
    )
    try:
        await _save_run(run)
    except Exception as exc:
        logger.error("Could not save SalesExportRun: %s", exc)

    # ── Fetch store list ────────────────────────────────────────────────────
    try:
        stores_df = await run_query(
            oc["host"], oc["port"], oc["service"], oc["user"], oc["pwd"],
            "SELECT store_no, store_name FROM rps.store WHERE active = 1 AND sbs_no = 1",
        )
        if stores_df is None or stores_df.is_empty():
            msg = "Sales export skipped: no active stores found in rps.store."
            logger.warning(msg)
            await _update_run(run_id, status="skipped", finished_at=now_pkt())
            _active_run_id = None
            _progress.pop(run_id, None)
            async with get_session() as session:
                async with session.begin():
                    await write_log(session, activity_type="sales_export", status="skipped", details=msg)
            return {"run_id": run_id, "status": "skipped"}
        store_list: list[tuple[int, str | None]] = [
            (int(row[0]), str(row[1]) if row[1] is not None else None)
            for row in stores_df.rows()
        ]
    except Exception as exc:
        msg = f"Sales export: failed to fetch store list: {exc}"
        logger.error(msg)
        await _update_run(run_id, status="failed", finished_at=now_pkt(), error_message=str(exc))
        _active_run_id = None
        _progress.pop(run_id, None)
        async with get_session() as session:
            async with session.begin():
                await write_log(session, activity_type="sales_export", status="failed", details=msg)
        return {"run_id": run_id, "status": "failed", "error": msg}

    total_stores = len(store_list)
    await _update_run(run_id, total_stores=total_stores)
    _progress[run_id] = {"total": total_stores, "done": 0,
                         "current_store": None, "status": "running"}

    # ── Per-store loop ───────────────────────────────────────────────────────
    timestamp = started_at.strftime('%Y%m%d_%H%M%S')
    results: list[dict] = []

    for idx, (store_no, store_name) in enumerate(store_list):
        # Check cancellation before each store
        if _is_cancelled(run_id):
            logger.info("Export run %s cancelled at store %s.", run_id, store_no)
            await _update_run(run_id, status="cancelled",
                              processed_stores=idx, finished_at=now_pkt())
            _active_run_id = None
            _cancel_requests.discard(run_id)
            _progress[run_id] = {**_progress.get(run_id, {}), "status": "cancelled"}
            async with get_session() as session:
                async with session.begin():
                    await write_log(session, activity_type="sales_export", status="cancelled",
                                    details=f"Sales export run {run_id} cancelled after {idx}/{total_stores} stores.")
            return {"run_id": run_id, "status": "cancelled", "processed": idx, "total": total_stores}

        _progress[run_id]["current_store"] = store_no
        _progress[run_id]["done"] = idx

        store_start = time.monotonic()
        filename    = f"{prefix}_{store_no}_{timestamp}.csv"
        store_sql   = _inject_store(sql_template, store_no)

        store_row = SalesExportStore(
            id=str(_uuid.uuid4()),
            run_id=run_id,
            store_no=store_no,
            store_name=store_name,
            filename=filename,
            ftp_path=export_path,
            status="processing",
        )

        # ── Oracle query ────────────────────────────────────────────────────
        try:
            df = await run_query(
                oc["host"], oc["port"], oc["service"], oc["user"], oc["pwd"],
                store_sql,
            )
            query_rows = len(df) if df is not None else 0
        except Exception as exc:
            msg = f"Sales Export Store {store_no}: query failed — {exc}"
            logger.error(msg)
            duration_ms = round((time.monotonic() - store_start) * 1000, 2)
            store_row.status = "failed"
            store_row.error_message = str(exc)
            store_row.duration_ms = duration_ms
            try:
                await _save_store(store_row)
            except Exception as dbe:
                logger.error("DB save failed for store %s: %s", store_no, dbe)
            async with get_session() as session:
                async with session.begin():
                    await write_log(session, activity_type="sales_export", status="failed",
                                    details=msg, duration_ms=duration_ms,
                                    metadata={"run_id": run_id, "store_no": store_no, "error": str(exc)})
            await _update_run(run_id, processed_stores=idx + 1)
            results.append({"store_no": store_no, "status": "failed", "error": str(exc)})
            continue

        # ── No data ─────────────────────────────────────────────────────────
        if df is None or df.is_empty():
            msg = f"Sales Export Store {store_no}: no data returned by query — skipped"
            logger.info(msg)
            duration_ms = round((time.monotonic() - store_start) * 1000, 2)
            store_row.status = "skipped"
            store_row.query_rows = 0
            store_row.written_rows = 0
            store_row.duration_ms = duration_ms
            store_row.error_message = "No data returned — file not created"
            try:
                await _save_store(store_row)
            except Exception as dbe:
                logger.error("DB save failed for store %s: %s", store_no, dbe)
            async with get_session() as session:
                async with session.begin():
                    await write_log(session, activity_type="sales_export", status="skipped",
                                    details=msg, duration_ms=duration_ms,
                                    metadata={"run_id": run_id, "store_no": store_no, "rows": 0})
            await _update_run(run_id, processed_stores=idx + 1)
            results.append({"store_no": store_no, "status": "skipped", "rows": 0})
            continue

        # ── CSV generation ───────────────────────────────────────────────────
        try:
            csv_bytes = df.write_csv().encode("utf-8")
            written_rows = max(0, csv_bytes.count(b'\n') - 1)
        except Exception as exc:
            msg = f"Sales Export Store {store_no}: CSV generation failed — {exc}"
            logger.error(msg)
            duration_ms = round((time.monotonic() - store_start) * 1000, 2)
            store_row.status = "failed"
            store_row.query_rows = query_rows
            store_row.error_message = str(exc)
            store_row.duration_ms = duration_ms
            try:
                await _save_store(store_row)
            except Exception as dbe:
                logger.error("DB save failed for store %s: %s", store_no, dbe)
            async with get_session() as session:
                async with session.begin():
                    await write_log(session, activity_type="sales_export", status="failed",
                                    details=msg, duration_ms=duration_ms,
                                    metadata={"run_id": run_id, "store_no": store_no})
            await _update_run(run_id, processed_stores=idx + 1)
            results.append({"store_no": store_no, "status": "failed", "error": str(exc)})
            continue

        # ── FTP upload ───────────────────────────────────────────────────────
        try:
            await asyncio.to_thread(
                upload_file, csv_bytes, filename,
                ftp_host, ftp_port, ftp_user, ftp_password, export_path,
            )
            msg = (
                f"Sales Export Store {store_no}: query returned {query_rows} rows — "
                f"wrote {written_rows} rows to CSV — uploaded as {filename}"
            )
            logger.info(msg)
            duration_ms = round((time.monotonic() - store_start) * 1000, 2)
            store_row.status = "success"
            store_row.query_rows = query_rows
            store_row.written_rows = written_rows
            store_row.duration_ms = duration_ms
            try:
                await _save_store(store_row)
            except Exception as dbe:
                logger.error("DB save failed for store %s: %s", store_no, dbe)
            async with get_session() as session:
                async with session.begin():
                    await write_log(session, activity_type="sales_export", status="success",
                                    details=msg, duration_ms=duration_ms,
                                    metadata={"run_id": run_id, "store_no": store_no,
                                              "query_rows": query_rows, "written_rows": written_rows,
                                              "filename": filename, "ftp_path": export_path})
            results.append({"store_no": store_no, "status": "success",
                             "query_rows": query_rows, "written_rows": written_rows,
                             "filename": filename})
        except Exception as exc:
            msg = f"Sales Export Store {store_no}: FTP upload failed — {exc}"
            logger.error(msg)
            duration_ms = round((time.monotonic() - store_start) * 1000, 2)
            store_row.status = "failed"
            store_row.query_rows = query_rows
            store_row.written_rows = written_rows
            store_row.error_message = str(exc)
            store_row.duration_ms = duration_ms
            try:
                await _save_store(store_row)
            except Exception as dbe:
                logger.error("DB save failed for store %s: %s", store_no, dbe)
            async with get_session() as session:
                async with session.begin():
                    await write_log(session, activity_type="sales_export", status="failed",
                                    details=msg, duration_ms=duration_ms,
                                    metadata={"run_id": run_id, "store_no": store_no, "error": str(exc)})
            results.append({"store_no": store_no, "status": "failed", "error": str(exc)})

        await _update_run(run_id, processed_stores=idx + 1)
        _progress[run_id]["done"] = idx + 1

    # ── Finalize run ─────────────────────────────────────────────────────────
    ok_count   = sum(1 for r in results if r["status"] == "success")
    skip_count = sum(1 for r in results if r["status"] == "skipped")
    fail_count = sum(1 for r in results if r["status"] == "failed")
    total_rows = sum(r.get("written_rows", 0) for r in results)
    duration_ms = round((time.monotonic() - job_start) * 1000, 2)

    overall = "success" if fail_count == 0 else ("partial" if ok_count > 0 else "failed")
    summary = (
        f"Sales export complete — {total_stores} stores: "
        f"{ok_count} uploaded, {skip_count} empty, {fail_count} failed, "
        f"{total_rows} total rows written."
    )
    logger.info(summary)

    await _update_run(run_id,
        status=overall, processed_stores=total_stores, finished_at=now_pkt()
    )
    _progress[run_id] = {"total": total_stores, "done": total_stores,
                         "current_store": None, "status": overall}

    async with get_session() as session:
        async with session.begin():
            await write_log(
                session, activity_type="sales_export", status=overall,
                details=summary, duration_ms=duration_ms,
                metadata={
                    "run_id": run_id, "summary": True,
                    "stores": total_stores, "uploaded": ok_count,
                    "skipped": skip_count, "failed": fail_count, "total_rows": total_rows,
                },
            )

    _active_run_id = None
    return {"run_id": run_id, "status": overall, "total_stores": total_stores,
            "uploaded": ok_count, "skipped": skip_count, "failed": fail_count}
