from fastapi import APIRouter, Depends, BackgroundTasks, Query, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel
from typing import Optional
from sqlalchemy import select, func

from app.core.security import get_current_user
from app.db.postgres import get_session
from app.models.activity_log import ActivityLog, log_to_response
from app.models.sales_export_run import SalesExportRun, SalesExportStore, run_to_response, store_to_response

router = APIRouter(prefix="/api/sales-export", tags=["sales-export"])


class ManualExportRequest(BaseModel):
    store_no: int
    from_date: str  # YYYY-MM-DD
    to_date: str    # YYYY-MM-DD


@router.post("/trigger")
async def trigger_sales_export(
    background_tasks: BackgroundTasks,
    _: str = Depends(get_current_user),
):
    """Manually trigger a sales export run (runs in background)."""
    from app.jobs.sales_export_job import run_sales_export
    background_tasks.add_task(run_sales_export, triggered_by="manual")
    return {"message": "Sales export triggered. Monitor progress via /api/sales-export/progress."}


@router.post("/kill")
async def kill_sales_export(_: str = Depends(get_current_user)):
    """Cancel the currently running export between store iterations."""
    from app.jobs.sales_export_job import request_cancel, get_active_run_id
    active = get_active_run_id()
    if not active:
        return {"cancelled": False, "message": "No export is currently running."}
    request_cancel()
    return {"cancelled": True, "run_id": active, "message": "Cancel signal sent — will stop after current store completes."}


@router.get("/progress")
async def get_export_progress(_: str = Depends(get_current_user)):
    """Return real-time progress of the current export run."""
    from app.jobs.sales_export_job import get_progress, get_active_run_id
    run_id = get_active_run_id()
    if not run_id:
        return {"active": False, "run_id": None}
    progress = get_progress().get(run_id, {})
    return {"active": True, "run_id": run_id, **progress}


@router.get("/runs")
async def list_export_runs(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    _: str = Depends(get_current_user),
):
    """List export runs ordered newest-first — used to populate the batch dropdown."""
    async with get_session() as session:
        total = await session.scalar(select(func.count()).select_from(SalesExportRun))
        result = await session.execute(
            select(SalesExportRun)
            .order_by(SalesExportRun.started_at.desc())
            .offset(offset).limit(limit)
        )
        runs = result.scalars().all()
    return {
        "total": total,
        "items": [run_to_response(r) for r in runs],
    }


@router.get("/runs/{run_id}/stores")
async def get_run_stores(run_id: str, _: str = Depends(get_current_user)):
    """Return all per-store rows for a given run."""
    async with get_session() as session:
        result = await session.execute(
            select(SalesExportStore)
            .where(SalesExportStore.run_id == run_id)
            .order_by(SalesExportStore.store_no)
        )
        stores = result.scalars().all()
    return {"run_id": run_id, "stores": [store_to_response(s) for s in stores]}


@router.get("/last-run")
async def get_last_run(_: str = Depends(get_current_user)):
    """Return the most recent sales_export activity log entry."""
    async with get_session() as session:
        result = await session.execute(
            select(ActivityLog)
            .where(ActivityLog.activity_type == "sales_export")
            .order_by(ActivityLog.timestamp.desc())
            .limit(1)
        )
        log = result.scalar_one_or_none()
    if not log:
        return {"last_run": None}
    return {"last_run": log_to_response(log)}


@router.get("/stores")
async def get_store_list(_: str = Depends(get_current_user)):
    """Fetch active store list from Oracle for the manual export dropdown."""
    from app.jobs.sales_export_job import _load_oracle_settings
    from app.services.oracle_service import run_query

    oc = await _load_oracle_settings()
    if not oc["host"] or not oc["service"]:
        raise HTTPException(status_code=503, detail="Oracle connection not configured.")

    try:
        sbs_df = await run_query(
            oc["host"], oc["port"], oc["service"], oc["user"], oc["pwd"],
            "SELECT sid FROM rps.subsidiary WHERE sbs_no = 1",
        )
        if sbs_df is None or sbs_df.is_empty():
            raise HTTPException(status_code=404, detail="No subsidiary found with sbs_no = 1.")

        sbs_sid = str(sbs_df.rows()[0][0])

        stores_df = await run_query(
            oc["host"], oc["port"], oc["service"], oc["user"], oc["pwd"],
            f"SELECT store_no, store_name FROM rps.store WHERE active = 1 AND sbs_sid = '{sbs_sid}'",
        )
        if stores_df is None or stores_df.is_empty():
            return {"stores": []}

        return {
            "stores": [
                {"store_no": int(r[0]), "store_name": str(r[1]) if r[1] is not None else None}
                for r in stores_df.rows()
            ]
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to fetch stores: {exc}")


@router.post("/manual-download")
async def manual_export_download(
    body: ManualExportRequest,
    _: str = Depends(get_current_user),
):
    """Run the export query for a single store + date range and stream back a CSV file."""
    from app.jobs.sales_export_job import _load_oracle_settings, _inject_store
    from app.services.oracle_service import run_query
    from app.db.settings_store import get_setting
    import logging
    logger = logging.getLogger(__name__)

    oc = await _load_oracle_settings()
    if not oc["host"] or not oc["service"]:
        raise HTTPException(status_code=503, detail="Oracle connection not configured.")

    sql_template = (await get_setting("sales_export_sql", "")) or ""
    if not sql_template:
        raise HTTPException(status_code=503, detail="Export SQL query is not configured.")

    # Inject store filter
    sql = _inject_store(sql_template, body.store_no)

    # Inject date filters (replace {from_date} and {to_date} placeholders)
    if "{from_date}" in sql:
        sql = sql.replace("{from_date}", body.from_date)
    else:
        logger.warning("No {from_date} placeholder in SQL — from_date filter not applied.")

    if "{to_date}" in sql:
        sql = sql.replace("{to_date}", body.to_date)
    else:
        logger.warning("No {to_date} placeholder in SQL — to_date filter not applied.")

    try:
        df = await run_query(oc["host"], oc["port"], oc["service"], oc["user"], oc["pwd"], sql)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Query failed: {exc}")

    if df is None or df.is_empty():
        raise HTTPException(status_code=404, detail="No data found for the selected store and date range.")

    csv_bytes = df.write_csv().encode("utf-8")
    filename = f"manual_export_{body.store_no}_{body.from_date}_{body.to_date}.csv"

    return Response(
        content=csv_bytes,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
