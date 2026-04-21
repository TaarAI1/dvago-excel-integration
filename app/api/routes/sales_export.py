from fastapi import APIRouter, Depends, BackgroundTasks, Query
from typing import Optional
from sqlalchemy import select, func

from app.core.security import get_current_user
from app.db.postgres import get_session
from app.models.activity_log import ActivityLog, log_to_response
from app.models.sales_export_run import SalesExportRun, SalesExportStore, run_to_response, store_to_response

router = APIRouter(prefix="/api/sales-export", tags=["sales-export"])


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
