from fastapi import APIRouter, Depends, BackgroundTasks
from sqlalchemy import select

from app.core.security import get_current_user
from app.db.postgres import get_session
from app.models.activity_log import ActivityLog, log_to_response
from app.jobs.sales_export_job import run_sales_export

router = APIRouter(prefix="/api/sales-export", tags=["sales-export"])


@router.post("/trigger")
async def trigger_sales_export(
    background_tasks: BackgroundTasks,
    _: str = Depends(get_current_user),
):
    """Manually trigger a sales export run."""
    background_tasks.add_task(run_sales_export)
    return {"message": "Sales export triggered. Check activity logs for progress."}


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
