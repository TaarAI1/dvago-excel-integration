from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional

from app.core.security import get_current_user
from app.scheduler import scheduler, setup_scheduler, get_schedule_status, FTP_JOB_ID
from app.db.mongodb import get_db
from app.core.config import settings

router = APIRouter(prefix="/api/schedule", tags=["schedule"])


class ScheduleConfig(BaseModel):
    cron: str  # e.g. "*/15 * * * *"


@router.get("/status")
async def get_status(_: str = Depends(get_current_user)):
    """Return current scheduler and job status."""
    return get_schedule_status()


@router.post("/configure")
async def configure_schedule(config: ScheduleConfig, _: str = Depends(get_current_user)):
    """Update the FTP poll cron schedule and persist to DB."""
    try:
        setup_scheduler(config.cron)
        if not scheduler.running:
            scheduler.start()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid cron expression: {exc}")

    db = get_db()
    await db.system_config.update_one(
        {"key": "poll_cron_schedule"},
        {"$set": {"key": "poll_cron_schedule", "value": config.cron}},
        upsert=True,
    )
    return {"message": "Schedule updated.", "cron": config.cron}


@router.delete("/pause")
async def pause_schedule(_: str = Depends(get_current_user)):
    """Pause the scheduler (stops all jobs from firing)."""
    if scheduler.running:
        scheduler.pause()
    db = get_db()
    await db.system_config.update_one(
        {"key": "scheduler_paused"},
        {"$set": {"key": "scheduler_paused", "value": True}},
        upsert=True,
    )
    return {"message": "Scheduler paused."}


@router.post("/resume")
async def resume_schedule(_: str = Depends(get_current_user)):
    """Resume a paused scheduler."""
    if scheduler.running:
        scheduler.resume()
    db = get_db()
    await db.system_config.update_one(
        {"key": "scheduler_paused"},
        {"$set": {"key": "scheduler_paused", "value": False}},
        upsert=True,
    )
    return {"message": "Scheduler resumed."}
