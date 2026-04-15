from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from app.core.security import get_current_user
from app.scheduler import scheduler, setup_scheduler, get_schedule_status
from app.db.postgres import get_session
from app.models.system_config import SystemConfig

router = APIRouter(prefix="/api/schedule", tags=["schedule"])


class ScheduleConfig(BaseModel):
    cron: str


@router.get("/status")
async def get_status(_: str = Depends(get_current_user)):
    return get_schedule_status()


@router.post("/configure")
async def configure_schedule(config: ScheduleConfig, _: str = Depends(get_current_user)):
    try:
        setup_scheduler(config.cron)
        if not scheduler.running:
            scheduler.start()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid cron expression: {exc}")

    async with get_session() as session:
        async with session.begin():
            await session.merge(SystemConfig(key="poll_cron_schedule", value=config.cron))

    return {"message": "Schedule updated.", "cron": config.cron}


@router.delete("/pause")
async def pause_schedule(_: str = Depends(get_current_user)):
    if scheduler.running:
        scheduler.pause()
    async with get_session() as session:
        async with session.begin():
            await session.merge(SystemConfig(key="scheduler_paused", value="true"))
    return {"message": "Scheduler paused."}


@router.post("/resume")
async def resume_schedule(_: str = Depends(get_current_user)):
    if scheduler.running:
        scheduler.resume()
    async with get_session() as session:
        async with session.begin():
            await session.merge(SystemConfig(key="scheduler_paused", value="false"))
    return {"message": "Scheduler resumed."}
