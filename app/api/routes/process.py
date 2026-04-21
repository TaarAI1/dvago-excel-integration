from fastapi import APIRouter, Depends, BackgroundTasks

from app.core.security import get_current_user
from app.jobs.ftp_job import poll_ftp_and_ingest

router = APIRouter(prefix="/api/process", tags=["process"])


@router.post("/trigger")
async def manual_trigger(
    background_tasks: BackgroundTasks,
    _: str = Depends(get_current_user),
):
    """Manually trigger an FTP poll + Item Master processing run."""
    background_tasks.add_task(poll_ftp_and_ingest)
    return {"message": "FTP poll triggered. Check activity logs for progress."}
