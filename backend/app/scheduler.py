from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
import logging

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()

FTP_JOB_ID = "ftp_poll_job"
API_JOB_ID = "api_process_job"


def setup_scheduler(poll_cron: str):
    """Register jobs. Called once during FastAPI lifespan startup."""
    from app.jobs.ftp_job import poll_ftp_and_ingest
    from app.jobs.api_job import process_pending_docs

    # Remove existing jobs if reconfiguring
    for job_id in (FTP_JOB_ID, API_JOB_ID):
        if scheduler.get_job(job_id):
            scheduler.remove_job(job_id)

    scheduler.add_job(
        poll_ftp_and_ingest,
        trigger=CronTrigger.from_crontab(poll_cron),
        id=FTP_JOB_ID,
        name="FTP Poll & Ingest",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=60,
    )

    scheduler.add_job(
        process_pending_docs,
        trigger=IntervalTrigger(minutes=1),
        id=API_JOB_ID,
        name="Process Pending Documents",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=30,
    )
    logger.info(f"Scheduler jobs registered. FTP cron: {poll_cron}")


def get_schedule_status() -> dict:
    ftp_job = scheduler.get_job(FTP_JOB_ID)
    api_job = scheduler.get_job(API_JOB_ID)

    def job_info(job):
        if not job:
            return None
        next_run = job.next_run_time
        return {
            "id": job.id,
            "name": job.name,
            "next_run": next_run.isoformat() if next_run else None,
            "pending": job.pending,
        }

    return {
        "running": scheduler.running,
        "ftp_job": job_info(ftp_job),
        "api_job": job_info(api_job),
    }
