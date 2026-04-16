from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import logging

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()

FTP_JOB_ID = "ftp_poll_job"
SALES_EXPORT_JOB_ID = "sales_export_job"


def setup_scheduler(poll_cron: str, sales_export_cron: str = "0 2 * * *"):
    from app.jobs.ftp_job import poll_ftp_and_ingest
    from app.jobs.sales_export_job import run_sales_export

    for job_id in (FTP_JOB_ID, SALES_EXPORT_JOB_ID):
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
        run_sales_export,
        trigger=CronTrigger.from_crontab(sales_export_cron),
        id=SALES_EXPORT_JOB_ID,
        name="Sales Export",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=120,
    )
    logger.info(f"Scheduler jobs registered. FTP cron: {poll_cron}, Sales cron: {sales_export_cron}")


def get_schedule_status() -> dict:
    ftp_job   = scheduler.get_job(FTP_JOB_ID)
    sales_job = scheduler.get_job(SALES_EXPORT_JOB_ID)

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
        "sales_export_job": job_info(sales_job),
    }
