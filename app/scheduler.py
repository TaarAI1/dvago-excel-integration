from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
import logging

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()

FTP_JOB_ID           = "ftp_poll_job"
SALES_EXPORT_JOB_ID  = "sales_export_job"
SALES_EXPORT_JOB_ID2 = "sales_export_job_2"
DIGEST_EMAIL_JOB_ID  = "digest_email_job"


def setup_scheduler(
    poll_cron: str,
    sales_export_cron: str = "0 2 * * *",
    sales_export_cron_2: str = "",
    digest_interval_hours: int = 6,
):
    from app.jobs.ftp_job import poll_ftp_and_ingest
    from app.jobs.sales_export_job import run_sales_export
    from app.jobs.digest_email_job import send_periodic_digest

    digest_hours = max(1, int(digest_interval_hours))

    # Remove all existing managed jobs before re-adding
    for job_id in (FTP_JOB_ID, SALES_EXPORT_JOB_ID, SALES_EXPORT_JOB_ID2, DIGEST_EMAIL_JOB_ID):
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
        name="Sales Export (Time 1)",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=120,
    )

    if sales_export_cron_2 and sales_export_cron_2.strip():
        scheduler.add_job(
            run_sales_export,
            trigger=CronTrigger.from_crontab(sales_export_cron_2.strip()),
            id=SALES_EXPORT_JOB_ID2,
            name="Sales Export (Time 2)",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=120,
        )

    scheduler.add_job(
        send_periodic_digest,
        trigger=IntervalTrigger(hours=digest_hours),
        id=DIGEST_EMAIL_JOB_ID,
        name=f"Import Digest Email (every {digest_hours}h)",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
    )

    if sales_export_cron_2 and sales_export_cron_2.strip():
        logger.info(
            "Scheduler jobs registered. FTP cron: %s, Sales cron 1: %s, Sales cron 2: %s, Digest: every %dh",
            poll_cron, sales_export_cron, sales_export_cron_2, digest_hours,
        )
    else:
        logger.info(
            "Scheduler jobs registered. FTP cron: %s, Sales cron: %s, Digest: every %dh",
            poll_cron, sales_export_cron, digest_hours,
        )


def get_schedule_status() -> dict:
    ftp_job     = scheduler.get_job(FTP_JOB_ID)
    sales_job   = scheduler.get_job(SALES_EXPORT_JOB_ID)
    sales_job2  = scheduler.get_job(SALES_EXPORT_JOB_ID2)
    digest_job  = scheduler.get_job(DIGEST_EMAIL_JOB_ID)

    def job_info(job):
        if not job:
            return None
        next_run = job.next_run_time
        return {
            "id":       job.id,
            "name":     job.name,
            "next_run": next_run.isoformat() if next_run else None,
            "pending":  job.pending,
        }

    return {
        "running":            scheduler.running,
        "ftp_job":            job_info(ftp_job),
        "sales_export_job":   job_info(sales_job),
        "sales_export_job_2": job_info(sales_job2),
        "digest_email_job":   job_info(digest_job),
    }
