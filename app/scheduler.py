from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
import logging

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()

FTP_JOB_ID              = "ftp_poll_job"
SALES_EXPORT_JOB_ID     = "sales_export_job"
SALES_EXPORT_JOB_ID2    = "sales_export_job_2"
DIGEST_EMAIL_JOB_ID     = "digest_email_job"
DIGEST_EMAIL_JOB_ID_2   = "digest_email_job_2"
DIGEST_EMAIL_JOB_ID_3   = "digest_email_job_3"
DUPLICATION_EMAIL_JOB_ID = "duplication_email_job"

_ALL_MANAGED_JOB_IDS = (
    FTP_JOB_ID,
    SALES_EXPORT_JOB_ID,
    SALES_EXPORT_JOB_ID2,
    DIGEST_EMAIL_JOB_ID,
    DIGEST_EMAIL_JOB_ID_2,
    DIGEST_EMAIL_JOB_ID_3,
    DUPLICATION_EMAIL_JOB_ID,
)


def setup_scheduler(
    poll_cron: str,
    sales_export_cron: str = "0 2 * * *",
    sales_export_cron_2: str = "",
    digest_interval_hours: int = 6,
    digest_interval_hours_2: int = 0,
    digest_interval_hours_3: int = 0,
    duplication_interval_hours: int = 0,
):
    from app.jobs.ftp_job import poll_ftp_and_ingest
    from app.jobs.sales_export_job import run_sales_export
    from app.jobs.digest_email_job import (
        send_periodic_digest,
        send_periodic_digest_2,
        send_periodic_digest_3,
        send_duplication_email,
    )

    digest_hours = max(1, int(digest_interval_hours))

    for job_id in _ALL_MANAGED_JOB_IDS:
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

    # Digest slot 1 — always scheduled (minimum 1 hour)
    scheduler.add_job(
        send_periodic_digest,
        trigger=IntervalTrigger(hours=digest_hours),
        id=DIGEST_EMAIL_JOB_ID,
        name=f"Digest Email 1 (every {digest_hours}h)",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
    )

    # Digest slot 2 — only when interval is configured
    if digest_interval_hours_2 and int(digest_interval_hours_2) >= 1:
        h2 = max(1, int(digest_interval_hours_2))
        scheduler.add_job(
            send_periodic_digest_2,
            trigger=IntervalTrigger(hours=h2),
            id=DIGEST_EMAIL_JOB_ID_2,
            name=f"Digest Email 2 (every {h2}h)",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=300,
        )

    # Digest slot 3 — only when interval is configured
    if digest_interval_hours_3 and int(digest_interval_hours_3) >= 1:
        h3 = max(1, int(digest_interval_hours_3))
        scheduler.add_job(
            send_periodic_digest_3,
            trigger=IntervalTrigger(hours=h3),
            id=DIGEST_EMAIL_JOB_ID_3,
            name=f"Digest Email 3 (every {h3}h)",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=300,
        )

    # Duplication email — only when interval is configured
    if duplication_interval_hours and int(duplication_interval_hours) >= 1:
        hd = max(1, int(duplication_interval_hours))
        scheduler.add_job(
            send_duplication_email,
            trigger=IntervalTrigger(hours=hd),
            id=DUPLICATION_EMAIL_JOB_ID,
            name=f"Duplication Email (every {hd}h)",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=300,
        )

    logger.info(
        "Scheduler jobs registered. FTP: %s | Sales: %s%s | Digest: %dh / %s / %s | Duplication: %s",
        poll_cron,
        sales_export_cron,
        f" + {sales_export_cron_2}" if sales_export_cron_2 and sales_export_cron_2.strip() else "",
        digest_hours,
        f"{digest_interval_hours_2}h" if digest_interval_hours_2 else "off",
        f"{digest_interval_hours_3}h" if digest_interval_hours_3 else "off",
        f"{duplication_interval_hours}h" if duplication_interval_hours else "off",
    )


def get_schedule_status() -> dict:
    ftp_job          = scheduler.get_job(FTP_JOB_ID)
    sales_job        = scheduler.get_job(SALES_EXPORT_JOB_ID)
    sales_job2       = scheduler.get_job(SALES_EXPORT_JOB_ID2)
    digest_job       = scheduler.get_job(DIGEST_EMAIL_JOB_ID)
    digest_job2      = scheduler.get_job(DIGEST_EMAIL_JOB_ID_2)
    digest_job3      = scheduler.get_job(DIGEST_EMAIL_JOB_ID_3)
    duplication_job  = scheduler.get_job(DUPLICATION_EMAIL_JOB_ID)

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
        "running":              scheduler.running,
        "ftp_job":              job_info(ftp_job),
        "sales_export_job":     job_info(sales_job),
        "sales_export_job_2":   job_info(sales_job2),
        "digest_email_job":     job_info(digest_job),
        "digest_email_job_2":   job_info(digest_job2),
        "digest_email_job_3":   job_info(digest_job3),
        "duplication_email_job": job_info(duplication_job),
    }
