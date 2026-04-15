import os
import time
import logging
from datetime import datetime

from app.db.mongodb import get_db
from app.services.ftp_service import list_csv_files, download_csv_file
from app.services.csv_processor import parse_csv, infer_document_type
from app.models.activity_log import write_log
from app.core.config import settings

logger = logging.getLogger(__name__)


async def poll_ftp_and_ingest():
    """
    APScheduler job: poll FTP for new CSV files, parse and insert into MongoDB.
    Skips files that have already been processed (tracked in ftp_seen_files collection).
    """
    db = get_db()
    if db is None:
        logger.error("Database not available, skipping FTP job.")
        return

    job_start = time.monotonic()
    logger.info("FTP poll job started.")

    # Update last poll timestamp in system_config
    await db.system_config.update_one(
        {"key": "last_ftp_poll_start"},
        {"$set": {"key": "last_ftp_poll_start", "value": datetime.utcnow().isoformat()}},
        upsert=True,
    )

    try:
        csv_files = list_csv_files()
    except Exception as exc:
        logger.error(f"FTP listing failed: {exc}")
        await write_log(
            db,
            activity_type="ftp_poll",
            status="failed",
            details=f"FTP listing error: {exc}",
        )
        return

    if not csv_files:
        logger.info("No CSV files found on FTP.")
        await write_log(db, activity_type="ftp_poll", status="success", details="No CSV files found.")
        return

    field_maps = settings.get_document_type_field_maps()
    new_files_count = 0
    total_docs_inserted = 0

    for filename in csv_files:
        # Check if already processed
        seen = await db.ftp_seen_files.find_one({"filename": filename})
        if seen:
            logger.debug(f"Skipping already-processed file: {filename}")
            continue

        new_files_count += 1
        tmp_path = None
        file_start = time.monotonic()

        try:
            tmp_path = download_csv_file(filename)
        except Exception as exc:
            logger.error(f"FTP download failed for {filename}: {exc}")
            await write_log(
                db,
                activity_type="ftp_poll",
                status="failed",
                details=f"Download error: {exc}",
                metadata={"filename": filename},
            )
            continue

        doc_type = infer_document_type(filename)
        field_map = field_maps.get(doc_type, {})

        try:
            records = parse_csv(tmp_path, doc_type, field_map)
        except Exception as exc:
            logger.error(f"CSV parse failed for {filename}: {exc}")
            await write_log(
                db,
                activity_type="csv_parse",
                status="failed",
                details=f"Parse error: {exc}",
                metadata={"filename": filename, "document_type": doc_type},
            )
            continue
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)

        if not records:
            logger.warning(f"No rows parsed from {filename}")
            await db.ftp_seen_files.insert_one({"filename": filename, "processed_at": datetime.utcnow()})
            continue

        # Build documents for MongoDB
        now = datetime.utcnow()
        documents = [
            {
                "document_type": doc_type,
                "original_data": row,
                "retailprosid": None,
                "posted": False,
                "has_error": False,
                "error_message": None,
                "created_at": now,
                "updated_at": now,
                "posted_at": None,
                "source_file": filename,
            }
            for row in records
        ]

        try:
            result = await db.documents.insert_many(documents, ordered=False)
            inserted = len(result.inserted_ids)
            total_docs_inserted += inserted

            file_duration = (time.monotonic() - file_start) * 1000
            await write_log(
                db,
                activity_type="csv_parse",
                status="success",
                details=f"Inserted {inserted} documents from {filename}",
                duration_ms=round(file_duration, 2),
                metadata={"filename": filename, "document_type": doc_type, "row_count": inserted},
            )
            # Mark file as seen
            await db.ftp_seen_files.insert_one({"filename": filename, "processed_at": now})
            logger.info(f"Inserted {inserted} docs from {filename}")

        except Exception as exc:
            logger.error(f"MongoDB insert failed for {filename}: {exc}")
            await write_log(
                db,
                activity_type="csv_parse",
                status="failed",
                details=f"DB insert error: {exc}",
                metadata={"filename": filename},
            )

    total_duration = (time.monotonic() - job_start) * 1000
    summary = f"Processed {new_files_count} new files, {total_docs_inserted} documents inserted."
    logger.info(f"FTP poll job done. {summary}")

    await write_log(
        db,
        activity_type="ftp_poll",
        status="success",
        details=summary,
        duration_ms=round(total_duration, 2),
        metadata={"new_files": new_files_count, "docs_inserted": total_docs_inserted},
    )

    await db.system_config.update_one(
        {"key": "last_ftp_poll_success"},
        {"$set": {"key": "last_ftp_poll_success", "value": datetime.utcnow().isoformat()}},
        upsert=True,
    )
