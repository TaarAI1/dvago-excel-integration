import time
import logging
from datetime import datetime

from bson import ObjectId

from app.db.mongodb import get_db
from app.services.retailpro_client import get_client, RetailProError
from app.models.activity_log import write_log
from app.core.config import settings

logger = logging.getLogger(__name__)


async def process_pending_docs():
    """
    APScheduler job: find all unposted, non-errored documents and submit them
    to the RetailPro API sequentially (one at a time).
    """
    db = get_db()
    if db is None:
        logger.error("Database not available, skipping API job.")
        return

    endpoint_map = settings.get_document_type_endpoints()
    client = get_client()

    # Fetch all pending documents (posted=False, has_error=False)
    cursor = db.documents.find({"posted": False, "has_error": False})
    pending_docs = await cursor.to_list(length=None)

    if not pending_docs:
        logger.debug("No pending documents to process.")
        return

    logger.info(f"Processing {len(pending_docs)} pending documents.")
    success_count = 0
    error_count = 0

    for doc in pending_docs:
        doc_id = doc["_id"]
        doc_type = doc.get("document_type", "unknown")
        endpoint = endpoint_map.get(doc_type)

        if not endpoint:
            logger.warning(f"No endpoint configured for document type: {doc_type}")
            await db.documents.update_one(
                {"_id": doc_id},
                {"$set": {
                    "has_error": True,
                    "error_message": f"No API endpoint configured for document_type='{doc_type}'",
                    "updated_at": datetime.utcnow(),
                }},
            )
            await write_log(
                db,
                activity_type="api_call",
                document_id=str(doc_id),
                document_type=doc_type,
                status="failed",
                details=f"No endpoint configured for type '{doc_type}'",
            )
            error_count += 1
            continue

        start = time.monotonic()
        try:
            response = await client.post_document(endpoint, doc.get("original_data", {}))
            duration_ms = round((time.monotonic() - start) * 1000, 2)

            # Extract sid from response["data"][0]["sid"]
            sid = None
            data = response.get("data")
            if data and isinstance(data, list) and len(data) > 0:
                sid = data[0].get("sid")

            now = datetime.utcnow()
            await db.documents.update_one(
                {"_id": doc_id},
                {"$set": {
                    "posted": True,
                    "retailprosid": sid,
                    "posted_at": now,
                    "updated_at": now,
                }},
            )
            await write_log(
                db,
                activity_type="api_call",
                document_id=str(doc_id),
                document_type=doc_type,
                status="success",
                details=f"Posted successfully. sid={sid}",
                duration_ms=duration_ms,
                metadata={"endpoint": endpoint, "sid": sid},
            )
            success_count += 1
            logger.debug(f"Document {doc_id} posted. sid={sid}")

        except RetailProError as exc:
            duration_ms = round((time.monotonic() - start) * 1000, 2)
            error_detail = f"HTTP {exc.status_code}: {exc.response_body}"
            logger.error(f"RetailPro API failed for doc {doc_id}: {error_detail}")

            await db.documents.update_one(
                {"_id": doc_id},
                {"$set": {
                    "has_error": True,
                    "error_message": error_detail,
                    "updated_at": datetime.utcnow(),
                }},
            )
            await write_log(
                db,
                activity_type="api_call",
                document_id=str(doc_id),
                document_type=doc_type,
                status="failed",
                details=error_detail,
                duration_ms=duration_ms,
                metadata={
                    "endpoint": endpoint,
                    "status_code": exc.status_code,
                    "response_body": exc.response_body,
                },
            )
            error_count += 1

        except Exception as exc:
            duration_ms = round((time.monotonic() - start) * 1000, 2)
            logger.error(f"Unexpected error processing doc {doc_id}: {exc}")

            await db.documents.update_one(
                {"_id": doc_id},
                {"$set": {
                    "has_error": True,
                    "error_message": str(exc),
                    "updated_at": datetime.utcnow(),
                }},
            )
            await write_log(
                db,
                activity_type="api_call",
                document_id=str(doc_id),
                document_type=doc_type,
                status="failed",
                details=str(exc),
                duration_ms=duration_ms,
            )
            error_count += 1

    logger.info(f"API job done. success={success_count}, errors={error_count}")
