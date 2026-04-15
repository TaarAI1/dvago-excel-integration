import time
import json
import logging
from datetime import datetime

from sqlalchemy import select, update

from app.db.postgres import get_session
from app.db.settings_store import get_setting
from app.models.document import Document
from app.models.activity_log import write_log
from app.services.retailpro_client import get_client, RetailProError

logger = logging.getLogger(__name__)


async def process_pending_docs():
    """
    APScheduler job: find all unposted, non-errored documents and submit them
    to the RetailPro API sequentially (one at a time).
    """
    endpoints_raw = await get_setting("document_type_endpoints", "{}")
    try:
        endpoint_map = json.loads(endpoints_raw or "{}")
    except Exception:
        endpoint_map = {}

    client = await get_client()

    async with get_session() as session:
        result = await session.execute(
            select(Document).where(Document.posted == False, Document.has_error == False)
        )
        pending_docs = result.scalars().all()

    if not pending_docs:
        logger.debug("No pending documents to process.")
        if hasattr(client, "close"):
            await client.close()
        return

    logger.info(f"Processing {len(pending_docs)} pending documents.")
    success_count = 0
    error_count = 0

    for doc in pending_docs:
        doc_type = doc.document_type
        endpoint = endpoint_map.get(doc_type)

        if not endpoint:
            async with get_session() as session:
                async with session.begin():
                    await session.execute(
                        update(Document).where(Document.id == doc.id).values(
                            has_error=True,
                            error_message=f"No API endpoint configured for document_type='{doc_type}'",
                            updated_at=datetime.utcnow(),
                        )
                    )
                    await write_log(session, activity_type="api_call", document_id=str(doc.id),
                                    document_type=doc_type, status="failed",
                                    details=f"No endpoint configured for type '{doc_type}'")
            error_count += 1
            continue

        start = time.monotonic()
        try:
            response = await client.post_document(endpoint, doc.original_data or {})
            duration_ms = round((time.monotonic() - start) * 1000, 2)

            sid = None
            data = response.get("data")
            if data and isinstance(data, list) and len(data) > 0:
                sid = data[0].get("sid")

            now = datetime.utcnow()
            async with get_session() as session:
                async with session.begin():
                    await session.execute(
                        update(Document).where(Document.id == doc.id).values(
                            posted=True,
                            retailprosid=sid,
                            posted_at=now,
                            updated_at=now,
                        )
                    )
                    await write_log(session, activity_type="api_call", document_id=str(doc.id),
                                    document_type=doc_type, status="success",
                                    details=f"Posted successfully. sid={sid}",
                                    duration_ms=duration_ms,
                                    metadata={"endpoint": endpoint, "sid": sid})
            success_count += 1

        except RetailProError as exc:
            duration_ms = round((time.monotonic() - start) * 1000, 2)
            error_detail = f"HTTP {exc.status_code}: {exc.response_body}"
            logger.error(f"RetailPro API failed for doc {doc.id}: {error_detail}")

            async with get_session() as session:
                async with session.begin():
                    await session.execute(
                        update(Document).where(Document.id == doc.id).values(
                            has_error=True,
                            error_message=error_detail,
                            updated_at=datetime.utcnow(),
                        )
                    )
                    await write_log(session, activity_type="api_call", document_id=str(doc.id),
                                    document_type=doc_type, status="failed",
                                    details=error_detail, duration_ms=duration_ms,
                                    metadata={"endpoint": endpoint,
                                              "status_code": exc.status_code,
                                              "response_body": exc.response_body})
            error_count += 1

        except Exception as exc:
            duration_ms = round((time.monotonic() - start) * 1000, 2)
            logger.error(f"Unexpected error processing doc {doc.id}: {exc}")

            async with get_session() as session:
                async with session.begin():
                    await session.execute(
                        update(Document).where(Document.id == doc.id).values(
                            has_error=True,
                            error_message=str(exc),
                            updated_at=datetime.utcnow(),
                        )
                    )
                    await write_log(session, activity_type="api_call", document_id=str(doc.id),
                                    document_type=doc_type, status="failed",
                                    details=str(exc), duration_ms=duration_ms)
            error_count += 1

    if hasattr(client, "close"):
        await client.close()

    logger.info(f"API job done. success={success_count}, errors={error_count}")
