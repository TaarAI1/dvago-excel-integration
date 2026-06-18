"""
Periodic import-digest email job.

Queries all five import modules for records created since the last digest
was sent, then emails a consolidated summary showing every file processed
and its posted / error counts.

The interval is configurable via the ``digest_email_interval_hours``
application setting (default 6 hours).  The last-run timestamp is persisted
in the ``system_config`` table under ``"last_digest_email_sent"``.
"""
import logging
from datetime import datetime, timedelta

from app.core.timezone import now_pkt

logger = logging.getLogger(__name__)

_LAST_SENT_KEY = "last_digest_email_sent"
_DEFAULT_WINDOW_HOURS = 6


async def _get_digest_interval_hours() -> int:
    """Read configured interval from settings, fallback to 6."""
    try:
        from app.db.settings_store import get_setting
        val = await get_setting("digest_email_interval_hours")
        return max(1, int(val or _DEFAULT_WINDOW_HOURS))
    except Exception:
        return _DEFAULT_WINDOW_HOURS


# ── Timestamp helpers ─────────────────────────────────────────────────────────

async def _get_last_sent() -> datetime:
    """Return the last-sent timestamp, or <interval> hours ago if never sent."""
    from app.db.postgres import get_session
    from app.models.system_config import SystemConfig

    async with get_session() as session:
        row = await session.get(SystemConfig, _LAST_SENT_KEY)

    if row and row.value:
        try:
            return datetime.fromisoformat(row.value)
        except ValueError:
            pass
    window = await _get_digest_interval_hours()
    return now_pkt() - timedelta(hours=window)


async def _save_last_sent(ts: datetime) -> None:
    from app.db.postgres import get_session
    from app.models.system_config import SystemConfig

    async with get_session() as session:
        async with session.begin():
            await session.merge(SystemConfig(key=_LAST_SENT_KEY, value=ts.isoformat()))


# ── Per-module data collectors ────────────────────────────────────────────────

async def _query_item_master(since: datetime, until: datetime) -> list[dict]:
    """Group item_master Documents by source_file within the time window."""
    from sqlalchemy import select, func, case
    from app.db.postgres import get_session
    from app.models.document import Document

    async with get_session() as session:
        stmt = (
            select(
                Document.source_file,
                func.count().label("total"),
                func.sum(case((Document.posted == True, 1), else_=0)).label("posted"),
                func.sum(case((Document.has_error == True, 1), else_=0)).label("errors"),
                func.min(Document.created_at).label("first_at"),
                func.max(Document.created_at).label("last_at"),
            )
            .where(
                Document.document_type == "item_master",
                Document.created_at >= since,
                Document.created_at < until,
            )
            .group_by(Document.source_file)
            .order_by(func.min(Document.created_at).asc())
        )
        result = await session.execute(stmt)
        rows = result.all()

    return [
        {
            "source_file": r.source_file or "—",
            "total":       int(r.total  or 0),
            "posted":      int(r.posted or 0),
            "errors":      int(r.errors or 0),
            "first_at":    r.first_at,
            "last_at":     r.last_at,
        }
        for r in rows
    ]


async def _query_doc_module(model_class, since: datetime, until: datetime) -> list[dict]:
    """
    Generic aggregation for GRN / TransferSlip / QtyAdj / PriceAdj.

    Returns one entry per source_file with document-level and item-level totals.
    """
    from sqlalchemy import select, func, case
    from app.db.postgres import get_session

    async with get_session() as session:
        stmt = (
            select(
                model_class.source_file,
                func.count().label("docs"),
                func.sum(
                    case((model_class.status == "posted",  1), else_=0)
                ).label("posted_docs"),
                func.sum(
                    case((model_class.status == "error",   1), else_=0)
                ).label("error_docs"),
                func.sum(
                    case((model_class.status == "partial", 1), else_=0)
                ).label("partial_docs"),
                func.sum(model_class.item_count).label("total_items"),
                func.sum(model_class.posted_count).label("posted_items"),
                func.sum(model_class.error_count).label("error_items"),
                func.min(model_class.created_at).label("first_at"),
                func.max(model_class.created_at).label("last_at"),
            )
            .where(
                model_class.created_at >= since,
                model_class.created_at < until,
            )
            .group_by(model_class.source_file)
            .order_by(func.min(model_class.created_at).asc())
        )
        result = await session.execute(stmt)
        rows = result.all()

    return [
        {
            "source_file":  r.source_file   or "—",
            "docs":         int(r.docs          or 0),
            "posted_docs":  int(r.posted_docs   or 0),
            "error_docs":   int(r.error_docs    or 0),
            "partial_docs": int(r.partial_docs  or 0),
            "total_items":  int(r.total_items   or 0),
            "posted_items": int(r.posted_items  or 0),
            "error_items":  int(r.error_items   or 0),
            "first_at":     r.first_at,
            "last_at":      r.last_at,
        }
        for r in rows
    ]


# ── Job entry point ───────────────────────────────────────────────────────────

async def send_periodic_digest() -> None:
    """
    APScheduler job: collect all import activity since the last digest
    and send a consolidated summary email.

    Safe to call manually.  Swallows all exceptions so it never crashes
    the scheduler loop.
    """
    try:
        since = await _get_last_sent()
        until = now_pkt()

        logger.info("[DigestEmail] Collecting activity since %s", since.isoformat())

        from app.models.grn_doc import GRNDoc
        from app.models.transfer_slip_doc import TransferSlipDoc
        from app.models.qty_adjustment_doc import QtyAdjustmentDoc
        from app.models.price_adjustment_doc import PriceAdjustmentDoc

        digest_data: dict[str, list[dict]] = {
            "item_master":      await _query_item_master(since, until),
            "grn":              await _query_doc_module(GRNDoc,              since, until),
            "transfer_slip":    await _query_doc_module(TransferSlipDoc,     since, until),
            "qty_adjustment":   await _query_doc_module(QtyAdjustmentDoc,    since, until),
            "price_adjustment": await _query_doc_module(PriceAdjustmentDoc,  since, until),
        }

        total_files = sum(len(v) for v in digest_data.values())
        logger.info("[DigestEmail] %d file entries found across all modules.", total_files)

        if total_files == 0:
            logger.info("[DigestEmail] Nothing processed in window — skipping email.")
            await _save_last_sent(until)
            return

        from app.services.email_service import send_digest_email
        await send_digest_email(digest_data, since, until)

        await _save_last_sent(until)
        logger.info("[DigestEmail] Digest sent successfully; window updated to %s.", until.isoformat())

    except Exception as exc:
        logger.exception("[DigestEmail] Job failed: %s", exc)
