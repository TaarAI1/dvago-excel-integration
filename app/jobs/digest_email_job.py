"""
Periodic import-digest email job.

Queries all five import modules for records created since the last digest
was sent, then emails a consolidated summary showing every file processed
and its posted / error counts.

Three independent digest slots are supported, each with its own interval and
recipient list.  A separate duplication-email slot uses the default SMTP
address and CC.  Timestamps are persisted per slot in the ``system_config``
table.
"""
import logging
from datetime import datetime, timedelta

from app.core.timezone import now_pkt

logger = logging.getLogger(__name__)

_DEFAULT_WINDOW_HOURS = 6

# Keys for per-slot interval settings and last-sent timestamps
_SLOT_INTERVAL_KEYS = {
    1: "digest_email_interval_hours",
    2: "digest_email_interval_hours_2",
    3: "digest_email_interval_hours_3",
}
_SLOT_RECIPIENTS_KEYS = {
    1: "digest_email_recipients_1",
    2: "digest_email_recipients_2",
    3: "digest_email_recipients_3",
}
_SLOT_LAST_SENT_KEYS = {
    1: "last_digest_email_sent",
    2: "last_digest_email_sent_2",
    3: "last_digest_email_sent_3",
}
_DUPLICATION_LAST_SENT_KEY = "last_duplication_email_sent"


async def _get_interval_hours(setting_key: str, default: int = _DEFAULT_WINDOW_HOURS) -> int:
    try:
        from app.db.settings_store import get_setting
        val = await get_setting(setting_key)
        return max(1, int(val or default))
    except Exception:
        return default


# ── Timestamp helpers ─────────────────────────────────────────────────────────

async def _get_last_sent(last_sent_key: str, interval_hours: int) -> datetime:
    """Return the last-sent timestamp for a given key, or <interval> hours ago."""
    from app.db.postgres import get_session
    from app.models.system_config import SystemConfig

    async with get_session() as session:
        row = await session.get(SystemConfig, last_sent_key)

    if row and row.value:
        try:
            return datetime.fromisoformat(row.value)
        except ValueError:
            pass
    return now_pkt() - timedelta(hours=interval_hours)


async def _save_last_sent(last_sent_key: str, ts: datetime) -> None:
    from app.db.postgres import get_session
    from app.models.system_config import SystemConfig

    async with get_session() as session:
        async with session.begin():
            await session.merge(SystemConfig(key=last_sent_key, value=ts.isoformat()))


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


# ── Shared data collection ────────────────────────────────────────────────────

async def _collect_digest_data(since: datetime, until: datetime) -> dict[str, list[dict]]:
    from app.models.grn_doc import GRNDoc
    from app.models.transfer_slip_doc import TransferSlipDoc
    from app.models.qty_adjustment_doc import QtyAdjustmentDoc
    from app.models.price_adjustment_doc import PriceAdjustmentDoc

    return {
        "item_master":      await _query_item_master(since, until),
        "grn":              await _query_doc_module(GRNDoc,              since, until),
        "transfer_slip":    await _query_doc_module(TransferSlipDoc,     since, until),
        "qty_adjustment":   await _query_doc_module(QtyAdjustmentDoc,    since, until),
        "price_adjustment": await _query_doc_module(PriceAdjustmentDoc,  since, until),
    }


# ── Digest slot jobs (custom recipients, no default CC) ───────────────────────

async def _run_digest_slot(slot: int) -> None:
    """Core logic for a digest email slot with custom recipients."""
    tag = f"[DigestEmail-{slot}]"
    try:
        interval_key   = _SLOT_INTERVAL_KEYS[slot]
        recipients_key = _SLOT_RECIPIENTS_KEYS[slot]
        last_sent_key  = _SLOT_LAST_SENT_KEYS[slot]

        from app.db.settings_store import get_setting
        recipients_raw = (await get_setting(recipients_key)) or ""
        recipients = [e.strip() for e in recipients_raw.split(",") if e.strip()]
        if not recipients:
            logger.debug("%s No recipients configured — skipping.", tag)
            return

        interval_hours = await _get_interval_hours(interval_key)
        since = await _get_last_sent(last_sent_key, interval_hours)
        until = now_pkt()

        logger.info("%s Collecting activity since %s for %d recipient(s).", tag, since.isoformat(), len(recipients))

        digest_data = await _collect_digest_data(since, until)
        total_files = sum(len(v) for v in digest_data.values())
        logger.info("%s %d file entries found across all modules.", tag, total_files)

        if total_files == 0:
            logger.info("%s Nothing processed in window — skipping email.", tag)
            await _save_last_sent(last_sent_key, until)
            return

        from app.services.email_service import send_digest_email
        await send_digest_email(digest_data, since, until, override_recipients=recipients)

        await _save_last_sent(last_sent_key, until)
        logger.info("%s Digest sent; window updated to %s.", tag, until.isoformat())

    except Exception as exc:
        logger.exception("%s Job failed: %s", tag, exc)


async def send_periodic_digest() -> None:
    """APScheduler job — Digest Email slot 1."""
    await _run_digest_slot(1)


async def send_periodic_digest_2() -> None:
    """APScheduler job — Digest Email slot 2."""
    await _run_digest_slot(2)


async def send_periodic_digest_3() -> None:
    """APScheduler job — Digest Email slot 3."""
    await _run_digest_slot(3)


# ── Duplication email job (default SMTP address + CC) ─────────────────────────

async def send_duplication_email() -> None:
    """
    APScheduler job — Duplication Email.

    Sends the same import-digest to the default SMTP to_email + CC address.
    Unlike the digest slots above, recipients are not customised here;
    the global smtp_to_email and smtp_cc_email settings are used instead.
    """
    tag = "[DuplicationEmail]"
    try:
        from app.db.settings_store import get_setting
        interval_hours = await _get_interval_hours("duplication_email_interval_hours")
        since = await _get_last_sent(_DUPLICATION_LAST_SENT_KEY, interval_hours)
        until = now_pkt()

        logger.info("%s Collecting activity since %s.", tag, since.isoformat())

        digest_data = await _collect_digest_data(since, until)
        total_files = sum(len(v) for v in digest_data.values())
        logger.info("%s %d file entries found across all modules.", tag, total_files)

        if total_files == 0:
            logger.info("%s Nothing processed in window — skipping email.", tag)
            await _save_last_sent(_DUPLICATION_LAST_SENT_KEY, until)
            return

        from app.services.email_service import send_digest_email
        # No override_recipients → uses default smtp_to_email + smtp_cc_email
        await send_digest_email(digest_data, since, until)

        await _save_last_sent(_DUPLICATION_LAST_SENT_KEY, until)
        logger.info("%s Email sent; window updated to %s.", tag, until.isoformat())

    except Exception as exc:
        logger.exception("%s Job failed: %s", tag, exc)
