"""
Duplication check job.

Runs on a configurable interval (duplication_email_interval_hours).
For each supported import module, cross-checks posted records between
the application database (PostgreSQL) and RetailPro (Oracle) to surface:

  - Duplicate SIDs            — same SID posted more than once on either side
  - Missing in App            — SID exists in RetailPro but not in the app DB
  - Missing in RetailPro      — SID exists in the app DB but not in RetailPro
  - Item count mismatch       — SID present on both sides but item counts differ

A separate email is sent per module when at least one issue is found.
Only records marked as "posted" in both systems are examined.

Modules covered:
  - Quantity Adjustment   Oracle table: rps.adjustment (adj_type=0, status=4)
                          Items:        rps.adj_item.adj_sid
  - Price Adjustment      Oracle table: rps.adjustment (adj_type=1, status=4)
                          Items:        rps.adj_item.adj_sid
  - Transfer Slip         Oracle table: rps.slip (status=4)
                          Items:        rps.slip_item.slip_sid
  - GRN                   Oracle table: rps.voucher (status=4)
                          Items:        rps.vou_item.vou_sid
"""
import importlib
import logging
from collections import Counter
from datetime import datetime, timedelta
from typing import Any

from app.core.timezone import now_pkt

logger = logging.getLogger(__name__)

_LAST_SENT_KEY = "last_duplication_email_sent"
_DEFAULT_INTERVAL_HOURS = 24


# ─────────────────────────────────────────────────────────────────────────────
# Module definitions
# Each entry drives the generic check logic — no per-module code branching.
# ─────────────────────────────────────────────────────────────────────────────

_MODULES: list[dict] = [
    {
        "name":                 "qty_adjustment",
        "model":                "app.models.qty_adjustment_doc.QtyAdjustmentDoc",
        "pg_sid_field":         "adj_sid",          # column name in the PG model
        "oracle_sid_table":     "rps.adjustment",
        "oracle_sid_filter":    "adj_type = 0 AND status = 4",
        "oracle_item_table":    "rps.adj_item",
        "oracle_item_sid_col":  "adj_sid",          # FK column in the item table
    },
    {
        "name":                 "price_adjustment",
        "model":                "app.models.price_adjustment_doc.PriceAdjustmentDoc",
        "pg_sid_field":         "adj_sid",
        "oracle_sid_table":     "rps.adjustment",
        "oracle_sid_filter":    "adj_type = 1 AND status = 4",
        "oracle_item_table":    "rps.adj_item",
        "oracle_item_sid_col":  "adj_sid",
    },
    {
        "name":                 "transfer_slip",
        "model":                "app.models.transfer_slip_doc.TransferSlipDoc",
        "pg_sid_field":         "slip_sid",
        "oracle_sid_table":     "rps.slip",
        "oracle_sid_filter":    "status = 4",
        "oracle_item_table":    "rps.slip_item",
        "oracle_item_sid_col":  "slip_sid",
    },
    {
        "name":                 "grn",
        "model":                "app.models.grn_doc.GRNDoc",
        "pg_sid_field":         "vousid",
        "oracle_sid_table":     "rps.voucher",
        "oracle_sid_filter":    "status = 4",
        "oracle_item_table":    "rps.vou_item",
        "oracle_item_sid_col":  "vou_sid",
    },
]

_MODULE_LABELS: dict[str, str] = {
    "qty_adjustment":   "Quantity Adjustment",
    "price_adjustment": "Price Adjustment",
    "transfer_slip":    "Transfer Slip",
    "grn":              "GRN (Goods Received Note)",
}

_MODULE_ACCENT: dict[str, str] = {
    "qty_adjustment":   "#059669",
    "price_adjustment": "#d97706",
    "transfer_slip":    "#0891b2",
    "grn":              "#7c3aed",
}


# ─────────────────────────────────────────────────────────────────────────────
# Settings & timestamp helpers
# ─────────────────────────────────────────────────────────────────────────────

async def _get_interval_hours() -> int:
    try:
        from app.db.settings_store import get_setting
        val = await get_setting("duplication_email_interval_hours")
        return max(1, int(val or _DEFAULT_INTERVAL_HOURS))
    except Exception:
        return _DEFAULT_INTERVAL_HOURS


async def _get_last_sent(interval_hours: int) -> datetime:
    from app.db.postgres import get_session
    from app.models.system_config import SystemConfig

    async with get_session() as session:
        row = await session.get(SystemConfig, _LAST_SENT_KEY)

    if row and row.value:
        try:
            return datetime.fromisoformat(row.value)
        except ValueError:
            pass
    return now_pkt() - timedelta(hours=interval_hours)


async def _save_last_sent(ts: datetime) -> None:
    from app.db.postgres import get_session
    from app.models.system_config import SystemConfig

    async with get_session() as session:
        async with session.begin():
            await session.merge(SystemConfig(key=_LAST_SENT_KEY, value=ts.isoformat()))


# ─────────────────────────────────────────────────────────────────────────────
# Oracle helpers
# ─────────────────────────────────────────────────────────────────────────────

async def _load_oracle_settings() -> dict | None:
    from app.db.settings_store import get_setting
    host = await get_setting("oracle_host", "")
    if not host:
        return None
    return {
        "host":         host,
        "port":         int((await get_setting("oracle_port", "1521")) or "1521"),
        "service_name": (await get_setting("oracle_service_name", "")) or "",
        "username":     (await get_setting("oracle_username", "")) or "",
        "password":     (await get_setting("oracle_password", "")) or "",
    }


def _safe_sid(sid: str) -> str:
    """Strip characters that could cause SQL injection from a SID value."""
    return "".join(c for c in sid if c.isalnum() or c in ("-", "_", "."))


async def _oracle_fetch_sids(
    oracle: dict,
    table: str,
    where_filter: str,
    since: datetime,
    until: datetime,
) -> list[str]:
    """
    Return all SIDs from *table* matching *where_filter* and the time window.
    Uses TO_CHAR(sid) so the value arrives as a plain string matching what
    PostgreSQL stores in the adj_sid / slip_sid / vousid text columns.
    Builds:
      SELECT TO_CHAR(sid) FROM <table>
      WHERE <where_filter>
        AND created_Datetime >= TO_DATE(...)
        AND created_Datetime <= TO_DATE(...)
    """
    since_str = since.strftime("%Y-%m-%d %H:%M:%S")
    until_str = until.strftime("%Y-%m-%d %H:%M:%S")
    sql = (
        f"SELECT TO_CHAR(sid) AS sid FROM {table}"
        f" WHERE {where_filter}"
        f"   AND created_Datetime >= TO_DATE('{since_str}', 'YYYY-MM-DD HH24:MI:SS')"
        f"   AND created_Datetime <= TO_DATE('{until_str}', 'YYYY-MM-DD HH24:MI:SS')"
    )
    from app.services.oracle_service import run_query
    try:
        df = await run_query(
            oracle["host"], oracle["port"], oracle["service_name"],
            oracle["username"], oracle["password"], sql,
        )
        if df.is_empty():
            return []
        col = df.columns[0]
        return [str(v) for v in df[col].to_list() if v is not None]
    except Exception as exc:
        logger.warning("[DupCheck] Oracle SID query failed (%s): %s", table, exc)
        return []


async def _oracle_item_counts_batch(
    oracle: dict,
    item_table: str,
    item_sid_col: str,
    sids: list[str],
) -> dict[str, int]:
    """
    Fetch item counts for a batch of SIDs in a single Oracle query.
    Builds:
      SELECT <item_sid_col>, COUNT(sid) AS cnt
      FROM <item_table>
      WHERE <item_sid_col> IN (...)
      GROUP BY <item_sid_col>
    Returns {sid: count}. Missing SIDs implicitly have count 0.
    """
    if not sids:
        return {}
    safe_sids = [_safe_sid(s) for s in sids if s]
    if not safe_sids:
        return {}
    in_clause = ", ".join(f"'{s}'" for s in safe_sids)
    sql = (
        f"SELECT {item_sid_col}, COUNT(sid) AS cnt"
        f" FROM {item_table}"
        f" WHERE {item_sid_col} IN ({in_clause})"
        f" GROUP BY {item_sid_col}"
    )
    from app.services.oracle_service import run_query
    try:
        df = await run_query(
            oracle["host"], oracle["port"], oracle["service_name"],
            oracle["username"], oracle["password"], sql,
        )
        if df.is_empty():
            return {}
        result: dict[str, int] = {}
        # oracledb returns column names in upper-case
        col_sid = item_sid_col.upper()
        col_cnt = "CNT"
        for row in df.iter_rows(named=True):
            sid_val = str(row.get(col_sid) or row.get(item_sid_col) or "")
            cnt_val = int(row.get(col_cnt) or row.get("cnt") or 0)
            if sid_val:
                result[sid_val] = cnt_val
        return result
    except Exception as exc:
        logger.warning("[DupCheck] Oracle item-count batch failed (%s): %s", item_table, exc)
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# PostgreSQL helpers
# ─────────────────────────────────────────────────────────────────────────────

async def _pg_posted_docs(
    model_class: Any,
    pg_sid_field: str,
    since: datetime,
    until: datetime,
) -> list[tuple[str, int]]:
    """
    Return [(sid, item_count)] for all posted docs in the window.
    Uses *pg_sid_field* to read the correct SID column for each model.
    The same SID may appear more than once if imported multiple times.
    """
    from sqlalchemy import select
    from app.db.postgres import get_session

    sid_col = getattr(model_class, pg_sid_field)

    async with get_session() as session:
        stmt = (
            select(sid_col, model_class.item_count)
            .where(
                model_class.status == "posted",
                model_class.created_at >= since,
                model_class.created_at < until,
                sid_col.isnot(None),
            )
        )
        result = await session.execute(stmt)
        rows = result.all()

    return [(str(r[0]), int(r[1] or 0)) for r in rows]


# ─────────────────────────────────────────────────────────────────────────────
# Core comparison logic
# ─────────────────────────────────────────────────────────────────────────────

async def _check_module(mod: dict, oracle: dict, since: datetime, until: datetime) -> list[dict]:
    """
    Run the full duplication check for one module config dict.
    Returns a list of issue dicts, one per problematic SID.
    """
    name = mod["name"]
    tag  = f"[DupCheck:{name}]"

    # Lazy-import model class
    module_path, class_name = mod["model"].rsplit(".", 1)
    model_class = getattr(importlib.import_module(module_path), class_name)

    # ── 1. PostgreSQL posted docs ─────────────────────────────────────────────
    pg_rows = await _pg_posted_docs(model_class, mod["pg_sid_field"], since, until)
    pg_counter: Counter = Counter(sid for sid, _ in pg_rows)
    pg_item_counts: dict[str, int] = {}
    for sid, item_count in pg_rows:
        pg_item_counts[sid] = max(pg_item_counts.get(sid, 0), item_count)

    logger.info("%s PG: %d posted docs, %d unique SIDs.", tag, len(pg_rows), len(pg_counter))

    # ── 2. Oracle SIDs ────────────────────────────────────────────────────────
    oracle_sids = await _oracle_fetch_sids(
        oracle, mod["oracle_sid_table"], mod["oracle_sid_filter"], since, until
    )
    oracle_counter: Counter = Counter(oracle_sids)
    logger.info("%s Oracle: %d SIDs, %d unique.", tag, len(oracle_sids), len(oracle_counter))

    all_sids = set(pg_counter.keys()) | set(oracle_counter.keys())
    if not all_sids:
        logger.info("%s No records in either system — skipping.", tag)
        return []

    # ── 3. Batch item counts for SIDs present on BOTH sides ───────────────────
    both_sids = sorted(set(pg_counter.keys()) & set(oracle_counter.keys()))
    oracle_item_counts = await _oracle_item_counts_batch(
        oracle, mod["oracle_item_table"], mod["oracle_item_sid_col"], both_sids
    )

    # ── 4. Build issue list ───────────────────────────────────────────────────
    issues: list[dict] = []

    for sid in sorted(all_sids):
        pg_cnt     = pg_counter.get(sid, 0)
        oracle_cnt = oracle_counter.get(sid, 0)
        pg_items   = pg_item_counts.get(sid, 0)
        oracle_items: int | None = None
        flags: list[str] = []

        if pg_cnt == 0:
            flags.append("missing_in_app")
        elif oracle_cnt == 0:
            flags.append("missing_in_retailpro")
        else:
            if pg_cnt > 1:
                flags.append("duplicate_in_app")
            if oracle_cnt > 1:
                flags.append("duplicate_in_retailpro")

            oracle_items = oracle_item_counts.get(sid)
            if oracle_items is not None and pg_items != oracle_items:
                flags.append("item_count_mismatch")

        if flags:
            issues.append({
                "sid":          sid,
                "flags":        flags,
                "pg_count":     pg_cnt,
                "oracle_count": oracle_cnt,
                "pg_items":     pg_items,
                "oracle_items": oracle_items,
            })

    logger.info("%s %d issue(s) found.", tag, len(issues))
    return issues


# ─────────────────────────────────────────────────────────────────────────────
# HTML email builder — one email per module
# ─────────────────────────────────────────────────────────────────────────────

_FLAG_LABELS: dict[str, tuple[str, str]] = {
    "missing_in_app":           ("Missing in App",          "#dc2626"),
    "missing_in_retailpro":     ("Missing in RetailPro",    "#ea580c"),
    "duplicate_in_app":         ("Duplicate in App",        "#7c3aed"),
    "duplicate_in_retailpro":   ("Duplicate in RetailPro",  "#7c3aed"),
    "item_count_mismatch":      ("Item Count Mismatch",     "#d97706"),
}


def _build_module_html(
    module: str,
    issues: list[dict],
    since: datetime,
    until: datetime,
) -> str:
    """Build the full HTML email for a single module."""
    label      = _MODULE_LABELS.get(module, module)
    accent     = _MODULE_ACCENT.get(module, "#1a56db")
    since_str  = since.strftime("%d %b %Y %H:%M")
    until_str  = until.strftime("%d %b %Y %H:%M")

    legend_html = "".join(
        f'<span style="display:inline-flex;align-items:center;margin-right:14px;font-size:11px;color:#374151;">'
        f'<span style="display:inline-block;width:9px;height:9px;border-radius:50%;'
        f'background:{color};margin-right:4px;"></span>{lbl}</span>'
        for lbl, color in _FLAG_LABELS.values()
    )

    # ── No issues: green all-clear body ──────────────────────────────────────
    if not issues:
        body_html = (
            f'<tr><td style="padding:24px 32px;">'
            f'<div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:8px;padding:20px 24px;">'
            f'<p style="margin:0;font-size:15px;color:#15803d;font-weight:600;">'
            f'&#10003;&nbsp; No Duplications Found</p>'
            f'<p style="margin:8px 0 0;font-size:13px;color:#166534;">'
            f'All records for this module match correctly between the Application DB and RetailPro. '
            f'No missing, duplicate, or item-count issues were detected.</p>'
            f'</div>'
            f'</td></tr>'
        )
        status_badge = (
            '<span style="display:inline-block;background:#d1fae5;color:#065f46;'
            'padding:3px 12px;border-radius:9999px;font-size:12px;font-weight:700;">'
            '&#10003; All Clear</span>'
        )
        status_line = "No issues found — all records match between App and RetailPro."
    else:
        # ── Issues table ──────────────────────────────────────────────────────
        rows_html = ""
        for issue in issues:
            sid          = issue["sid"]
            flags        = issue["flags"]
            pg_cnt       = issue["pg_count"]
            oracle_cnt   = issue["oracle_count"]
            pg_items     = issue["pg_items"]
            oracle_items = issue.get("oracle_items")
            oracle_items_str = str(oracle_items) if oracle_items is not None else "—"

            badges = "".join(
                f'<span style="display:inline-block;background:{color};color:#fff;'
                f'padding:2px 8px;border-radius:9999px;font-size:11px;margin-right:4px;">'
                f'{lbl}</span>'
                for f in flags
                for lbl, color in [_FLAG_LABELS.get(f, (f, "#6b7280"))]
            )

            if "missing_in_app" in flags or "missing_in_retailpro" in flags:
                row_bg = "#fef2f2"
            elif "duplicate_in_app" in flags or "duplicate_in_retailpro" in flags:
                row_bg = "#f5f3ff"
            elif "item_count_mismatch" in flags:
                row_bg = "#fff7ed"
            else:
                row_bg = "#fff"

            rows_html += (
                f'<tr style="background:{row_bg};border-bottom:1px solid #e5e7eb;">'
                f'<td style="padding:8px 12px;font-family:monospace;font-size:12px;color:#111827;">{sid}</td>'
                f'<td style="padding:8px 12px;">{badges}</td>'
                f'<td style="padding:8px 12px;text-align:center;font-size:13px;">{pg_cnt}</td>'
                f'<td style="padding:8px 12px;text-align:center;font-size:13px;">{oracle_cnt}</td>'
                f'<td style="padding:8px 12px;text-align:center;font-size:13px;">{pg_items}</td>'
                f'<td style="padding:8px 12px;text-align:center;font-size:13px;">{oracle_items_str}</td>'
                f'</tr>'
            )

        body_html = (
            f'<tr><td style="padding:12px 32px 0;">'
            f'<div style="padding:4px 0;">{legend_html}</div>'
            f'</td></tr>'
            f'<tr><td style="padding:16px 32px 32px;">'
            f'<table width="100%" cellpadding="0" cellspacing="0"'
            f' style="border-collapse:collapse;border:1px solid #e5e7eb;border-radius:6px;font-size:13px;">'
            f'<thead><tr style="background:#f9fafb;border-bottom:2px solid #e5e7eb;">'
            f'<th style="padding:10px 12px;text-align:left;color:#6b7280;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;">SID</th>'
            f'<th style="padding:10px 12px;text-align:left;color:#6b7280;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;">Issues</th>'
            f'<th style="padding:10px 12px;text-align:center;color:#6b7280;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;">App<br>Count</th>'
            f'<th style="padding:10px 12px;text-align:center;color:#6b7280;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;">RetailPro<br>Count</th>'
            f'<th style="padding:10px 12px;text-align:center;color:#6b7280;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;">App<br>Items</th>'
            f'<th style="padding:10px 12px;text-align:center;color:#6b7280;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;">RetailPro<br>Items</th>'
            f'</tr></thead>'
            f'<tbody>{rows_html}</tbody>'
            f'</table>'
            f'</td></tr>'
        )

        status_badge = (
            f'<span style="display:inline-block;background:#fee2e2;color:#991b1b;'
            f'padding:3px 12px;border-radius:9999px;font-size:12px;font-weight:700;">'
            f'&#9888; {len(issues)} Issue{"s" if len(issues) != 1 else ""} Found</span>'
        )
        status_line = (
            f"<strong>{len(issues)}</strong> issue{'s' if len(issues) != 1 else ''} found comparing "
            f"<strong>Application DB (PostgreSQL)</strong> vs <strong>RetailPro (Oracle)</strong>. "
            f"Only records marked as <em>posted</em> are included."
        )

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"></head>
<body style="margin:0;padding:0;background:#f3f4f6;font-family:Arial,Helvetica,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f3f4f6;">
<tr><td align="center" style="padding:32px 16px;">
<table width="720" cellpadding="0" cellspacing="0"
       style="background:#fff;border-radius:8px;box-shadow:0 1px 4px rgba(0,0,0,.10);max-width:100%;">

  <tr><td style="background:{accent};padding:24px 32px;border-radius:8px 8px 0 0;">
    <p style="margin:0;color:rgba(255,255,255,.75);font-size:11px;letter-spacing:1.5px;text-transform:uppercase;">Duplication Check Report</p>
    <h1 style="margin:6px 0 0;color:#fff;font-size:22px;font-weight:700;">{label}</h1>
    <p style="margin:8px 0 0;color:rgba(255,255,255,.85);font-size:13px;">{since_str} &rarr; {until_str} (PKT)</p>
  </td></tr>

  <tr><td style="padding:20px 32px 0;">
    <div style="margin-bottom:8px;">{status_badge}</div>
    <p style="margin:0;font-size:14px;color:#374151;">{status_line}</p>
  </td></tr>

  {body_html}

  <tr><td style="padding:16px 32px;background:#f9fafb;border-top:1px solid #e5e7eb;border-radius:0 0 8px 8px;">
    <p style="margin:0;color:#9ca3af;font-size:11px;">
      Generated by Dvago Excel Integration &middot; {until.strftime('%d %b %Y %H:%M')} PKT
    </p>
  </td></tr>

</table>
</td></tr>
</table>
</body>
</html>"""


# ─────────────────────────────────────────────────────────────────────────────
# APScheduler entry point
# ─────────────────────────────────────────────────────────────────────────────

async def send_duplication_email() -> None:
    """
    APScheduler job — Duplication Check Email.

    Sends ONE email per module on every scheduler fire.
    Issues found    → detailed table of problematic SIDs.
    No issues found → green "All Clear — No Duplications Found" message.
    """
    tag = "[DuplicationEmail]"
    try:
        interval_hours = await _get_interval_hours()
        since = await _get_last_sent(interval_hours)
        until = now_pkt()

        logger.info(
            "%s Window: %s → %s (%dh). Checking %d module(s).",
            tag, since.isoformat(), until.isoformat(), interval_hours, len(_MODULES),
        )

        from app.db.settings_store import get_setting
        recipients_raw = (await get_setting("duplication_email_recipients")) or ""
        recipients = [e.strip() for e in recipients_raw.split(",") if e.strip()]
        if not recipients:
            logger.info("%s No recipients configured — skipping.", tag)
            await _save_last_sent(until)
            return

        oracle = await _load_oracle_settings()
        if not oracle:
            logger.warning("%s Oracle not configured — skipping.", tag)
            await _save_last_sent(until)
            return

        from app.services.email_service import send_duplication_report_email

        for mod in _MODULES:
            issues = await _check_module(mod, oracle, since, until)
            html   = _build_module_html(mod["name"], issues, since, until)
            await send_duplication_report_email(
                module=mod["name"],
                issues=issues,
                since=since,
                until=until,
                html=html,
                recipients=recipients,
            )

        await _save_last_sent(until)
        logger.info("%s All module emails sent. Window updated to %s.", tag, until.isoformat())

    except Exception as exc:
        logger.exception("%s Job failed: %s", tag, exc)
