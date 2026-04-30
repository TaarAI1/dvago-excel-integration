"""
Shared batch-summary email service.

Called after every import batch (manual or FTP) to send a module-specific
HTML summary email to the configured SMTP recipient.

If SMTP is not configured, or if sending fails for any reason, the error is
logged silently — the import response is never blocked or failed.

Usage (fire-and-forget from an async route):
    import asyncio
    from app.services.email_service import send_batch_email
    asyncio.create_task(send_batch_email("item_master", batch_key, result))
"""

import asyncio
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)

# ── Module display names & accent colours ────────────────────────────────────

MODULE_LABELS: dict[str, str] = {
    "item_master":      "Item Master",
    "grn":              "GRN (Goods Received Note)",
    "transfer_slip":    "Transfer Slip",
    "qty_adjustment":   "Quantity Adjustment",
    "price_adjustment": "Price Adjustment",
}

MODULE_ACCENT: dict[str, str] = {
    "item_master":      "#1a56db",
    "grn":              "#7c3aed",
    "transfer_slip":    "#0891b2",
    "qty_adjustment":   "#059669",
    "price_adjustment": "#d97706",
}


# ── SMTP config loader ───────────────────────────────────────────────────────

async def _load_smtp() -> dict | None:
    """Load SMTP settings from DB. Returns None when host is not configured."""
    from app.db.settings_store import get_setting
    host = await get_setting("smtp_host", "")
    if not host:
        return None
    return {
        "host":       host,
        "port":       int((await get_setting("smtp_port",     "587")) or "587"),
        "username":   (await get_setting("smtp_username",  "")) or "",
        "password":   (await get_setting("smtp_password",  "")) or "",
        "use_tls":    ((await get_setting("smtp_use_tls",  "true")) or "true").lower() == "true",
        "from_email": (await get_setting("smtp_from_email","")) or "",
        "to_email":   (await get_setting("smtp_to_email",  "")) or "",
        "reply_to":   (await get_setting("smtp_reply_to",  "")) or "",
        "cc_email":   (await get_setting("smtp_cc_email",  "")) or "",
    }


# ── Error-row fetchers per module ────────────────────────────────────────────

async def _fetch_item_master_errors(batch_key: str) -> list[dict]:
    from sqlalchemy import select
    from app.db.postgres import get_session
    from app.models.document import Document

    async with get_session() as session:
        res = await session.execute(
            select(Document)
            .where(
                Document.source_file == batch_key,
                Document.document_type == "item_master",
                Document.has_error == True,
            )
            .order_by(Document.created_at.asc())
        )
        docs = res.scalars().all()

    rows = []
    for doc in docs:
        od = doc.original_data or {}
        rows.append({
            "identifier":  od.get("UPC") or "—",
            "description": od.get("DESCRIPTION1") or od.get("DESCRIPTION") or "—",
            "error":       doc.error_message or "Unknown error",
        })
    return rows


async def _fetch_doc_errors(batch_key: str, model_class, id_field: str) -> list[dict]:
    from sqlalchemy import select
    from app.db.postgres import get_session

    async with get_session() as session:
        res = await session.execute(
            select(model_class)
            .where(
                model_class.source_file == batch_key,
                model_class.status == "error",
            )
            .order_by(model_class.created_at.asc())
        )
        docs = res.scalars().all()

    rows = []
    for doc in docs:
        identifier  = getattr(doc, id_field, None) or "—"
        description = (
            getattr(doc, "store_name", None)
            or getattr(doc, "store_code", None)
            or "—"
        )
        rows.append({
            "identifier":  identifier,
            "description": description,
            "error":       doc.error_message or "Unknown error",
        })
    return rows


async def _get_error_rows(module: str, batch_key: str) -> list[dict]:
    if module == "item_master":
        return await _fetch_item_master_errors(batch_key)

    if module == "grn":
        from app.models.grn_doc import GRNDoc
        return await _fetch_doc_errors(batch_key, GRNDoc, "note")

    if module == "transfer_slip":
        from app.models.transfer_slip_doc import TransferSlipDoc
        return await _fetch_doc_errors(batch_key, TransferSlipDoc, "note")

    if module == "qty_adjustment":
        from app.models.qty_adjustment_doc import QtyAdjustmentDoc
        return await _fetch_doc_errors(batch_key, QtyAdjustmentDoc, "note")

    if module == "price_adjustment":
        from app.models.price_adjustment_doc import PriceAdjustmentDoc
        return await _fetch_doc_errors(batch_key, PriceAdjustmentDoc, "note")

    return []


# ── HTML email builder ───────────────────────────────────────────────────────

def _tr(label: str, value, good: bool = True) -> str:
    colour = "#15803d" if good else "#b91c1c"
    return (
        f'<tr>'
        f'<td style="padding:7px 14px;border-bottom:1px solid #f3f4f6;'
        f'color:#6b7280;font-size:13px;width:50%">{label}</td>'
        f'<td style="padding:7px 14px;border-bottom:1px solid #f3f4f6;'
        f'font-weight:600;font-size:13px;color:{colour}">{value}</td>'
        f'</tr>'
    )


def _summary_item_master(result: dict) -> str:
    total   = result.get("of_total", result.get("total", 0))
    created = result.get("created", 0)
    updated = result.get("updated", 0)
    errors  = result.get("errors",  0)
    return "".join([
        _tr("Total Rows",  total),
        _tr("Processed",   result.get("total", 0)),
        _tr("✓ Created",   created),
        _tr("✓ Updated",   updated),
        _tr("✗ Errors",    errors,  good=(errors == 0)),
        _tr("Cancelled",   "Yes" if result.get("cancelled") else "No",
            good=not result.get("cancelled")),
    ])


def _summary_doc_module(result: dict) -> str:
    partial = result.get("partial_docs", 0)
    errors  = result.get("error_docs",   0)
    return "".join([
        _tr("Total Rows",         result.get("total_rows",   0)),
        _tr("Total Documents",    result.get("total_docs",   0)),
        _tr("✓ Posted Documents", result.get("posted_docs",  0)),
        _tr("⚠ Partial Documents",partial, good=(partial == 0)),
        _tr("✗ Error Documents",  errors,  good=(errors  == 0)),
        _tr("Total Items",        result.get("total_items",  0)),
        _tr("✓ Posted Items",     result.get("posted_items", 0)),
        _tr("Cancelled",          "Yes" if result.get("cancelled") else "No",
            good=not result.get("cancelled")),
    ])


def _error_table(error_rows: list[dict], id_label: str) -> str:
    if not error_rows:
        return (
            '<p style="color:#15803d;font-size:13px;font-weight:600">'
            '&#10003; No errors — all records processed successfully.</p>'
        )

    header = (
        '<table width="100%" cellpadding="0" cellspacing="0" '
        'style="border-collapse:collapse;font-size:12px;border:1px solid #fecaca;border-radius:4px">'
        '<thead><tr style="background:#fef2f2">'
        '<th style="padding:7px 10px;text-align:left;border-bottom:1px solid #fecaca;'
        'color:#b91c1c;font-size:11px;font-weight:600">#</th>'
        f'<th style="padding:7px 10px;text-align:left;border-bottom:1px solid #fecaca;'
        f'color:#b91c1c;font-size:11px;font-weight:600">{id_label}</th>'
        '<th style="padding:7px 10px;text-align:left;border-bottom:1px solid #fecaca;'
        'color:#b91c1c;font-size:11px;font-weight:600">Description / Store</th>'
        '<th style="padding:7px 10px;text-align:left;border-bottom:1px solid #fecaca;'
        'color:#b91c1c;font-size:11px;font-weight:600">Error</th>'
        '</tr></thead><tbody>'
    )

    body = ""
    for i, row in enumerate(error_rows, 1):
        bg  = "#ffffff" if i % 2 else "#fafafa"
        err = str(row.get("error", ""))[:400]
        body += (
            f'<tr style="background:{bg}">'
            f'<td style="padding:6px 10px;border-bottom:1px solid #f3f4f6;color:#9ca3af">{i}</td>'
            f'<td style="padding:6px 10px;border-bottom:1px solid #f3f4f6;'
            f'font-family:monospace;font-size:11px">{row.get("identifier","—")}</td>'
            f'<td style="padding:6px 10px;border-bottom:1px solid #f3f4f6">'
            f'{row.get("description","—")}</td>'
            f'<td style="padding:6px 10px;border-bottom:1px solid #f3f4f6;'
            f'color:#dc2626;font-size:11px">{err}</td>'
            f'</tr>'
        )

    return header + body + "</tbody></table>"


def _build_html(
    module: str,
    batch_key: str,
    result: dict,
    error_rows: list[dict],
) -> str:
    label    = MODULE_LABELS.get(module, module.replace("_", " ").title())
    accent   = MODULE_ACCENT.get(module, "#1a56db")
    filename = batch_key.split("::")[0]
    ts       = batch_key.split("::")[-1] if "::" in batch_key else "—"

    error_count = result.get("errors", result.get("error_docs", 0))
    status_txt   = "COMPLETED WITH ERRORS" if error_count else "COMPLETED SUCCESSFULLY"
    status_col   = "#dc2626" if error_count else "#15803d"

    summary_rows = (
        _summary_item_master(result)
        if module == "item_master"
        else _summary_doc_module(result)
    )
    id_label   = "UPC" if module == "item_master" else "Note / Reference"
    err_count  = len(error_rows)
    err_label  = f"Error Details ({err_count} {'error' if err_count == 1 else 'errors'})"
    err_table  = _error_table(error_rows, id_label)

    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f3f4f6;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f3f4f6">
<tr><td style="padding:32px 16px">
<table width="600" align="center" cellpadding="0" cellspacing="0"
  style="background:#ffffff;border-radius:10px;border:1px solid #e5e7eb;overflow:hidden;max-width:600px">

  <!-- Header -->
  <tr><td style="background:{accent};padding:22px 28px">
    <p style="margin:0 0 4px;font-size:11px;color:rgba(255,255,255,0.65);
       letter-spacing:0.08em;text-transform:uppercase">Dvago Excel Integration</p>
    <h1 style="margin:0;font-size:21px;color:#ffffff;font-weight:700;line-height:1.3">
      {label}<br>
      <span style="font-size:14px;font-weight:400;opacity:0.85">Batch Import Report</span>
    </h1>
  </td></tr>

  <!-- Batch meta -->
  <tr><td style="padding:20px 28px 0">
    <table width="100%" cellpadding="0" cellspacing="0">
      <tr>
        <td style="width:50%;vertical-align:top">
          <p style="margin:0 0 2px;font-size:11px;color:#9ca3af;text-transform:uppercase;
             letter-spacing:0.05em">Batch File</p>
          <p style="margin:0;font-size:13px;font-weight:600;color:#111827;
             font-family:monospace;word-break:break-all">{filename}</p>
        </td>
        <td style="width:50%;vertical-align:top;text-align:right">
          <p style="margin:0 0 2px;font-size:11px;color:#9ca3af;text-transform:uppercase;
             letter-spacing:0.05em">Processed At</p>
          <p style="margin:0;font-size:13px;font-weight:600;color:#111827">{ts}</p>
        </td>
      </tr>
      <tr><td colspan="2" style="padding-top:12px">
        <span style="display:inline-block;padding:4px 12px;border-radius:4px;font-size:12px;
          font-weight:700;background:{'#fef2f2' if error_count else '#f0fdf4'};
          color:{status_col};border:1px solid {'#fecaca' if error_count else '#d1fae5'}">
          {status_txt}
        </span>
      </td></tr>
    </table>
  </td></tr>

  <!-- Summary -->
  <tr><td style="padding:20px 28px 0">
    <p style="margin:0 0 10px;font-size:12px;font-weight:700;color:#374151;
       text-transform:uppercase;letter-spacing:0.06em">Summary</p>
    <table width="100%" cellpadding="0" cellspacing="0"
      style="border:1px solid #e5e7eb;border-radius:6px;overflow:hidden">
      <tbody>{summary_rows}</tbody>
    </table>
  </td></tr>

  <!-- Errors -->
  <tr><td style="padding:20px 28px 0">
    <p style="margin:0 0 10px;font-size:12px;font-weight:700;color:#374151;
       text-transform:uppercase;letter-spacing:0.06em">{err_label}</p>
    {err_table}
  </td></tr>

  <!-- Footer -->
  <tr><td style="padding:24px 28px;margin-top:8px">
    <hr style="border:none;border-top:1px solid #f3f4f6;margin:0 0 16px">
    <p style="margin:0;font-size:11px;color:#9ca3af;line-height:1.6">
      This email was sent automatically by the Dvago Excel Integration system.<br>
      Please do not reply to this message.
    </p>
  </td></tr>

</table>
</td></tr>
</table>
</body>
</html>"""


def _build_subject(module: str, batch_key: str, result: dict) -> str:
    label    = MODULE_LABELS.get(module, module.replace("_", " ").title())
    filename = batch_key.split("::")[0]

    if module == "item_master":
        created = result.get("created", 0)
        updated = result.get("updated", 0)
        errors  = result.get("errors",  0)
        flag    = "✓" if not errors else "✗"
        return (
            f"[{label}] {flag} {filename} — "
            f"{created} created, {updated} updated, {errors} errors"
        )

    posted  = result.get("posted_docs",  0)
    total   = result.get("total_docs",   0)
    errors  = result.get("error_docs",   0)
    flag    = "✓" if not errors else "✗"
    return (
        f"[{label}] {flag} {filename} — "
        f"{posted}/{total} docs posted, {errors} errors"
    )


# ── Public entry point ───────────────────────────────────────────────────────

async def send_batch_email(module: str, batch_key: str, result: dict) -> None:
    """
    Build and send a batch-summary email for *module* / *batch_key*.

    Parameters
    ----------
    module    : one of "item_master", "grn", "transfer_slip",
                "qty_adjustment", "price_adjustment"
    batch_key : the source_file value used for this batch
                (e.g. "items.csv::20260430_152305")
    result    : the dict returned by the service's process_* function

    The function swallows all exceptions so a misconfigured SMTP server
    never breaks the import response.
    """
    try:
        smtp = await _load_smtp()
        if not smtp or not smtp["to_email"]:
            logger.debug(
                "SMTP not configured — skipping batch email for %s / %s",
                module, batch_key,
            )
            return

        error_rows = await _get_error_rows(module, batch_key)
        subject    = _build_subject(module, batch_key, result)
        html       = _build_html(module, batch_key, result, error_rows)

        def _send() -> None:
            msg = MIMEMultipart("alternative")
            msg["From"]    = smtp["from_email"]
            msg["To"]      = smtp["to_email"]
            msg["Subject"] = subject
            if smtp.get("reply_to"):
                msg["Reply-To"] = smtp["reply_to"]
            if smtp.get("cc_email"):
                msg["Cc"] = smtp["cc_email"]
            msg.attach(MIMEText(html, "html", "utf-8"))

            if smtp["use_tls"]:
                server = smtplib.SMTP(smtp["host"], smtp["port"], timeout=20)
                server.ehlo()
                server.starttls()
                server.ehlo()
            else:
                server = smtplib.SMTP_SSL(smtp["host"], smtp["port"], timeout=20)

            if smtp["username"]:
                server.login(smtp["username"], smtp["password"])

            recipients = [smtp["to_email"]]
            if smtp.get("cc_email"):
                recipients.append(smtp["cc_email"])

            server.sendmail(smtp["from_email"], recipients, msg.as_string())
            server.quit()

        await asyncio.to_thread(_send)
        logger.info(
            "Batch email sent  module=%s  batch=%s  to=%s",
            module, batch_key, smtp["to_email"],
        )

    except Exception as exc:
        logger.warning(
            "Batch email failed  module=%s  batch=%s  error=%s",
            module, batch_key, exc,
        )
