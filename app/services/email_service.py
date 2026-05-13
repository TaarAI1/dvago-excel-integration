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
import csv
import io
import logging
import smtplib
from email import encoders
from email.mime.base import MIMEBase
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

async def _load_smtp(module: str | None = None) -> dict | None:
    """Load SMTP settings from DB.

    When *module* is given, look for a module-specific ``smtp_to_email_<module>``
    recipient first; fall back to the global ``smtp_to_email`` when that key is
    empty. Returns None when host is not configured.
    """
    from app.db.settings_store import get_setting
    host = await get_setting("smtp_host", "")
    if not host:
        return None

    global_to = (await get_setting("smtp_to_email", "")) or ""
    module_to = ""
    if module:
        module_to = (await get_setting(f"smtp_to_email_{module}", "")) or ""
    effective_to = module_to or global_to

    return {
        "host":       host,
        "port":       int((await get_setting("smtp_port",     "587")) or "587"),
        "username":   (await get_setting("smtp_username",  "")) or "",
        "password":   (await get_setting("smtp_password",  "")) or "",
        "use_tls":    ((await get_setting("smtp_use_tls",  "true")) or "true").lower() == "true",
        "from_email": (await get_setting("smtp_from_email","")) or "",
        "to_email":   effective_to,
        "reply_to":   (await get_setting("smtp_reply_to",  "")) or "",
        "cc_email":   (await get_setting("smtp_cc_email",  "")) or "",
    }


# ── Error-row fetchers per module ────────────────────────────────────────────

async def _fetch_item_master_errors(batch_key: str) -> list[dict]:
    """Return one CSV row per failed item-master document with every original field."""
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

    rows: list[dict] = []
    for i, doc in enumerate(docs, 1):
        od = doc.original_data or {}
        row: dict = {
            "#":                i,
            "document_id":      str(doc.id),
            "posted":           doc.posted,
            "error_message":    doc.error_message or "",
            "created_at":       doc.created_at.isoformat() if doc.created_at else "",
        }
        # Spread every original source-file field as its own column
        for k, v in od.items():
            row[k] = v if v is not None else ""
        rows.append(row)
    return rows


async def _fetch_doc_errors(
    batch_key: str,
    model_class,
    has_items_data: bool = False,
) -> list[dict]:
    """Return one CSV row per failed/partial document, expanding per-item errors inline."""
    from sqlalchemy import select
    from app.db.postgres import get_session

    async with get_session() as session:
        res = await session.execute(
            select(model_class)
            .where(
                model_class.source_file == batch_key,
                model_class.status.in_(["error", "partial"]),
            )
            .order_by(model_class.created_at.asc())
        )
        docs = res.scalars().all()

    rows: list[dict] = []
    for i, doc in enumerate(docs, 1):
        base: dict = {
            "#":                i,
            "document_id":      str(doc.id),
            "note":             getattr(doc, "note",         None) or "",
            "store_code":       getattr(doc, "store_code",   None) or "",
            "store_name":       getattr(doc, "store_name",   None) or "",
            "store_sid":        getattr(doc, "store_sid",    None) or "",
            "status":           doc.status,
            "item_count":       getattr(doc, "item_count",   ""),
            "posted_count":     getattr(doc, "posted_count", ""),
            "error_count":      getattr(doc, "error_count",  ""),
            "error_message":    doc.error_message or "",
            "error_traceback":  getattr(doc, "error_traceback", None) or "",
            "created_at":       doc.created_at.isoformat() if doc.created_at else "",
            "posted_at":        (doc.posted_at.isoformat() if doc.posted_at else "") if hasattr(doc, "posted_at") else "",
        }

        if has_items_data:
            items_data = getattr(doc, "items_data", None) or []
            failed_items = [it for it in items_data if not it.get("ok", True)]
            if failed_items:
                # Expand: one row per failed item, repeating document fields
                for item in failed_items:
                    item_row = dict(base)
                    item_row["item_upc"]         = item.get("upc", "")
                    item_row["item_adj_value"]    = item.get("adj_value", item.get("new_price", ""))
                    item_row["item_error"]        = item.get("error", "")
                    rows.append(item_row)
                continue  # already appended item rows, skip the doc-level row

        rows.append(base)
    return rows


async def _get_error_rows(module: str, batch_key: str) -> list[dict]:
    if module == "item_master":
        return await _fetch_item_master_errors(batch_key)

    if module == "grn":
        from app.models.grn_doc import GRNDoc
        return await _fetch_doc_errors(batch_key, GRNDoc, has_items_data=False)

    if module == "transfer_slip":
        from app.models.transfer_slip_doc import TransferSlipDoc
        return await _fetch_doc_errors(batch_key, TransferSlipDoc, has_items_data=False)

    if module == "qty_adjustment":
        from app.models.qty_adjustment_doc import QtyAdjustmentDoc
        return await _fetch_doc_errors(batch_key, QtyAdjustmentDoc, has_items_data=True)

    if module == "price_adjustment":
        from app.models.price_adjustment_doc import PriceAdjustmentDoc
        return await _fetch_doc_errors(batch_key, PriceAdjustmentDoc, has_items_data=True)

    return []


# ── CSV builder ───────────────────────────────────────────────────────────────

def _build_csv(error_rows: list[dict]) -> bytes:
    """Serialize *error_rows* to UTF-8 CSV bytes (BOM included for Excel)."""
    if not error_rows:
        return b""
    # Collect all keys preserving insertion order, deduped
    all_keys: list[str] = []
    seen: set[str] = set()
    for row in error_rows:
        for k in row:
            if k not in seen:
                all_keys.append(k)
                seen.add(k)

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=all_keys, extrasaction="ignore", lineterminator="\r\n")
    writer.writeheader()
    writer.writerows(error_rows)
    # UTF-8 BOM so Excel opens it correctly without import wizard
    return "\ufeff".encode("utf-8") + output.getvalue().encode("utf-8")


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


def _error_table(error_rows: list[dict], module: str) -> str:
    """Render a compact HTML preview table (max 50 rows). Full data is in the CSV attachment."""
    if not error_rows:
        return (
            '<p style="color:#15803d;font-size:13px;font-weight:600">'
            '&#10003; No errors — all records processed successfully.</p>'
        )

    is_item_master = module == "item_master"

    # Pick the 3-4 most relevant columns for the preview
    if is_item_master:
        id_label    = "UPC"
        desc_label  = "Description"
        id_key      = "UPC"
        desc_key    = "DESCRIPTION1"
    else:
        id_label    = "Note / Reference"
        desc_label  = "Store"
        id_key      = "note"
        desc_key    = "store_name"

    preview_rows = error_rows[:50]
    more         = len(error_rows) - len(preview_rows)

    th = (
        'style="padding:7px 10px;text-align:left;border-bottom:1px solid #fecaca;'
        'color:#b91c1c;font-size:11px;font-weight:600"'
    )
    header = (
        '<table width="100%" cellpadding="0" cellspacing="0" '
        'style="border-collapse:collapse;font-size:12px;border:1px solid #fecaca;border-radius:4px">'
        f'<thead><tr style="background:#fef2f2">'
        f'<th {th}>#</th>'
        f'<th {th}>{id_label}</th>'
        f'<th {th}>{desc_label}</th>'
        f'<th {th}>Error</th>'
        f'</tr></thead><tbody>'
    )

    body = ""
    for i, row in enumerate(preview_rows, 1):
        bg  = "#ffffff" if i % 2 else "#fafafa"
        err = str(row.get("error_message", row.get("item_error", "")))[:300]
        id_val   = str(row.get(id_key,   row.get("item_upc", "—")) or "—")
        desc_val = str(row.get(desc_key, "—") or "—")
        body += (
            f'<tr style="background:{bg}">'
            f'<td style="padding:6px 10px;border-bottom:1px solid #f3f4f6;color:#9ca3af">{i}</td>'
            f'<td style="padding:6px 10px;border-bottom:1px solid #f3f4f6;'
            f'font-family:monospace;font-size:11px">{id_val}</td>'
            f'<td style="padding:6px 10px;border-bottom:1px solid #f3f4f6">{desc_val}</td>'
            f'<td style="padding:6px 10px;border-bottom:1px solid #f3f4f6;'
            f'color:#dc2626;font-size:11px">{err}</td>'
            f'</tr>'
        )

    footer = ""
    if more > 0:
        footer = (
            f'<tr><td colspan="4" style="padding:8px 10px;text-align:center;'
            f'color:#9ca3af;font-size:11px;font-style:italic">'
            f'… and {more} more error{"s" if more != 1 else ""} — see attached CSV for full details'
            f'</td></tr>'
        )

    return header + body + footer + "</tbody></table>"


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
    err_count  = len(error_rows)
    err_label  = f"Error Details ({err_count} {'error' if err_count == 1 else 'errors'})"
    err_table  = _error_table(error_rows, module)
    csv_note   = (
        '<p style="margin:8px 0 0;font-size:11px;color:#6b7280">'
        '&#128206; Full error data with all columns is attached as a CSV file.</p>'
    ) if err_count > 0 else ""

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
    {csv_note}
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

    The email includes:
    * An HTML summary with a preview error table (up to 50 rows).
    * A CSV attachment with every error row and all available columns
      (only attached when there are errors).

    The module-specific ``smtp_to_email_<module>`` recipient is used when
    configured; falls back to the global ``smtp_to_email`` otherwise.

    The function swallows all exceptions so a misconfigured SMTP server
    never breaks the import response.
    """
    try:
        smtp = await _load_smtp(module)
        if not smtp or not smtp["to_email"]:
            logger.debug(
                "SMTP not configured — skipping batch email for %s / %s",
                module, batch_key,
            )
            return

        error_rows = await _get_error_rows(module, batch_key)
        subject    = _build_subject(module, batch_key, result)
        html       = _build_html(module, batch_key, result, error_rows)
        csv_bytes  = _build_csv(error_rows)

        # Derive a safe filename for the CSV attachment
        safe_batch = batch_key.replace("::", "_").replace("/", "_").replace("\\", "_")
        csv_filename = f"{module}_errors_{safe_batch}.csv"

        def _send() -> None:
            # Use "mixed" so we can attach files; nest HTML in an "alternative" sub-part
            msg = MIMEMultipart("mixed")
            msg["From"]    = smtp["from_email"]
            msg["To"]      = smtp["to_email"]
            msg["Subject"] = subject
            if smtp.get("reply_to"):
                msg["Reply-To"] = smtp["reply_to"]
            if smtp.get("cc_email"):
                msg["Cc"] = smtp["cc_email"]

            # HTML body
            alt = MIMEMultipart("alternative")
            alt.attach(MIMEText(html, "html", "utf-8"))
            msg.attach(alt)

            # CSV attachment (only when there are errors)
            if csv_bytes:
                attachment = MIMEBase("text", "csv", charset="utf-8")
                attachment.set_payload(csv_bytes)
                encoders.encode_base64(attachment)
                attachment.add_header(
                    "Content-Disposition", "attachment", filename=csv_filename
                )
                msg.attach(attachment)

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
            "Batch email sent  module=%s  batch=%s  to=%s  csv_rows=%d",
            module, batch_key, smtp["to_email"], len(error_rows),
        )

    except Exception as exc:
        logger.warning(
            "Batch email failed  module=%s  batch=%s  error=%s",
            module, batch_key, exc,
        )
