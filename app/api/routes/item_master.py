"""
Item Master import routes.

POST /api/item-master/preview    – parse Excel, return first 50 rows (no writes)
POST /api/item-master/import-csv – manual CSV upload, full pipeline
POST /api/item-master/import     – manual Excel upload, full pipeline
GET  /api/item-master/status     – whether an import is currently running
POST /api/item-master/kill       – cancel the running import after the current row
"""
import logging

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Query
from fastapi.responses import StreamingResponse
from typing import Any

from app.core.security import get_current_user

router = APIRouter(prefix="/api/item-master", tags=["item-master"])
logger = logging.getLogger(__name__)

_ALLOWED_TYPES = {
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.ms-excel",
    "application/octet-stream",
}


@router.get("/status")
async def import_status(_: str = Depends(get_current_user)):
    """Return whether a manual import is currently running."""
    from app.services.item_master_service import get_active_import_id
    active = get_active_import_id()
    return {"running": active is not None, "import_id": active}


@router.post("/kill")
async def kill_import(_: str = Depends(get_current_user)):
    """Cancel the running import — it will stop after the current row finishes."""
    from app.services.item_master_service import request_cancel_import, get_active_import_id
    active = get_active_import_id()
    if not active:
        return {"cancelled": False, "message": "No import is currently running."}
    request_cancel_import()
    return {"cancelled": True, "import_id": active, "message": "Stop signal sent — will halt after current row completes."}


@router.post("/debug-payload")
async def debug_payload(
    body: dict[str, Any],
    _: str = Depends(get_current_user),
):
    """
    Debug endpoint: given a RetailPro InventorySaveItems payload object,
    binary-searches through every field of InventoryItems[0] and invnextend[0]
    to find the exact field causing the 'is not a valid integer' serialiser error.

    Request body: the single payload object (NOT wrapped in {"data": [...]}).
    Example:
        {
            "OriginApplication": "RProPrismWeb",
            "PrimaryItemDefinition": {...},
            "InventoryItems": [{...}],
            ...
        }

    Response:
        {"found": true,  "section": "InventoryItems", "field": "itemsize", "value": "14'S"}
        {"found": false, "message": "Error not reproducible — ..."}
    """
    from app.services.item_master_service import diagnose_bad_field
    try:
        result = await diagnose_bad_field(body)
    except Exception as exc:
        logger.exception("debug-payload failed")
        raise HTTPException(status_code=500, detail=str(exc))
    return result


@router.post("/preview")
async def preview_excel(
    file: UploadFile = File(...),
    _: str = Depends(get_current_user),
):
    """
    Parse the uploaded Excel file and return a preview of the first 50 data rows.
    No data is written to Oracle or RetailPro.
    """
    from app.services.item_master_service import parse_excel

    raw = await file.read()
    try:
        rows = parse_excel(raw)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception("Excel parse error")
        raise HTTPException(status_code=422, detail=f"Could not read Excel file: {exc}")

    if not rows:
        raise HTTPException(status_code=400, detail="No data rows found (all rows missing UPC).")

    # Truncate long cell values for the preview response
    preview = [
        {k: (str(v)[:300] if v is not None else None) for k, v in row.items()}
        for row in rows[:50]
    ]
    return {
        "total_rows": len(rows),
        "preview_rows": preview,
        "columns": list(rows[0].keys()) if rows else [],
    }


@router.post("/import-csv")
async def import_csv(
    file: UploadFile = File(...),
    _: str = Depends(get_current_user),
):
    """
    Manual CSV upload — runs the full item master pipeline immediately.

    Accepts the same CSV format produced by FTP polling.
    Returns a summary with total, created, updated, and error counts.
    """
    import asyncio
    from app.services.item_master_service import process_csv_batch
    from app.services.email_service import send_batch_email
    from app.core.timezone import now_pkt

    raw = await file.read()
    base_name = file.filename or "upload.csv"
    batch_key = f"{base_name}::{now_pkt().strftime('%Y%m%d_%H%M%S')}"
    try:
        result = await process_csv_batch(raw, source_file=batch_key)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception("Item master CSV import failed")
        raise HTTPException(status_code=500, detail=str(exc))

    asyncio.create_task(send_batch_email("item_master", batch_key, result))
    return result


@router.post("/import")
async def import_excel(
    file: UploadFile = File(...),
    _: str = Depends(get_current_user),
):
    """
    Full import pipeline for Item Master Excel file.

    For each row:
      1. Check / create DCS in RetailPro (Oracle lookup + API create if missing)
      2. Check / create Vendor in RetailPro (Oracle lookup + API create if missing)
      3. GET inventory by UPC — update if found, create if not
      4. POST /api/backoffice/inventory?action=InventorySaveItems

    Auth-Session is obtained once for the entire batch.
    Oracle lookups are cached per DCS_CODE / VEND_CODE / TAX_CODE / SBS_NO.

    Returns a summary with per-row results.
    """
    import asyncio
    from app.services.item_master_service import process_excel_batch
    from app.services.email_service import send_batch_email
    from app.core.timezone import now_pkt

    raw = await file.read()
    # Unique batch key: filename + timestamp so uploading the same file twice
    # always produces separate, distinct batches.
    base_name = file.filename or "upload.xlsx"
    batch_key = f"{base_name}::{now_pkt().strftime('%Y%m%d_%H%M%S')}"
    try:
        result = await process_excel_batch(raw, source_file=batch_key)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception("Item master import failed")
        raise HTTPException(status_code=500, detail=str(exc))

    asyncio.create_task(send_batch_email("item_master", batch_key, result))
    return result


@router.get("/batch-download")
async def download_batch_csv(
    source_file: str = Query(..., description="Batch key (source_file) to download"),
    _: str = Depends(get_current_user),
):
    """
    Download a processed CSV for an item master batch.

    Every row from the batch is included. The UPC column is filled with the
    value RetailPro returned (or assigned) for successfully posted items.
    For items that failed to post, the UPC column is left blank so the user
    can clearly see which entries need attention.
    """
    import csv
    import io
    from sqlalchemy import select
    from app.db.postgres import get_session
    from app.models.document import Document

    # Internal keys written by _persist_result that must not appear in the download
    SKIP_KEYS = {"_payload_sent", "_dcs_debug", "_vend_debug"}

    async with get_session() as session:
        result = await session.execute(
            select(Document)
            .where(
                Document.source_file == source_file,
                Document.document_type == "item_master",
            )
            .order_by(Document.created_at.asc())
        )
        docs = result.scalars().all()

    if not docs:
        raise HTTPException(status_code=404, detail="No documents found for this batch.")

    # Build an ordered, deduplicated list of CSV columns, skipping internal keys.
    # UPC is always placed first so it is easy to spot.
    seen_keys: set[str] = set()
    all_keys: list[str] = []
    for doc in docs:
        for k in (doc.original_data or {}):
            if k not in seen_keys and k not in SKIP_KEYS:
                seen_keys.add(k)
                all_keys.append(k)

    if "UPC" in seen_keys:
        all_keys.remove("UPC")
        all_keys.insert(0, "UPC")

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=all_keys, extrasaction="ignore")
    writer.writeheader()

    for doc in docs:
        row = {k: v for k, v in (doc.original_data or {}).items() if k not in SKIP_KEYS}
        # Failed items get a blank UPC regardless of what was in the source file
        if not doc.posted:
            row["UPC"] = ""
        writer.writerow(row)

    # Derive a sensible filename: strip the timestamp suffix added by the batch key
    base_name = source_file.split("::")[0]
    stem = base_name.rsplit(".", 1)[0] if "." in base_name else base_name
    download_name = f"{stem}_processed.csv"

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{download_name}"'},
    )
