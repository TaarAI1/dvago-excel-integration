"""
Item Master import routes.

POST /api/item-master/preview  – parse Excel, return first 50 rows (no writes)
POST /api/item-master/import   – full pipeline: DCS/Vendor check-or-create, item upsert
"""
import logging

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File

from app.core.security import get_current_user

router = APIRouter(prefix="/api/item-master", tags=["item-master"])
logger = logging.getLogger(__name__)

_ALLOWED_TYPES = {
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.ms-excel",
    "application/octet-stream",
}


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
    from app.services.item_master_service import process_excel_batch
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

    return result
