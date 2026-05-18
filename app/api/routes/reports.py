"""
Reports API — unified view across all import / export modules.

GET /api/reports
  ?module   = grn | transfer_slip | qty_adjustment | price_adjustment | item_master | sales_export
  &date_from = ISO-8601 datetime (inclusive)
  &date_to   = ISO-8601 datetime (inclusive)
  &status    = posted | error | partial | pending   (optional filter)
  &limit     = 1-500  (default 100)
  &offset    = 0+

Response shape:
  {
    "summary": { "total": n, "success": n, "error": n, "partial": n, "pending": n },
    "rows":    [ { id, created_at, status, sid, note, upc, store, error_message,
                   item_count, posted_count, error_count, source_file } ],
    "total_count": n
  }
"""
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, func, case, and_

from app.core.security import get_current_user
from app.db.postgres import get_session

router = APIRouter(prefix="/api/reports", tags=["reports"])


def _parse_dt(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.replace(tzinfo=None)          # store is UTC-naive in the DB
    except ValueError:
        return None


def _dt_filters(model_col, dt_from, dt_to):
    filters = []
    if dt_from:
        filters.append(model_col >= dt_from)
    if dt_to:
        filters.append(model_col <= dt_to)
    return filters


def _build_summary(rows: list[dict]) -> dict:
    summary = {"total": 0, "success": 0, "error": 0, "partial": 0, "pending": 0}
    for r in rows:
        s = r.get("status", "")
        summary["total"] += 1
        if s == "posted":
            summary["success"] += 1
        elif s == "error":
            summary["error"] += 1
        elif s == "partial":
            summary["partial"] += 1
        else:
            summary["pending"] += 1
    return summary


# ─── GRN ──────────────────────────────────────────────────────────────────────

async def _report_grn(dt_from, dt_to, status, limit, offset):
    from app.models.grn_doc import GRNDoc

    async with get_session() as session:
        filters = _dt_filters(GRNDoc.created_at, dt_from, dt_to)
        if status:
            filters.append(GRNDoc.status == status)

        total = await session.scalar(
            select(func.count(GRNDoc.id)).where(and_(*filters))
        )

        # Summary counts
        summary_q = await session.execute(
            select(
                GRNDoc.status,
                func.count(GRNDoc.id).label("cnt"),
            ).where(and_(*filters)).group_by(GRNDoc.status)
        )
        summary = {"total": total or 0, "success": 0, "error": 0, "partial": 0, "pending": 0}
        for row in summary_q.all():
            if row.status == "posted":
                summary["success"] += row.cnt
            elif row.status == "error":
                summary["error"] += row.cnt
            elif row.status == "partial":
                summary["partial"] += row.cnt
            else:
                summary["pending"] += row.cnt

        docs_q = await session.execute(
            select(GRNDoc)
            .where(and_(*filters))
            .order_by(GRNDoc.created_at.desc())
            .limit(limit).offset(offset)
        )
        docs = docs_q.scalars().all()

    rows = [
        {
            "id":            str(d.id),
            "created_at":    d.created_at.isoformat() if d.created_at else None,
            "status":        d.status,
            "sid":           d.vousid,
            "note":          d.note,
            "upc":           None,
            "store":         d.store_name or d.store_code,
            "error_message": d.error_message,
            "item_count":    d.item_count,
            "posted_count":  d.posted_count,
            "error_count":   d.error_count,
            "source_file":   d.source_file,
            "items_data":    d.items_data,
        }
        for d in docs
    ]
    return {"summary": summary, "rows": rows, "total_count": total or 0}


# ─── Transfer Slip ────────────────────────────────────────────────────────────

async def _report_transfer_slip(dt_from, dt_to, status, limit, offset):
    from app.models.transfer_slip_doc import TransferSlipDoc

    async with get_session() as session:
        filters = _dt_filters(TransferSlipDoc.created_at, dt_from, dt_to)
        if status:
            filters.append(TransferSlipDoc.status == status)

        total = await session.scalar(
            select(func.count(TransferSlipDoc.id)).where(and_(*filters))
        )

        summary_q = await session.execute(
            select(TransferSlipDoc.status, func.count(TransferSlipDoc.id).label("cnt"))
            .where(and_(*filters)).group_by(TransferSlipDoc.status)
        )
        summary = {"total": total or 0, "success": 0, "error": 0, "partial": 0, "pending": 0}
        for row in summary_q.all():
            if row.status == "posted":
                summary["success"] += row.cnt
            elif row.status == "error":
                summary["error"] += row.cnt
            elif row.status == "partial":
                summary["partial"] += row.cnt
            else:
                summary["pending"] += row.cnt

        docs_q = await session.execute(
            select(TransferSlipDoc)
            .where(and_(*filters))
            .order_by(TransferSlipDoc.created_at.desc())
            .limit(limit).offset(offset)
        )
        docs = docs_q.scalars().all()

    rows = [
        {
            "id":            str(d.id),
            "created_at":    d.created_at.isoformat() if d.created_at else None,
            "status":        d.status,
            "sid":           d.slip_sid,
            "note":          d.note,
            "upc":           None,
            "store":         f"{d.out_store_name or ''} → {d.in_store_name or ''}".strip(" →"),
            "error_message": d.error_message,
            "item_count":    d.item_count,
            "posted_count":  d.posted_count,
            "error_count":   d.error_count,
            "source_file":   d.source_file,
            "items_data":    d.items_data,
        }
        for d in docs
    ]
    return {"summary": summary, "rows": rows, "total_count": total or 0}


# ─── Qty Adjustment ───────────────────────────────────────────────────────────

async def _report_qty_adjustment(dt_from, dt_to, status, limit, offset):
    from app.models.qty_adjustment_doc import QtyAdjustmentDoc

    async with get_session() as session:
        filters = _dt_filters(QtyAdjustmentDoc.created_at, dt_from, dt_to)
        if status:
            filters.append(QtyAdjustmentDoc.status == status)

        total = await session.scalar(
            select(func.count(QtyAdjustmentDoc.id)).where(and_(*filters))
        )

        summary_q = await session.execute(
            select(QtyAdjustmentDoc.status, func.count(QtyAdjustmentDoc.id).label("cnt"))
            .where(and_(*filters)).group_by(QtyAdjustmentDoc.status)
        )
        summary = {"total": total or 0, "success": 0, "error": 0, "partial": 0, "pending": 0}
        for row in summary_q.all():
            if row.status == "posted":
                summary["success"] += row.cnt
            elif row.status == "error":
                summary["error"] += row.cnt
            elif row.status == "partial":
                summary["partial"] += row.cnt
            else:
                summary["pending"] += row.cnt

        docs_q = await session.execute(
            select(QtyAdjustmentDoc)
            .where(and_(*filters))
            .order_by(QtyAdjustmentDoc.created_at.desc())
            .limit(limit).offset(offset)
        )
        docs = docs_q.scalars().all()

    rows = [
        {
            "id":            str(d.id),
            "created_at":    d.created_at.isoformat() if d.created_at else None,
            "status":        d.status,
            "sid":           d.adj_sid,
            "note":          d.note,
            "upc":           None,
            "store":         d.store_name or d.store_code,
            "error_message": d.error_message,
            "item_count":    d.item_count,
            "posted_count":  d.posted_count,
            "error_count":   d.error_count,
            "source_file":   d.source_file,
            "items_data":    d.items_data,
        }
        for d in docs
    ]
    return {"summary": summary, "rows": rows, "total_count": total or 0}


# ─── Price Adjustment ─────────────────────────────────────────────────────────

async def _report_price_adjustment(dt_from, dt_to, status, limit, offset):
    from app.models.price_adjustment_doc import PriceAdjustmentDoc

    async with get_session() as session:
        filters = _dt_filters(PriceAdjustmentDoc.created_at, dt_from, dt_to)
        if status:
            filters.append(PriceAdjustmentDoc.status == status)

        total = await session.scalar(
            select(func.count(PriceAdjustmentDoc.id)).where(and_(*filters))
        )

        summary_q = await session.execute(
            select(PriceAdjustmentDoc.status, func.count(PriceAdjustmentDoc.id).label("cnt"))
            .where(and_(*filters)).group_by(PriceAdjustmentDoc.status)
        )
        summary = {"total": total or 0, "success": 0, "error": 0, "partial": 0, "pending": 0}
        for row in summary_q.all():
            if row.status == "posted":
                summary["success"] += row.cnt
            elif row.status == "error":
                summary["error"] += row.cnt
            elif row.status == "partial":
                summary["partial"] += row.cnt
            else:
                summary["pending"] += row.cnt

        docs_q = await session.execute(
            select(PriceAdjustmentDoc)
            .where(and_(*filters))
            .order_by(PriceAdjustmentDoc.created_at.desc())
            .limit(limit).offset(offset)
        )
        docs = docs_q.scalars().all()

    rows = [
        {
            "id":            str(d.id),
            "created_at":    d.created_at.isoformat() if d.created_at else None,
            "status":        d.status,
            "sid":           d.adj_sid,
            "note":          d.note,
            "upc":           None,
            "store":         d.store_name or d.store_code,
            "error_message": d.error_message,
            "item_count":    d.item_count,
            "posted_count":  d.posted_count,
            "error_count":   d.error_count,
            "source_file":   d.source_file,
            "items_data":    d.items_data,
        }
        for d in docs
    ]
    return {"summary": summary, "rows": rows, "total_count": total or 0}


# ─── Item Master ──────────────────────────────────────────────────────────────

async def _report_item_master(dt_from, dt_to, status, limit, offset):
    from app.models.document import Document

    async with get_session() as session:
        base_filters = [Document.document_type == "item_master"]
        base_filters.extend(_dt_filters(Document.created_at, dt_from, dt_to))

        if status == "posted":
            base_filters.append(Document.posted == True)
            base_filters.append(Document.has_error == False)
        elif status == "error":
            base_filters.append(Document.has_error == True)
        elif status == "pending":
            base_filters.append(Document.posted == False)
            base_filters.append(Document.has_error == False)

        total = await session.scalar(
            select(func.count(Document.id)).where(and_(*base_filters))
        )

        # Summary: group by (posted, has_error)
        posted_ok   = await session.scalar(
            select(func.count(Document.id)).where(and_(
                Document.document_type == "item_master",
                *_dt_filters(Document.created_at, dt_from, dt_to),
                Document.posted == True, Document.has_error == False,
            ))
        )
        errored = await session.scalar(
            select(func.count(Document.id)).where(and_(
                Document.document_type == "item_master",
                *_dt_filters(Document.created_at, dt_from, dt_to),
                Document.has_error == True,
            ))
        )
        all_total = await session.scalar(
            select(func.count(Document.id)).where(and_(
                Document.document_type == "item_master",
                *_dt_filters(Document.created_at, dt_from, dt_to),
            ))
        )
        summary = {
            "total":   all_total or 0,
            "success": posted_ok or 0,
            "error":   errored or 0,
            "partial": 0,
            "pending": max(0, (all_total or 0) - (posted_ok or 0) - (errored or 0)),
        }

        docs_q = await session.execute(
            select(Document)
            .where(and_(*base_filters))
            .order_by(Document.created_at.desc())
            .limit(limit).offset(offset)
        )
        docs = docs_q.scalars().all()

    def _derive_status(d):
        if d.has_error:
            return "error"
        if d.posted:
            return "posted"
        return "pending"

    def _extract_upc(d):
        od = d.original_data or {}
        return od.get("UPC") or od.get("upc") or od.get("Upc") or None

    rows = [
        {
            "id":            str(d.id),
            "created_at":    d.created_at.isoformat() if d.created_at else None,
            "status":        _derive_status(d),
            "sid":           d.retailprosid,
            "note":          None,
            "upc":           _extract_upc(d),
            "store":         None,
            "error_message": d.error_message,
            "item_count":    1,
            "posted_count":  1 if d.posted else 0,
            "error_count":   1 if d.has_error else 0,
            "source_file":   d.source_file,
            "items_data":    None,
        }
        for d in docs
    ]
    return {"summary": summary, "rows": rows, "total_count": total or 0}


# ─── Sales Export ─────────────────────────────────────────────────────────────

async def _report_sales_export(dt_from, dt_to, status, limit, offset):
    from app.models.sales_export_run import SalesExportStore

    async with get_session() as session:
        filters = _dt_filters(SalesExportStore.created_at, dt_from, dt_to)
        if status:
            filters.append(SalesExportStore.status == status)

        total = await session.scalar(
            select(func.count(SalesExportStore.id)).where(and_(*filters))
        )

        summary_q = await session.execute(
            select(SalesExportStore.status, func.count(SalesExportStore.id).label("cnt"))
            .where(and_(*filters)).group_by(SalesExportStore.status)
        )
        summary = {"total": total or 0, "success": 0, "error": 0, "partial": 0, "pending": 0}
        for row in summary_q.all():
            if row.status in ("done", "ok", "success"):
                summary["success"] += row.cnt
            elif row.status == "error":
                summary["error"] += row.cnt
            elif row.status == "pending":
                summary["pending"] += row.cnt
            else:
                summary["pending"] += row.cnt

        docs_q = await session.execute(
            select(SalesExportStore)
            .where(and_(*filters))
            .order_by(SalesExportStore.created_at.desc())
            .limit(limit).offset(offset)
        )
        docs = docs_q.scalars().all()

    rows = [
        {
            "id":            str(d.id),
            "created_at":    d.created_at.isoformat() if d.created_at else None,
            "status":        d.status,
            "sid":           None,
            "note":          None,
            "upc":           None,
            "store":         d.store_name or str(d.store_no or ""),
            "file_type":     d.file_type,
            "filename":      d.filename,
            "error_message": d.error_message,
            "item_count":    d.query_rows,
            "posted_count":  d.written_rows,
            "error_count":   0,
            "source_file":   d.run_id,
            "items_data":    None,
        }
        for d in docs
    ]
    return {"summary": summary, "rows": rows, "total_count": total or 0}


# ─── Main endpoint ────────────────────────────────────────────────────────────

MODULES = {"grn", "transfer_slip", "qty_adjustment", "price_adjustment", "item_master", "sales_export"}


@router.get("")
async def get_report(
    module: str = Query(...),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    _: str = Depends(get_current_user),
):
    if module not in MODULES:
        raise HTTPException(status_code=400, detail=f"Unknown module '{module}'. Valid: {sorted(MODULES)}")

    dt_from = _parse_dt(date_from)
    dt_to   = _parse_dt(date_to)

    dispatch = {
        "grn":               _report_grn,
        "transfer_slip":     _report_transfer_slip,
        "qty_adjustment":    _report_qty_adjustment,
        "price_adjustment":  _report_price_adjustment,
        "item_master":       _report_item_master,
        "sales_export":      _report_sales_export,
    }
    return await dispatch[module](dt_from, dt_to, status, limit, offset)
