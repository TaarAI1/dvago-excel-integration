"""
GRN (Goods Received Note) import service.

Processing pipeline (per note-group, up to 900 items per voucher):
  1.  Parse CSV — find header row, extract Store Code, upc, Vendor Code, note, totalqty
  2.  Skip rows missing upc or note
  3.  Group rows by note field
  4.  Duplicate check (both must return nothing to proceed):
        Q1: SELECT note FROM rps.voucher WHERE status = 4 AND note = '{note}'
        Q2: SELECT vc.comments FROM rps.vou_comment vc
            JOIN rps.voucher v ON vc.vou_sid = v.sid
            WHERE vc.comments = '{note}' AND v.status = 4
        → skip if either query returns a result
  5.  For each note-group (chunked to 900 items per voucher):
      a. Oracle lookup for store:
            SELECT sid, sbs_sid FROM rps.store WHERE store_code = '{store_code}'
            → storesid (sid), sbssid (sbs_sid)
      b. Oracle lookup for vendor:
            SELECT sid FROM rps.vendor WHERE vend_code = '{vendor_code}'
            → vendsid
      c. POST /api/backoffice/receiving   (note included directly in payload)
            payload: {"data":[{…, "note": "{note}"}]}
            → vousid
      d. GET  /api/backoffice/receiving?filter=(sid,eq,{vousid})
            → rowversion
      e. PUT  /api/backoffice/receiving/{vousid}           (set vendor)
            payload: {"data":[{"rowversion":…,"vendsid":"…"}]}
      f. Resolve each item SID:
            SELECT sid FROM rps.invn_sbs_item WHERE upc = '{upc}'
      g. POST /api/backoffice/receiving/{vousid}/recvitem   (all items, max 900)
      h. GET  /api/backoffice/receiving?filter=(sid,eq,{vousid})  → updated rowversion
      i. PUT  /api/backoffice/receiving/{vousid}  → status 4
  6.  Persist each GRN document to grn_docs table
"""
import asyncio
import csv
import io
import logging
import traceback
import uuid as _uuid
from typing import Optional

import httpx

from app.core.timezone import now_pkt
from app.services.http_utils import http_call_with_retry

logger = logging.getLogger(__name__)

GRN_ITEM_LIMIT = 900

# ── In-memory cancel state ─────────────────────────────────────────────────────
_active_import_id: Optional[str] = None
_cancel_requests: set = set()


def get_active_import_id() -> Optional[str]:
    return _active_import_id


def request_cancel_import() -> bool:
    global _active_import_id
    if not _active_import_id:
        return False
    _cancel_requests.add(_active_import_id)
    logger.info("Cancel requested for GRN import %s", _active_import_id)
    return True


def _is_cancelled(import_id: str) -> bool:
    return import_id in _cancel_requests


# ── CSV parsing ────────────────────────────────────────────────────────────────

def parse_grn_csv(file_bytes: bytes) -> list[dict]:
    """
    Parse GRN CSV with multi-row headers.
    Scans lines to find the row containing 'note', 'upc', and 'totalqty'
    (case-insensitive), then uses that as the header row.
    Returns a list of normalised row dicts.
    Skips rows where upc or note is empty.
    """
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            text = file_bytes.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise ValueError("Could not decode CSV (tried utf-8-sig, utf-8, latin-1).")

    lines = text.splitlines()
    header_idx: Optional[int] = None

    for i, line in enumerate(lines):
        cols_lower = [c.strip().lower() for c in line.split(",")]
        if "note" in cols_lower and "upc" in cols_lower and "totalqty" in cols_lower:
            header_idx = i
            break

    if header_idx is None:
        raise ValueError(
            "Could not find header row containing 'note', 'upc', and 'totalqty' columns."
        )

    csv_text = "\n".join(lines[header_idx:])
    reader = csv.DictReader(io.StringIO(csv_text))

    rows_out: list[dict] = []
    for row in reader:
        normalised = {
            k.strip().upper().replace(" ", "_"): str(v).strip()
            for k, v in row.items()
            if k
        }
        upc  = normalised.get("UPC", "").strip()
        note = normalised.get("NOTE", "").strip()
        if not upc or not note:
            continue
        rows_out.append(normalised)

    return rows_out


# ── Oracle helpers ─────────────────────────────────────────────────────────────

_ORA_IN_LIMIT = 999  # Oracle hard-limits IN (...) to 1000 expressions


def _chunks(lst: list, size: int):
    """Yield successive sub-lists of at most `size` items."""
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


async def _oracle_row(sql: str, oc: dict) -> Optional[tuple]:
    """Run a query and return the first row as a tuple, or None. Uses pool if available."""
    pool = oc.get("pool")
    if pool is not None:
        from app.services.oracle_service import run_query_with_pool
        df = await run_query_with_pool(pool, sql)
    else:
        from app.services.oracle_service import run_query
        df = await run_query(
            oc["host"], oc["port"], oc["service_name"], oc["username"], oc["password"], sql
        )
    if df is None or df.is_empty():
        return None
    return df.row(0)


async def _get_store_sids(
    store_code: str, cache: dict, oc: dict
) -> tuple[Optional[str], Optional[str]]:
    key = str(store_code).strip()
    if key not in cache:
        row = await _oracle_row(
            f"SELECT sid, sbs_sid FROM rps.store WHERE store_code = {key}", oc
        )
        if row:
            cache[key] = (str(row[0]) if row[0] else None, str(row[1]) if row[1] else None)
        else:
            cache[key] = (None, None)
    return cache[key]


async def _get_vendor_sid(vendor_code: str, cache: dict, oc: dict) -> Optional[str]:
    key = str(vendor_code).strip()
    if key not in cache:
        row = await _oracle_row(
            f"SELECT sid FROM rps.vendor WHERE vend_code = '{key}'", oc
        )
        cache[key] = str(row[0]) if row and row[0] else None
    return cache[key]


async def _get_item_info(upc: str, cache: dict, oc: dict) -> tuple[Optional[str], Optional[int]]:
    """Return (sid, active) for a UPC.  active=1 → active, 0 → inactive, None → not found."""
    key = str(upc).strip()
    if key not in cache:
        row = await _oracle_row(
            f"SELECT sid, active FROM rps.invn_sbs_item WHERE upc = '{key}'", oc
        )
        if row:
            cache[key] = (
                str(row[0]) if row[0] is not None else None,
                int(row[1]) if row[1] is not None else None,
            )
        else:
            cache[key] = (None, None)
    return cache[key]


async def _batch_load_store_sids(store_codes: list[str], cache: dict, oc: dict) -> None:
    """Bulk-load store SID/sbs_sid for all given store codes (chunked to avoid ORA-01795)."""
    unknown = [s for s in store_codes if s and s not in cache]
    if not unknown:
        return
    pool = oc.get("pool")
    for chunk in _chunks(unknown, _ORA_IN_LIMIT):
        placeholders = ", ".join(str(s) for s in chunk)
        sql = f"SELECT store_code, sid, sbs_sid FROM rps.store WHERE store_code IN ({placeholders})"
        if pool is not None:
            from app.services.oracle_service import run_query_with_pool
            df = await run_query_with_pool(pool, sql)
        else:
            from app.services.oracle_service import run_query
            df = await run_query(oc["host"], oc["port"], oc["service_name"], oc["username"], oc["password"], sql)
        if df is not None and not df.is_empty():
            df.columns = [c.upper() for c in df.columns]
            for row in df.iter_rows(named=True):
                sc = str(row.get("STORE_CODE") or "").strip()
                if sc:
                    val = (
                        str(row["SID"]) if row.get("SID") else None,
                        str(row["SBS_SID"]) if row.get("SBS_SID") else None,
                    )
                    cache[sc] = val

    # Oracle may return numeric store codes as "13" while the CSV input was "013".
    # Only alias the key when we can confirm the match; leave it absent otherwise
    # so _get_store_sids falls back to the individual Oracle query (never false (None,None)).
    for s in unknown:
        if s not in cache:
            try:
                norm = str(int(float(s)))  # "013" → "13"
                if norm in cache:
                    cache[s] = cache[norm]
                # else: leave absent → _get_store_sids individual query handles it
            except (ValueError, TypeError):
                pass  # non-numeric code → leave absent


async def _batch_load_vendor_sids(vendor_codes: list[str], cache: dict, oc: dict) -> None:
    """Bulk-load vendor SIDs for all given vendor codes (chunked to avoid ORA-01795)."""
    unknown = [v for v in vendor_codes if v and v not in cache]
    if not unknown:
        return
    pool = oc.get("pool")
    for chunk in _chunks(unknown, _ORA_IN_LIMIT):
        placeholders = ", ".join(f"'{v}'" for v in chunk)
        sql = f"SELECT vend_code, sid FROM rps.vendor WHERE vend_code IN ({placeholders})"
        if pool is not None:
            from app.services.oracle_service import run_query_with_pool
            df = await run_query_with_pool(pool, sql)
        else:
            from app.services.oracle_service import run_query
            df = await run_query(oc["host"], oc["port"], oc["service_name"], oc["username"], oc["password"], sql)
        if df is not None and not df.is_empty():
            df.columns = [c.upper() for c in df.columns]
            for row in df.iter_rows(named=True):
                vk = str(row.get("VEND_CODE") or "").strip()
                if vk:
                    cache[vk] = str(row["SID"]) if row.get("SID") else None

    # vend_code is VARCHAR — exact string match; leave uncached keys absent so
    # _get_vendor_sid falls back to the individual Oracle query.
    # No numeric normalisation needed for string vendor codes.


async def _batch_load_item_info(upcs: list[str], cache: dict, oc: dict) -> None:
    """Bulk-load item SID/active for all given UPCs (chunked to avoid ORA-01795)."""
    unknown = [u for u in upcs if u and u not in cache]
    if not unknown:
        return
    pool = oc.get("pool")
    for chunk in _chunks(unknown, _ORA_IN_LIMIT):
        placeholders = ", ".join(f"'{u}'" for u in chunk)
        sql = f"SELECT upc, sid, active FROM rps.invn_sbs_item WHERE upc IN ({placeholders})"
        if pool is not None:
            from app.services.oracle_service import run_query_with_pool
            df = await run_query_with_pool(pool, sql)
        else:
            from app.services.oracle_service import run_query
            df = await run_query(oc["host"], oc["port"], oc["service_name"], oc["username"], oc["password"], sql)
        if df is not None and not df.is_empty():
            df.columns = [c.upper() for c in df.columns]
            for row in df.iter_rows(named=True):
                upc_key = str(row.get("UPC") or "").strip()
                if upc_key:
                    cache[upc_key] = (
                        str(row["SID"]) if row.get("SID") is not None else None,
                        int(row["ACTIVE"]) if row.get("ACTIVE") is not None else None,
                    )
    for u in unknown:
        if u not in cache:
            cache[u] = (None, None)


async def _batch_check_processed_notes(notes: list[str], oc: dict) -> set[str]:
    """Return the set of notes already finalised in RetailPro (chunked to avoid ORA-01795).

    Runs two independent queries and unions the results — a note is considered
    already processed if it appears in EITHER:

    Query 1 (voucher.note):
        SELECT note FROM rps.voucher WHERE status = 4 AND note IN (...)

    Query 2 (vou_comment.comments joined to voucher):
        SELECT vc.comments FROM rps.vou_comment vc
        JOIN rps.voucher v ON vc.vou_sid = v.sid
        WHERE vc.comments IN (...) AND v.status = 4
    """
    if not notes:
        return set()
    pool = oc.get("pool")
    result: set[str] = set()

    async def _run(sql: str) -> None:
        if pool is not None:
            from app.services.oracle_service import run_query_with_pool
            df = await run_query_with_pool(pool, sql)
        else:
            from app.services.oracle_service import run_query
            df = await run_query(
                oc["host"], oc["port"], oc["service_name"], oc["username"], oc["password"], sql
            )
        return df

    for chunk in _chunks(notes, _ORA_IN_LIMIT):
        placeholders = ", ".join(f"'{n}'" for n in chunk)

        # Query 1 — voucher.note
        sql1 = (
            f"SELECT note FROM rps.voucher "
            f"WHERE status = 4 AND note IN ({placeholders})"
        )
        df1 = await _run(sql1)
        if df1 is not None and not df1.is_empty():
            df1.columns = [c.upper() for c in df1.columns]
            result.update(
                str(row.get("NOTE") or "").strip()
                for row in df1.iter_rows(named=True)
                if row.get("NOTE")
            )

        # Query 2 — vou_comment joined to voucher
        sql2 = (
            f"SELECT vc.comments FROM rps.vou_comment vc "
            f"JOIN rps.voucher v ON vc.vou_sid = v.sid "
            f"WHERE vc.comments IN ({placeholders}) AND v.status = 4"
        )
        df2 = await _run(sql2)
        if df2 is not None and not df2.is_empty():
            df2.columns = [c.upper() for c in df2.columns]
            result.update(
                str(row.get("COMMENTS") or "").strip()
                for row in df2.iter_rows(named=True)
                if row.get("COMMENTS")
            )

    return result


# ── RetailPro API calls ────────────────────────────────────────────────────────

def _rp_headers(auth_session: str) -> dict:
    return {
        "Accept": "application/json, version=2",
        "Auth-Session": auth_session,
        "Content-Type": "application/json",
    }


async def _create_grn_doc(
    http: httpx.AsyncClient,
    base_url: str,
    auth_session: str,
    storesid: str,
    sbssid: str,
    note: str = "",
) -> tuple[Optional[str], dict, dict]:
    """
    POST /api/backoffice/receiving
    The note from the CSV is included directly in the create payload (no createddatetime).
    Returns (vousid, payload_sent, response_json).
    """
    payload = {
        "data": [{
            "originapplication": "RProPrismWeb",
            "isBlank": True,
            "sbssid": sbssid,
            "storesid": storesid,
            "publishstatus": 1,
            "voutype": 0,
            "vouclass": 0,
            "verified": True,
            "note": note,
        }]
    }
    resp = await http_call_with_retry(
        http.post,
        f"{base_url}/api/backoffice/receiving",
        json=payload,
        headers=_rp_headers(auth_session),
    )
    try:
        resp_json = resp.json()
    except Exception:
        resp_json = {"raw": resp.text}

    data = resp_json.get("data") or [] if isinstance(resp_json, dict) else []
    vousid = data[0].get("sid") if data else None
    return vousid, payload, resp_json


async def _get_grn_rowversion(
    http: httpx.AsyncClient,
    base_url: str,
    auth_session: str,
    vousid: str,
) -> tuple[Optional[int], dict]:
    """
    GET /api/backoffice/receiving?filter=(sid,eq,{vousid})
    Returns (rowversion, response_json).
    """
    resp = await http_call_with_retry(
        http.get,
        f"{base_url}/api/backoffice/receiving",
        params={"filter": f"(sid,eq,{vousid})"},
        headers=_rp_headers(auth_session),
    )
    try:
        resp_json = resp.json()
    except Exception:
        resp_json = {"raw": resp.text}

    data = resp_json.get("data") or [] if isinstance(resp_json, dict) else []
    rowversion = data[0].get("rowversion") if data else None
    return rowversion, resp_json


async def _update_grn_vendor(
    http: httpx.AsyncClient,
    base_url: str,
    auth_session: str,
    vousid: str,
    rowversion: int,
    vendsid: str,
) -> tuple[dict, dict]:
    """
    PUT /api/backoffice/receiving/{vousid}   (set vendor + rowversion)
    Returns (payload_sent, response_json).
    """
    payload = {"data": [{"rowversion": rowversion, "vendsid": vendsid}]}
    resp = await http_call_with_retry(
        http.put,
        f"{base_url}/api/backoffice/receiving/{vousid}",
        json=payload,
        headers=_rp_headers(auth_session),
    )
    try:
        resp_json = resp.json()
    except Exception:
        resp_json = {"raw": resp.text}
    return payload, resp_json


async def _post_grn_items(
    http: httpx.AsyncClient,
    base_url: str,
    auth_session: str,
    vousid: str,
    items: list[dict],
) -> tuple[dict, dict]:
    """
    POST /api/backoffice/receiving/{vousid}/recvitem
    items: [{"item_sid": …, "qty": …}]
    Returns (payload_sent, response_json).
    """
    payload = {
        "data": [
            {
                "originapplication": "RProPrismWeb",
                "itemsid": item["item_sid"],
                "qty": item["qty"],
                "vousid": vousid,
            }
            for item in items
            if item.get("item_sid")
        ]
    }
    resp = await http_call_with_retry(
        http.post,
        f"{base_url}/api/backoffice/receiving/{vousid}/recvitem",
        json=payload,
        headers=_rp_headers(auth_session),
    )
    try:
        resp_json = resp.json()
    except Exception:
        resp_json = {"raw": resp.text}
    return payload, resp_json


async def _post_grn_comment(
    http: httpx.AsyncClient,
    base_url: str,
    auth_session: str,
    vousid: str,
    note: str,
) -> tuple[dict, dict]:
    """
    POST /api/backoffice/receiving/{vousid}/recvcomment?comments={note}
    Returns (payload_sent, response_json).
    """
    payload = {
        "data": [{
            "originapplication": "RProPrismWeb",
            "comments": note,
            "vousid": vousid,
        }]
    }
    resp = await http_call_with_retry(
        http.post,
        f"{base_url}/api/backoffice/receiving/{vousid}/recvcomment",
        params={"comments": note},
        json=payload,
        headers=_rp_headers(auth_session),
    )
    try:
        resp_json = resp.json()
    except Exception:
        resp_json = {"raw": resp.text}
    return payload, resp_json


async def _finalize_grn(
    http: httpx.AsyncClient,
    base_url: str,
    auth_session: str,
    vousid: str,
    rowversion: int,
) -> tuple[dict, dict]:
    """
    PUT /api/backoffice/receiving/{vousid}
    Returns (payload_sent, response_json).
    """
    payload = {"data": [{"rowversion": rowversion, "status": 4, "approvstatus": 2, "publishstatus": 2}]}
    resp = await http_call_with_retry(
        http.put,
        f"{base_url}/api/backoffice/receiving/{vousid}",
        json=payload,
        headers=_rp_headers(auth_session),
    )
    try:
        resp_json = resp.json()
    except Exception:
        resp_json = {"raw": resp.text}
    return payload, resp_json


# ── DB persistence ─────────────────────────────────────────────────────────────

async def _persist_grn_doc(doc_data: dict) -> None:
    from app.db.postgres import get_session
    from app.models.grn_doc import GRNDoc

    existing_id = doc_data.get("_existing_id")

    async with get_session() as session:
        async with session.begin():
            if existing_id:
                doc = await session.get(GRNDoc, existing_id)
                if doc:
                    doc.storesid                   = doc_data.get("storesid")
                    doc.sbssid                     = doc_data.get("sbssid")
                    doc.vendsid                    = doc_data.get("vendsid")
                    doc.vousid                     = doc_data.get("vousid")
                    doc.item_count                 = doc_data.get("item_count", 0)
                    doc.posted_count               = doc_data.get("posted_count", 0)
                    doc.error_count                = doc_data.get("error_count", 0)
                    doc.status                     = doc_data.get("status", "pending")
                    doc.error_message              = doc_data.get("error_message")
                    doc.error_traceback            = doc_data.get("error_traceback")
                    doc.api_create_payload         = doc_data.get("api_create_payload")
                    doc.api_create_response        = doc_data.get("api_create_response")
                    doc.api_get_rowversion_response= doc_data.get("api_get_rowversion_response")
                    doc.api_vendor_payload         = doc_data.get("api_vendor_payload")
                    doc.api_vendor_response        = doc_data.get("api_vendor_response")
                    doc.api_items_payload          = doc_data.get("api_items_payload")
                    doc.api_items_response         = doc_data.get("api_items_response")
                    doc.api_comment_payload        = doc_data.get("api_comment_payload")
                    doc.api_comment_response       = doc_data.get("api_comment_response")
                    doc.api_get_rowversion2_response = doc_data.get("api_get_rowversion2_response")
                    doc.api_finalize_payload       = doc_data.get("api_finalize_payload")
                    doc.api_finalize_response      = doc_data.get("api_finalize_response")
                    doc.items_data                 = doc_data.get("items_data")
                    doc.raw_rows                   = doc_data.get("raw_rows")
                    if doc_data.get("status") == "posted":
                        doc.posted_at = now_pkt()
                    return
            doc = GRNDoc(
                id=_uuid.uuid4(),
                source_file=doc_data.get("source_file"),
                note=doc_data.get("note"),
                store_code=doc_data.get("store_code"),
                store_name=doc_data.get("store_name"),
                storesid=doc_data.get("storesid"),
                sbssid=doc_data.get("sbssid"),
                vendsid=doc_data.get("vendsid"),
                vousid=doc_data.get("vousid"),
                item_count=doc_data.get("item_count", 0),
                posted_count=doc_data.get("posted_count", 0),
                error_count=doc_data.get("error_count", 0),
                status=doc_data.get("status", "pending"),
                error_message=doc_data.get("error_message"),
                error_traceback=doc_data.get("error_traceback"),
                api_create_payload=doc_data.get("api_create_payload"),
                api_create_response=doc_data.get("api_create_response"),
                api_get_rowversion_response=doc_data.get("api_get_rowversion_response"),
                api_vendor_payload=doc_data.get("api_vendor_payload"),
                api_vendor_response=doc_data.get("api_vendor_response"),
                api_items_payload=doc_data.get("api_items_payload"),
                api_items_response=doc_data.get("api_items_response"),
                api_comment_payload=doc_data.get("api_comment_payload"),
                api_comment_response=doc_data.get("api_comment_response"),
                api_get_rowversion2_response=doc_data.get("api_get_rowversion2_response"),
                api_finalize_payload=doc_data.get("api_finalize_payload"),
                api_finalize_response=doc_data.get("api_finalize_response"),
                items_data=doc_data.get("items_data"),
                raw_rows=doc_data.get("raw_rows"),
                posted_at=now_pkt() if doc_data.get("status") == "posted" else None,
            )
            session.add(doc)


# ── Note-group processor ───────────────────────────────────────────────────────

async def _process_note_group(
    note: str,
    rows: list[dict],
    source_file: str,
    base_url: str,
    auth_session: str,
    http: httpx.AsyncClient,
    oc: dict,
    store_cache: dict,
    vendor_cache: dict,
    item_info_cache: dict,
    existing_doc_id=None,
) -> list[dict]:
    """
    Process all rows for a single note group.
    Chunks items to GRN_ITEM_LIMIT (900) per voucher.
    Returns a list of doc result dicts (one per voucher/chunk).
    """
    # Pull common fields from the first non-empty row
    store_code  = rows[0].get("STORE_CODE", "").strip()
    store_name  = rows[0].get("STORE_NAME", "").strip()
    vendor_code = rows[0].get("VENDOR_CODE", "").strip()

    all_docs: list[dict] = []

    # Resolve store SIDs once for the note group
    storesid, sbssid = await _get_store_sids(store_code, store_cache, oc)
    if not storesid:
        err_doc = {
            "source_file": source_file,
            "note": note,
            "store_code": store_code,
            "store_name": store_name,
            "storesid": None,
            "sbssid": None,
            "vendsid": None,
            "vousid": None,
            "item_count": len(rows),
            "posted_count": 0,
            "error_count": len(rows),
            "status": "error",
            "error_message": f"Store SID not found in Oracle for store_code='{store_code}'",
            "items_data": [],
            "raw_rows": rows,
            "_existing_id": existing_doc_id,
        }
        await _persist_grn_doc(err_doc)
        return [err_doc]

    # Resolve vendor SID once for the note group
    vendsid = await _get_vendor_sid(vendor_code, vendor_cache, oc)
    if not vendsid:
        err_doc = {
            "source_file": source_file,
            "note": note,
            "store_code": store_code,
            "store_name": store_name,
            "storesid": storesid,
            "sbssid": sbssid,
            "vendsid": None,
            "vousid": None,
            "item_count": len(rows),
            "posted_count": 0,
            "error_count": len(rows),
            "status": "error",
            "error_message": f"Vendor SID not found in Oracle for vend_id='{vendor_code}'",
            "items_data": [],
            "raw_rows": rows,
            "_existing_id": existing_doc_id,
        }
        await _persist_grn_doc(err_doc)
        return [err_doc]

    # ── Pre-flight: validate every item before any API call ──────────────────
    items_detail: list[dict] = []
    preflight_has_error = False

    for row in rows:
        upc = row.get("UPC", "").strip()
        try:
            qty = int(float(row.get("TOTALQTY", "0") or "0"))
        except (ValueError, TypeError):
            qty = 0

        try:
            item_sid, item_active = await _get_item_info(upc, item_info_cache, oc)
        except Exception as _exc:
            item_sid, item_active = None, None
            logger.warning("[GRN] Oracle item lookup failed upc=%s: %s", upc, _exc)

        if item_sid is None:
            item_err = "Item not found in Oracle"
            preflight_has_error = True
        elif item_active == 0:
            item_err = "Item is inactive"
            preflight_has_error = True
        else:
            item_err = None

        items_detail.append({
            "upc":      upc,
            "qty":      qty,
            "item_sid": item_sid,
            "active":   item_active,
            "ok":       False,
            "error":    item_err,
        })

    if preflight_has_error:
        err_doc = {
            "source_file":   source_file,
            "note":          note,
            "store_code":    store_code,
            "store_name":    store_name,
            "storesid":      storesid,
            "sbssid":        sbssid,
            "vendsid":       vendsid,
            "vousid":        None,
            "item_count":    len(rows),
            "posted_count":  0,
            "error_count":   len(rows),
            "status":        "error",
            "error_message": (
                "One or more items in this group are not found or inactive in Oracle. "
                "Group was not processed — see item details for per-item errors."
            ),
            "items_data": items_detail,
            "raw_rows": rows,
            "_existing_id": existing_doc_id,
        }
        await _persist_grn_doc(err_doc)
        return [err_doc]

    # Fresh duplicate check immediately before any API call — last-resort guard against
    # race conditions where two imports both passed the bulk pre-load snapshot.
    fresh_check = await _batch_check_processed_notes([note], oc)
    if note in fresh_check:
        err_doc = {
            "source_file":   source_file,
            "note":          note,
            "store_code":    store_code,
            "store_name":    store_name,
            "storesid":      storesid,
            "sbssid":        sbssid,
            "vendsid":       vendsid,
            "vousid":        None,
            "item_count":    len(rows),
            "posted_count":  0,
            "error_count":   len(rows),
            "status":        "error",
            "error_message": (
                f"Note '{note}' was found in RetailPro at the point of posting — "
                "skipping to prevent duplicate GRN."
            ),
            "items_data":    [],
            "raw_rows":      rows,
            "_existing_id":  existing_doc_id,
        }
        logger.warning(
            "[GRN] Fresh duplicate check blocked posting for note='%s'.", note
        )
        await _persist_grn_doc(err_doc)
        return [err_doc]

    # All items valid — chunk into batches of GRN_ITEM_LIMIT
    chunks = [items_detail[i:i + GRN_ITEM_LIMIT] for i in range(0, len(items_detail), GRN_ITEM_LIMIT)]
    # Map each chunk back to corresponding source rows for retry support
    chunk_rows = [rows[i:i + GRN_ITEM_LIMIT] for i in range(0, len(rows), GRN_ITEM_LIMIT)]

    for chunk_idx, chunk in enumerate(chunks):
        doc_data: dict = {
            "source_file": source_file,
            "note": note,
            "store_code": store_code,
            "store_name": store_name,
            "storesid": storesid,
            "sbssid": sbssid,
            "vendsid": vendsid,
            "vousid": None,
            "item_count": len(chunk),
            "posted_count": 0,
            "error_count": 0,
            "status": "error",
            "error_message": None,
            "items_data": list(chunk),
            "raw_rows": chunk_rows[chunk_idx] if chunk_idx < len(chunk_rows) else rows,
            "_existing_id": existing_doc_id if len(chunks) == 1 else None,
        }

        try:
            # Step 1: Create GRN voucher (note is embedded directly in the payload)
            vousid, create_payload, create_resp = await _create_grn_doc(
                http, base_url, auth_session, storesid, sbssid or "", note=note
            )
            doc_data["api_create_payload"]  = {"_url": f"POST {base_url}/api/backoffice/receiving", **create_payload}
            doc_data["api_create_response"] = create_resp

            if not vousid:
                doc_data["error_message"] = f"No voucher SID in create response: {create_resp}"
                doc_data["error_count"] += len(chunk)
                await _persist_grn_doc(doc_data)
                all_docs.append(doc_data)
                continue

            doc_data["vousid"] = vousid

            # Step 2: Get rowversion
            rowversion, get_resp = await _get_grn_rowversion(http, base_url, auth_session, vousid)
            doc_data["api_get_rowversion_response"] = {"_url": f"GET {base_url}/api/backoffice/receiving?filter=(sid,eq,{vousid})", **get_resp}

            if rowversion is None:
                doc_data["error_message"] = f"Could not get rowversion: {get_resp}"
                doc_data["error_count"] += len(chunk)
                await _persist_grn_doc(doc_data)
                all_docs.append(doc_data)
                continue

            # Step 3: Set vendor
            vendor_payload, vendor_resp = await _update_grn_vendor(
                http, base_url, auth_session, vousid, rowversion, vendsid
            )
            doc_data["api_vendor_payload"]  = {"_url": f"PUT {base_url}/api/backoffice/receiving/{vousid}", **vendor_payload}
            doc_data["api_vendor_response"] = vendor_resp

            # Step 4: Post items
            items_payload, items_resp = await _post_grn_items(
                http, base_url, auth_session, vousid, chunk
            )
            doc_data["api_items_payload"]  = {"_url": f"POST {base_url}/api/backoffice/receiving/{vousid}/recvitem", **items_payload}
            doc_data["api_items_response"] = items_resp

            # Step 5: Get updated rowversion (comment step removed — note is in the create payload)
            rowversion2, get_resp2 = await _get_grn_rowversion(http, base_url, auth_session, vousid)
            doc_data["api_get_rowversion2_response"] = {"_url": f"GET {base_url}/api/backoffice/receiving?filter=(sid,eq,{vousid})", **get_resp2}

            if rowversion2 is None:
                doc_data["error_message"] = f"Could not get updated rowversion: {get_resp2}"
                doc_data["error_count"] += len(chunk)
                await _persist_grn_doc(doc_data)
                all_docs.append(doc_data)
                continue

            # Step 6: Finalize
            fin_payload, fin_resp = await _finalize_grn(
                http, base_url, auth_session, vousid, rowversion2
            )
            doc_data["api_finalize_payload"]  = {"_url": f"PUT {base_url}/api/backoffice/receiving/{vousid}", **fin_payload}
            doc_data["api_finalize_response"] = fin_resp

            # Mark chunk items as ok
            for item in doc_data["items_data"]:
                if item.get("item_sid"):
                    item["ok"] = True

            posted = sum(1 for it in doc_data["items_data"] if it.get("ok"))
            errors = len(doc_data["items_data"]) - posted

            doc_data.update({
                "posted_count": posted,
                "error_count":  errors,
                "status": "posted" if errors == 0 else ("partial" if posted > 0 else "error"),
            })

        except Exception as exc:
            logger.exception("Error processing GRN note='%s' chunk=%d", note, chunk_idx)
            doc_data["error_message"]   = str(exc)
            doc_data["error_traceback"] = traceback.format_exc()
            doc_data["error_count"]     = len(chunk)

        try:
            await _persist_grn_doc(doc_data)
        except Exception as db_exc:
            logger.error("[GRN] DB persist failed for note=%s chunk=%d: %s", note, chunk_idx, db_exc)

        logger.info(
            "[GRN] note=%s vousid=%s items=%d posted=%d errors=%d",
            note, doc_data.get("vousid"),
            doc_data["item_count"], doc_data["posted_count"], doc_data["error_count"],
        )
        all_docs.append(doc_data)

    return all_docs


# ── Duplicate-note guard ───────────────────────────────────────────────────────

async def _note_already_processed_in_retailpro(note: str, oc: dict) -> bool:
    """Single-note duplicate check (kept for fallback; prefer _batch_check_processed_notes)."""
    processed = await _batch_check_processed_notes([note], oc)
    return note in processed


# ── Main entry point ───────────────────────────────────────────────────────────

async def process_grn_csv(
    file_bytes: bytes,
    source_file: str = "grn.csv",
) -> dict:
    """
    Full GRN pipeline: parse CSV → group by note → process each note group.
    Supports cooperative cancellation via request_cancel_import().
    Returns a summary dict.
    """
    global _active_import_id, _cancel_requests

    # Guard: refuse a second concurrent import to eliminate duplicate-posting race conditions.
    if _active_import_id is not None:
        return {
            "ok": False,
            "cancelled": False,
            "error": (
                "A GRN import is already running "
                f"(import_id={_active_import_id}). "
                "Please wait for it to finish before uploading another file."
            ),
            "total_docs": 0,
            "posted_docs": 0,
            "partial_docs": 0,
            "error_docs": 0,
            "total_items": 0,
            "posted_items": 0,
        }

    from app.services.retailpro_auth import get_auth_session, sit_session, stand_session
    from app.db.settings_store import get_setting

    batch_started_at = now_pkt()
    rows = parse_grn_csv(file_bytes)
    if not rows:
        return {
            "ok": False,
            "cancelled": False,
            "error": "No data rows found (all rows missing UPC or NOTE).",
            "total_docs": 0,
            "posted_docs": 0,
            "partial_docs": 0,
            "error_docs": 0,
            "total_items": 0,
            "posted_items": 0,
        }

    oc = {
        "host":         (await get_setting("oracle_host"))         or "",
        "port":         int((await get_setting("oracle_port"))     or "1521"),
        "service_name": (await get_setting("oracle_service_name")) or "",
        "username":     (await get_setting("oracle_username"))     or "",
        "password":     (await get_setting("oracle_password"))     or "",
    }

    # Create Oracle connection pool (1 per concurrent group + 1 for pre-loads)
    from app.services.oracle_service import create_oracle_pool, close_oracle_pool
    try:
        oc["pool"] = await create_oracle_pool(
            oc["host"], oc["port"], oc["service_name"],
            oc["username"], oc["password"],
            min_size=1, max_size=5,
        )
        logger.info("[GRN] Oracle connection pool created.")
    except Exception as exc:
        logger.warning("[GRN] Oracle pool creation failed, using per-query connections: %s", exc)
        oc["pool"] = None

    base_url     = ((await get_setting("retailpro_base_url")) or "").rstrip("/")
    auth_session = await get_auth_session()
    await sit_session(base_url, auth_session)

    # Group rows by note
    note_groups: dict[str, list[dict]] = {}
    for row in rows:
        note = row.get("NOTE", "").strip()
        note_groups.setdefault(note, []).append(row)

    store_cache:     dict = {}
    vendor_cache:    dict = {}
    item_info_cache: dict = {}
    all_docs:        list[dict] = []

    # ── Pre-load caches (bulk Oracle queries before any processing) ───────────
    all_store_codes  = list({row.get("STORE_CODE", "").strip() for row in rows if row.get("STORE_CODE", "").strip()})
    all_vendor_codes = list({row.get("VENDOR_CODE", "").strip() for row in rows if row.get("VENDOR_CODE", "").strip()})
    all_upcs         = list({row.get("UPC", "").strip()         for row in rows if row.get("UPC", "").strip()})
    all_notes        = list(note_groups.keys())

    await _batch_load_store_sids(all_store_codes, store_cache, oc)
    await _batch_load_vendor_sids(all_vendor_codes, vendor_cache, oc)
    await _batch_load_item_info(all_upcs, item_info_cache, oc)
    already_processed_notes = await _batch_check_processed_notes(all_notes, oc)
    logger.info(
        "[GRN] Pre-loaded %d stores, %d vendors, %d UPCs; %d/%d notes already processed.",
        len(store_cache), len(vendor_cache), len(item_info_cache),
        len(already_processed_notes), len(all_notes),
    )

    import_id = str(_uuid.uuid4())
    _active_import_id = import_id
    cancelled = False

    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(connect=10.0, read=120.0, write=60.0, pool=10.0),
            verify=False,
            follow_redirects=True,
        ) as http:
            sem = asyncio.Semaphore(3)

            async def _process_one(note: str, note_rows: list[dict]) -> Optional[list[dict]]:
                if _is_cancelled(import_id):
                    return None

                if note in already_processed_notes:
                    store_code = note_rows[0].get("STORE_CODE", "").strip()
                    store_name = note_rows[0].get("STORE_NAME", "").strip()
                    dup_doc = {
                        "source_file": source_file,
                        "note": note,
                        "store_code": store_code,
                        "store_name": store_name,
                        "storesid": None, "sbssid": None, "vendsid": None, "vousid": None,
                        "item_count": len(note_rows),
                        "posted_count": 0,
                        "error_count": len(note_rows),
                        "status": "error",
                        "error_message": (
                            f"Note '{note}' has already been processed in RetailPro. "
                            "Skipping to avoid duplicate GRN."
                        ),
                        "items_data": [],
                    }
                    logger.warning("[GRN] Skipping note='%s' — already finalised in RetailPro.", note)
                    await _persist_grn_doc(dup_doc)
                    return [dup_doc]

                # Validate: every row must share the same Vendor Code
                vendor_codes_set = {str(r.get("VENDOR_CODE", "")).strip() for r in note_rows}
                vendor_codes_set.discard("")
                if len(vendor_codes_set) > 1:
                    store_code = note_rows[0].get("STORE_CODE", "").strip()
                    store_name = note_rows[0].get("STORE_NAME", "").strip()
                    err_doc = {
                        "source_file": source_file,
                        "note": note,
                        "store_code": store_code,
                        "store_name": store_name,
                        "storesid": None, "sbssid": None, "vendsid": None, "vousid": None,
                        "item_count": len(note_rows),
                        "posted_count": 0,
                        "error_count": len(note_rows),
                        "status": "error",
                        "error_message": (
                            f"Multiple vendors in GRN note='{note}': "
                            f"{', '.join(sorted(vendor_codes_set))}. "
                            "All rows in a note group must share the same Vendor Code."
                        ),
                        "items_data": [],
                    }
                    logger.warning("[GRN] Skipping note='%s' — multiple vendor codes: %s", note, vendor_codes_set)
                    await _persist_grn_doc(err_doc)
                    return [err_doc]

                async with sem:
                    if _is_cancelled(import_id):
                        return None
                    return await _process_note_group(
                        note=note,
                        rows=note_rows,
                        source_file=source_file,
                        base_url=base_url,
                        auth_session=auth_session,
                        http=http,
                        oc=oc,
                        store_cache=store_cache,
                        vendor_cache=vendor_cache,
                        item_info_cache=item_info_cache,
                    )

            note_items = list(note_groups.items())
            for idx, (note, note_rows) in enumerate(note_items):
                try:
                    r = await _process_one(note, note_rows)
                except Exception as exc:
                    r = exc
                if isinstance(r, Exception):
                    logger.error("[GRN] Unexpected error in task: %s", r)
                elif r is None:
                    cancelled = True
                else:
                    all_docs.extend(r)
                # Wait 30 s between documents to avoid overloading RetailPro
                if idx < len(note_items) - 1 and not _is_cancelled(import_id):
                    logger.info("[GRN] Waiting 30 s before next document (%d/%d)…", idx + 1, len(note_items))
                    await asyncio.sleep(30)
    finally:
        _active_import_id = None
        _cancel_requests.discard(import_id)
        try:
            await stand_session(base_url, auth_session)
        except Exception:
            pass
        if oc.get("pool") is not None:
            try:
                await close_oracle_pool(oc["pool"])
            except Exception as exc:
                logger.warning("[GRN] Failed to close Oracle pool: %s", exc)

    total_docs   = len(all_docs)
    posted_docs  = sum(1 for d in all_docs if d.get("status") == "posted")
    partial_docs = sum(1 for d in all_docs if d.get("status") == "partial")
    error_docs   = sum(1 for d in all_docs if d.get("status") == "error")
    total_items  = sum(d.get("item_count", 0) for d in all_docs)
    posted_items = sum(d.get("posted_count", 0) for d in all_docs)

    batch_completed_at = now_pkt()
    duration_seconds   = round((batch_completed_at - batch_started_at).total_seconds())

    return {
        "ok":               True,
        "cancelled":        cancelled,
        "total_rows":       len(rows),
        "total_docs":       total_docs,
        "posted_docs":      posted_docs,
        "partial_docs":     partial_docs,
        "error_docs":       error_docs,
        "total_items":      total_items,
        "posted_items":     posted_items,
        "started_at":       batch_started_at.strftime("%d-%b-%Y %H:%M:%S"),
        "completed_at":     batch_completed_at.strftime("%d-%b-%Y %H:%M:%S"),
        "duration_seconds": duration_seconds,
    }


# ── Retry ─────────────────────────────────────────────────────────────────────

async def retry_grn_doc(doc_id: str) -> dict:
    """Re-run the full pipeline for a single failed GRN doc."""
    import uuid as _uuid_mod
    from app.db.postgres import get_session
    from app.models.grn_doc import GRNDoc
    from app.services.retailpro_auth import get_auth_session
    from app.db.settings_store import get_setting

    try:
        oid = _uuid_mod.UUID(doc_id)
    except ValueError:
        raise ValueError("Invalid document ID.")

    async with get_session() as session:
        doc = await session.get(GRNDoc, oid)

    if not doc:
        raise ValueError("Document not found.")
    if doc.status == "posted":
        raise ValueError("Document is already posted.")
    if not doc.raw_rows:
        raise ValueError("No raw CSV data stored for this document — cannot retry.")

    base_url = ((await get_setting("retailpro_base_url")) or "").rstrip("/")
    auth_session = await get_auth_session()
    oc = {
        "host":         (await get_setting("oracle_host"))         or "",
        "port":         int((await get_setting("oracle_port"))     or "1521"),
        "service_name": (await get_setting("oracle_service_name")) or "",
        "username":     (await get_setting("oracle_username"))     or "",
        "password":     (await get_setting("oracle_password"))     or "",
    }

    # Duplicate check before retry — prevents re-posting an already-finalised note.
    already = await _batch_check_processed_notes([doc.note or ""], oc)
    if (doc.note or "") in already:
        raise ValueError(
            f"Note '{doc.note}' is already finalised in RetailPro (status=4). "
            "Retry blocked to prevent duplicate GRN."
        )

    async with httpx.AsyncClient(timeout=120) as http:
        results = await _process_note_group(
            note=doc.note or "",
            rows=doc.raw_rows,
            source_file=doc.source_file or "retry",
            base_url=base_url,
            auth_session=auth_session,
            http=http,
            oc=oc,
            store_cache={},
            vendor_cache={},
            item_info_cache={},
            existing_doc_id=oid,
        )
    return results[0] if results else {"status": "error", "error_message": "No result returned"}
