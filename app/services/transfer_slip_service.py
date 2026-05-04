"""
Transfer Slip import service.

Processing pipeline (per note-group):
  1. Parse CSV — extract Out Store Name, In Store Name, upc, note, totaltransferqty
  2. Skip rows missing upc or note
  3. Group rows by note field
  4. For each note-group:
     a. Resolve In Store SIDs from Oracle:
           SELECT sid, sbs_sid FROM rps.store WHERE store_code = '{in_store_name}'
           → instoresid (sid), insbssid (sbs_sid)
     b. Resolve Out Store SIDs from Oracle:
           SELECT sid, sbs_sid FROM rps.store WHERE store_code = '{out_store_name}'
           → outstoresid (sid), outsbssid (sbs_sid)
     c. POST /api/backoffice/transferslip            → slip_sid
     d. Resolve each item SID from Oracle:
           SELECT sid FROM rps.invn_sbs_item WHERE upc = '{upc}'
     e. POST /api/backoffice/transferslip/{slip_sid}/slipitem
     f. POST /api/backoffice/slipcomment?comments={note}&slipsid={slip_sid}
     g. GET  /api/backoffice/transferslip?filter=(sid,eq,{slip_sid})  → rowversion
     h. PUT  /api/backoffice/transferslip/{slip_sid}  → status 4
  5. Persist each transfer slip document to transfer_slip_docs table
"""
import csv
import io
import logging
import uuid as _uuid
from typing import Any, Optional

import httpx

from app.core.timezone import now_pkt

logger = logging.getLogger(__name__)

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
    logger.info("Cancel requested for Transfer Slip import %s", _active_import_id)
    return True


def _is_cancelled(import_id: str) -> bool:
    return import_id in _cancel_requests


# ── CSV parsing ───────────────────────────────────────────────────────────────

def parse_transfer_slip_csv(file_bytes: bytes) -> list[dict]:
    """
    Parse CSV bytes with multi-row headers.
    Scans lines to find the row containing 'note', 'upc', and 'totaltransferqty',
    then uses that as the header row and parses all subsequent non-empty rows.
    Returns list of normalised row dicts.
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
        if "note" in cols_lower and "upc" in cols_lower and "totaltransferqty" in cols_lower:
            header_idx = i
            break

    if header_idx is None:
        raise ValueError(
            "Could not find header row containing 'note', 'upc', and 'totaltransferqty' columns."
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


# ── Oracle helpers ────────────────────────────────────────────────────────────

async def _oracle_row(sql: str, oc: dict) -> Optional[tuple]:
    """Run a query and return the first row as a tuple, or None."""
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
    """
    Returns (sid, sbs_sid) for a store code.
    sid → instoresid / outstoresid
    sbs_sid → insbssid / outsbssid
    """
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


# ── RetailPro API calls ───────────────────────────────────────────────────────

def _rp_headers(auth_session: str) -> dict:
    return {
        "Accept": "application/json, version=2",
        "Auth-Session": auth_session,
        "Content-Type": "application/json",
    }


async def _create_transfer_slip(
    http: httpx.AsyncClient,
    base_url: str,
    auth_session: str,
    instoresid: str,
    insbssid: str,
    outstoresid: str,
    outsbssid: str,
) -> tuple[Optional[str], dict, dict]:
    """
    POST /api/backoffice/transferslip
    Returns (slip_sid, payload_sent, response_json).
    """
    payload = {
        "data": [{
            "originapplication": "RProPrismWeb",
            "status": 3,
            "insbssid": insbssid,
            "outsbssid": outsbssid,
            "instoresid": instoresid,
            "outstoresid": outstoresid,
            "Verified": True,
        }]
    }
    resp = await http.post(
        f"{base_url}/api/backoffice/transferslip",
        json=payload,
        headers=_rp_headers(auth_session),
    )
    try:
        resp_json = resp.json()
    except Exception:
        resp_json = {"raw": resp.text}

    data = resp_json.get("data") or [] if isinstance(resp_json, dict) else []
    slip_sid = data[0].get("sid") if data else None
    return slip_sid, payload, resp_json


async def _post_slip_items(
    http: httpx.AsyncClient,
    base_url: str,
    auth_session: str,
    slip_sid: str,
    items: list[dict],  # [{item_sid, qty, upc}]
) -> tuple[dict, dict]:
    """
    POST /api/backoffice/transferslip/{slip_sid}/slipitem
    Returns (payload_sent, response_json).
    """
    payload = {
        "data": [
            {
                "originapplication": "RProPrismWeb",
                "qty": item["qty"],
                "itemsid": item["item_sid"],
                "slipsid": slip_sid,
            }
            for item in items
            if item.get("item_sid")
        ]
    }
    resp = await http.post(
        f"{base_url}/api/backoffice/transferslip/{slip_sid}/slipitem",
        json=payload,
        headers=_rp_headers(auth_session),
    )
    try:
        resp_json = resp.json()
    except Exception:
        resp_json = {"raw": resp.text}
    return payload, resp_json


async def _post_slip_comment(
    http: httpx.AsyncClient,
    base_url: str,
    auth_session: str,
    slip_sid: str,
    note: str,
) -> tuple[dict, dict]:
    """
    POST /api/backoffice/slipcomment?comments={note}&slipsid={slip_sid}
    Returns (payload_sent, response_json).
    """
    payload = {
        "data": [{
            "originapplication": "RProPrismWeb",
            "slipsid": slip_sid,
            "comments": note,
        }]
    }
    resp = await http.post(
        f"{base_url}/api/backoffice/slipcomment",
        params={"comments": note, "slipsid": slip_sid},
        json=payload,
        headers=_rp_headers(auth_session),
    )
    try:
        resp_json = resp.json()
    except Exception:
        resp_json = {"raw": resp.text}
    return payload, resp_json


async def _get_slip_rowversion(
    http: httpx.AsyncClient,
    base_url: str,
    auth_session: str,
    slip_sid: str,
) -> tuple[Optional[int], dict]:
    """
    GET /api/backoffice/transferslip?filter=(sid,eq,{slip_sid})
    Returns (rowversion, response_json).
    """
    resp = await http.get(
        f"{base_url}/api/backoffice/transferslip",
        params={"filter": f"(sid,eq,{slip_sid})"},
        headers=_rp_headers(auth_session),
    )
    try:
        resp_json = resp.json()
    except Exception:
        resp_json = {"raw": resp.text}

    data = resp_json.get("data") or [] if isinstance(resp_json, dict) else []
    rowversion = data[0].get("rowversion") if data else None
    return rowversion, resp_json


async def _finalize_slip(
    http: httpx.AsyncClient,
    base_url: str,
    auth_session: str,
    slip_sid: str,
    rowversion: int,
) -> tuple[dict, dict]:
    """
    PUT /api/backoffice/transferslip/{slip_sid}
    Returns (payload_sent, response_json).
    """
    payload = {"data": [{"rowversion": rowversion, "status": 4}]}
    resp = await http.put(
        f"{base_url}/api/backoffice/transferslip/{slip_sid}",
        json=payload,
        headers=_rp_headers(auth_session),
    )
    try:
        resp_json = resp.json()
    except Exception:
        resp_json = {"raw": resp.text}
    return payload, resp_json


# ── DB persistence ────────────────────────────────────────────────────────────

async def _persist_slip_doc(doc_data: dict) -> None:
    from app.db.postgres import get_session
    from app.models.transfer_slip_doc import TransferSlipDoc

    async with get_session() as session:
        async with session.begin():
            doc = TransferSlipDoc(
                id=_uuid.uuid4(),
                source_file=doc_data.get("source_file"),
                note=doc_data.get("note"),
                in_store_name=doc_data.get("in_store_name"),
                out_store_name=doc_data.get("out_store_name"),
                instoresid=doc_data.get("instoresid"),
                insbssid=doc_data.get("insbssid"),
                outstoresid=doc_data.get("outstoresid"),
                outsbssid=doc_data.get("outsbssid"),
                slip_sid=doc_data.get("slip_sid"),
                item_count=doc_data.get("item_count", 0),
                posted_count=doc_data.get("posted_count", 0),
                error_count=doc_data.get("error_count", 0),
                status=doc_data.get("status", "pending"),
                error_message=doc_data.get("error_message"),
                api_create_payload=doc_data.get("api_create_payload"),
                api_create_response=doc_data.get("api_create_response"),
                api_items_payload=doc_data.get("api_items_payload"),
                api_items_response=doc_data.get("api_items_response"),
                api_comment_payload=doc_data.get("api_comment_payload"),
                api_comment_response=doc_data.get("api_comment_response"),
                api_get_response=doc_data.get("api_get_response"),
                api_finalize_payload=doc_data.get("api_finalize_payload"),
                api_finalize_response=doc_data.get("api_finalize_response"),
                items_data=doc_data.get("items_data"),
                posted_at=now_pkt() if doc_data.get("status") == "posted" else None,
            )
            session.add(doc)


# ── Note-group processor ──────────────────────────────────────────────────────

async def _process_note_group(
    note: str,
    rows: list[dict],
    source_file: str,
    base_url: str,
    auth_session: str,
    http: httpx.AsyncClient,
    oc: dict,
    store_sids_cache: dict,
    item_info_cache: dict,
) -> dict:
    """
    Process all rows for a single note group (one transfer slip document).
    Returns the doc result dict.
    """
    in_store_name  = rows[0].get("IN_STORE_NAME",  "").strip()
    out_store_name = rows[0].get("OUT_STORE_NAME", "").strip()

    doc_data: dict = {
        "source_file":   source_file,
        "note":          note,
        "in_store_name": in_store_name,
        "out_store_name": out_store_name,
        "instoresid":    None,
        "insbssid":      None,
        "outstoresid":   None,
        "outsbssid":     None,
        "slip_sid":      None,
        "item_count":    len(rows),
        "posted_count":  0,
        "error_count":   0,
        "status":        "error",
        "error_message": None,
        "items_data":    [],
    }

    try:
        # ── Step 1: Resolve In Store SIDs ────────────────────────────────────
        instoresid, insbssid = await _get_store_sids(in_store_name, store_sids_cache, oc)
        doc_data["instoresid"] = instoresid
        doc_data["insbssid"]   = insbssid

        if not instoresid:
            doc_data["error_message"] = (
                f"In Store SID not found in Oracle for store_code='{in_store_name}'"
            )
            doc_data["error_count"] = len(rows)
            await _persist_slip_doc(doc_data)
            return doc_data

        # ── Step 2: Resolve Out Store SIDs ───────────────────────────────────
        outstoresid, outsbssid = await _get_store_sids(out_store_name, store_sids_cache, oc)
        doc_data["outstoresid"] = outstoresid
        doc_data["outsbssid"]   = outsbssid

        if not outstoresid:
            doc_data["error_message"] = (
                f"Out Store SID not found in Oracle for store_code='{out_store_name}'"
            )
            doc_data["error_count"] = len(rows)
            await _persist_slip_doc(doc_data)
            return doc_data

        # ── Pre-flight: validate every item before any API call ───────────────
        preflight_items: list[dict] = []
        preflight_has_error = False

        for row in rows:
            upc = row.get("UPC", "").strip()
            try:
                qty = int(float(row.get("TOTALTRANSFERQTY", "0") or "0"))
            except (ValueError, TypeError):
                qty = 0

            try:
                item_sid, item_active = await _get_item_info(upc, item_info_cache, oc)
            except Exception as _exc:
                item_sid, item_active = None, None
                logger.warning("[TransferSlip] Oracle item lookup failed upc=%s: %s", upc, _exc)

            if item_sid is None:
                item_err = "Item not found in Oracle"
                preflight_has_error = True
            elif item_active == 0:
                item_err = "Item is inactive"
                preflight_has_error = True
            else:
                item_err = None

            preflight_items.append({
                "upc":      upc,
                "qty":      qty,
                "item_sid": item_sid,
                "active":   item_active,
                "ok":       False,
                "error":    item_err,
            })

        if preflight_has_error:
            doc_data["items_data"]    = preflight_items
            doc_data["error_count"]   = len(preflight_items)
            doc_data["error_message"] = (
                "One or more items in this group are not found or inactive in Oracle. "
                "Group was not processed — see item details for per-item errors."
            )
            await _persist_slip_doc(doc_data)
            return doc_data

        # ── Step 3: Create transfer slip document ─────────────────────────────
        slip_sid, create_payload, create_resp = await _create_transfer_slip(
            http, base_url, auth_session,
            instoresid, insbssid or "",
            outstoresid, outsbssid or "",
        )
        doc_data["api_create_payload"]  = create_payload
        doc_data["api_create_response"] = create_resp

        if not slip_sid:
            doc_data["error_message"]  = f"No slip sid in create response: {create_resp}"
            doc_data["error_count"]    = len(rows)
            await _persist_slip_doc(doc_data)
            return doc_data

        doc_data["slip_sid"] = slip_sid

        # ── Step 4: Build items list (all already validated via pre-flight) ──────
        # Cache hits only — no Oracle round-trips here.
        items_for_api: list[dict] = []
        items_detail:  list[dict] = []

        for row in rows:
            upc = row.get("UPC", "").strip()
            try:
                qty = int(float(row.get("TOTALTRANSFERQTY", "0") or "0"))
            except (ValueError, TypeError):
                qty = 0

            item_sid, _ = await _get_item_info(upc, item_info_cache, oc)

            items_detail.append({
                "upc":      upc,
                "qty":      qty,
                "item_sid": item_sid,
                "ok":       False,
                "error":    None,
            })
            items_for_api.append({"item_sid": item_sid, "qty": qty, "upc": upc})

        doc_data["items_data"] = items_detail

        # ── Step 5: Post slip items ───────────────────────────────────────────
        items_payload, items_resp = await _post_slip_items(
            http, base_url, auth_session, slip_sid, items_for_api
        )
        doc_data["api_items_payload"]  = items_payload
        doc_data["api_items_response"] = items_resp

        # ── Step 6: Post comment (note) ───────────────────────────────────────
        comment_payload, comment_resp = await _post_slip_comment(
            http, base_url, auth_session, slip_sid, note
        )
        doc_data["api_comment_payload"]  = comment_payload
        doc_data["api_comment_response"] = comment_resp

        # ── Step 7: Get rowversion ────────────────────────────────────────────
        rowversion, get_resp = await _get_slip_rowversion(
            http, base_url, auth_session, slip_sid
        )
        doc_data["api_get_response"] = get_resp

        if rowversion is None:
            doc_data["error_message"] = (
                f"Could not get rowversion from GET response: {get_resp}"
            )
            doc_data["error_count"] = len(rows)
            await _persist_slip_doc(doc_data)
            return doc_data

        # ── Step 8: Finalize (status → 4) ────────────────────────────────────
        fin_payload, fin_resp = await _finalize_slip(
            http, base_url, auth_session, slip_sid, rowversion
        )
        doc_data["api_finalize_payload"]  = fin_payload
        doc_data["api_finalize_response"] = fin_resp

        # Mark items that were resolved as ok
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
        logger.exception("Error processing note group '%s'", note)
        doc_data["error_message"] = str(exc)
        doc_data["error_count"]   = len(rows)

    try:
        await _persist_slip_doc(doc_data)
    except Exception as db_exc:
        logger.error("[TransferSlip] Failed to persist doc for note=%s: %s", note, db_exc)

    logger.info(
        "[TransferSlip] note=%s slip_sid=%s items=%d posted=%d errors=%d",
        note, doc_data.get("slip_sid"),
        doc_data["item_count"], doc_data["posted_count"], doc_data["error_count"],
    )
    return doc_data


# ── Duplicate-note guard ──────────────────────────────────────────────────────

async def _note_already_processed(note: str) -> bool:
    """Return True if *note* exists in transfer_slip_docs (any previous batch)."""
    from app.db.postgres import get_session
    from app.models.transfer_slip_doc import TransferSlipDoc
    from sqlalchemy import select

    async with get_session() as session:
        result = await session.execute(
            select(TransferSlipDoc.id).where(TransferSlipDoc.note == note).limit(1)
        )
        return result.first() is not None


# ── Main entry point ──────────────────────────────────────────────────────────

async def process_transfer_slip_csv(
    file_bytes: bytes,
    source_file: str = "transfer_slip.csv",
) -> dict:
    """
    Full pipeline: parse CSV → group by note → process each note group.
    Supports cooperative cancellation via request_cancel_import().
    Returns a summary dict.
    """
    global _active_import_id, _cancel_requests

    from app.services.retailpro_auth import get_auth_session, sit_session, stand_session
    from app.db.settings_store import get_setting

    rows = parse_transfer_slip_csv(file_bytes)
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

    base_url     = ((await get_setting("retailpro_base_url")) or "").rstrip("/")
    auth_session = await get_auth_session()
    await sit_session(base_url, auth_session)

    # Group rows by note
    note_groups:   dict[str, list[dict]] = {}
    for row in rows:
        note = row.get("NOTE", "").strip()
        note_groups.setdefault(note, []).append(row)

    store_sids_cache: dict = {}
    item_info_cache:  dict = {}
    all_docs: list[dict]   = []

    import_id = str(_uuid.uuid4())
    _active_import_id = import_id
    cancelled = False

    try:
        async with httpx.AsyncClient(timeout=30.0, verify=False, follow_redirects=True) as http:
            for note, note_rows in note_groups.items():
                if _is_cancelled(import_id):
                    cancelled = True
                    logger.info("[TransferSlip] Import %s cancelled after note=%s", import_id, note)
                    break

                if await _note_already_processed(note):
                    logger.warning("Skipping duplicate note: %s", note)
                    dup_doc: dict = {
                        "source_file":  source_file,
                        "note":         note,
                        "in_store_name":  note_rows[0].get("IN_STORE_NAME", "").strip(),
                        "out_store_name": note_rows[0].get("OUT_STORE_NAME", "").strip(),
                        "item_count":   len(note_rows),
                        "posted_count": 0,
                        "error_count":  len(note_rows),
                        "status":       "error",
                        "error_message": f'"{note}" has already been processed',
                    }
                    await _persist_slip_doc(dup_doc)
                    all_docs.append(dup_doc)
                    continue

                doc = await _process_note_group(
                    note=note,
                    rows=note_rows,
                    source_file=source_file,
                    base_url=base_url,
                    auth_session=auth_session,
                    http=http,
                    oc=oc,
                    store_sids_cache=store_sids_cache,
                    item_info_cache=item_info_cache,
                )
                all_docs.append(doc)
    finally:
        _active_import_id = None
        _cancel_requests.discard(import_id)
        await stand_session(base_url, auth_session)

    total_docs   = len(all_docs)
    posted_docs  = sum(1 for d in all_docs if d.get("status") == "posted")
    partial_docs = sum(1 for d in all_docs if d.get("status") == "partial")
    error_docs   = sum(1 for d in all_docs if d.get("status") == "error")
    total_items  = sum(d.get("item_count", 0) for d in all_docs)
    posted_items = sum(d.get("posted_count", 0) for d in all_docs)

    return {
        "ok":           True,
        "cancelled":    cancelled,
        "total_rows":   len(rows),
        "total_docs":   total_docs,
        "posted_docs":  posted_docs,
        "partial_docs": partial_docs,
        "error_docs":   error_docs,
        "total_items":  total_items,
        "posted_items": posted_items,
    }
