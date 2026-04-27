"""
Price Adjustment import service.

Processing pipeline:
  1. Parse CSV — extract Store Code, scanupc, adjvalue, Price Level Name columns
  2. Skip blank rows (no scanupc)
  3. Group rows by Store Code
  4. For each store:
     a. Resolve store SID:        SELECT sid FROM rps.store WHERE store_Code = ?
     b. Resolve sbs SID:          SELECT sid FROM rps.subsidiary WHERE sbs_no = 1
     c. Resolve price level SID:  SELECT sid FROM rps.price_level WHERE price_lvl = ?
        (default price_lvl = 2; use PRICE_LEVEL_NAME from CSV if present)
  5. Chunk rows into batches of 900
  6. For each batch:
     a. POST /api/backoffice/adjustment            → adj_sid  (adjtype=1, pricelvlsid added)
     b. Resolve each item SID from Oracle:
           SELECT sid FROM rps.invn_Sbs_item WHERE upc = ?
     c. POST /api/backoffice/adjustment/{adj_sid}/adjitem  (adjvalue = price from CSV)
     d. GET  /api/backoffice/adjustment?filter=(sid,eq,{adj_sid})  → rowversion
     e. PUT  /api/backoffice/adjustment/{adj_sid}  → status 4
  7. Persist each document to price_adjustment_docs table
"""
import csv
import io
import logging
import uuid as _uuid
from typing import Any, Optional

import httpx

from app.core.timezone import now_pkt

logger = logging.getLogger(__name__)

BATCH_SIZE = 900
DEFAULT_PRICE_LEVEL = 2

# ── In-memory cancel state ────────────────────────────────────────────────────
_active_import_id: Optional[str] = None
_cancel_requests: set = set()


def get_active_import_id() -> Optional[str]:
    return _active_import_id


def request_cancel_import() -> bool:
    """Signal the running import to stop after the current store/batch finishes."""
    global _active_import_id
    if not _active_import_id:
        return False
    _cancel_requests.add(_active_import_id)
    logger.info("Cancel requested for price adjustment import %s", _active_import_id)
    return True


def _is_import_cancelled(import_id: str) -> bool:
    return import_id in _cancel_requests


# ── CSV parsing ──────────────────────────────────────────────────────────────

def parse_price_adjustment_csv(file_bytes: bytes) -> list[dict]:
    """
    Parse CSV bytes. Returns list of row dicts with normalised uppercase keys.
    Rows without a SCANUPC value are skipped.
    Handles comma-formatted numbers in ADJVALUE (e.g. "-3,600.00").
    """
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            text = file_bytes.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise ValueError("Could not decode CSV (tried utf-8, latin-1).")

    reader = csv.DictReader(io.StringIO(text))
    rows_out: list[dict] = []
    for row in reader:
        normalised = {
            k.strip().upper().replace(" ", "_"): str(v).strip()
            for k, v in row.items() if k
        }
        upc = normalised.get("SCANUPC", "").strip()
        if not upc:
            continue
        rows_out.append(normalised)
    return rows_out


def _parse_adjvalue(raw: str) -> float:
    """Parse adjvalue which may contain commas as thousands separator e.g. '-3,600.00'."""
    cleaned = raw.replace(",", "").strip()
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return 0.0


# ── Oracle helpers ────────────────────────────────────────────────────────────

async def _oracle_scalar(sql: str, oc: dict) -> Optional[str]:
    from app.services.oracle_service import run_query
    df = await run_query(
        oc["host"], oc["port"], oc["service_name"], oc["username"], oc["password"], sql
    )
    if df is None or df.is_empty():
        return None
    return str(df.row(0)[0])


async def _get_store_sid(store_code: str, cache: dict, oc: dict) -> Optional[str]:
    key = str(store_code).strip()
    if key not in cache:
        cache[key] = await _oracle_scalar(
            f"SELECT sid FROM rps.store WHERE store_Code = {key}", oc
        )
    return cache[key]


async def _get_sbs_sid(cache: dict, oc: dict) -> Optional[str]:
    if "__sbs1__" not in cache:
        cache["__sbs1__"] = await _oracle_scalar(
            "SELECT sid FROM rps.subsidiary WHERE sbs_no = 1", oc
        )
    return cache["__sbs1__"]


async def _get_price_lvl_sid(price_lvl: int, cache: dict, oc: dict) -> Optional[str]:
    key = f"__pricelvl_{price_lvl}__"
    if key not in cache:
        cache[key] = await _oracle_scalar(
            f"SELECT sid FROM rps.price_level WHERE price_lvl = {price_lvl}", oc
        )
    return cache[key]


async def _get_item_sid(upc: str, cache: dict, oc: dict) -> Optional[str]:
    key = str(upc).strip()
    if key not in cache:
        cache[key] = await _oracle_scalar(
            f"SELECT sid FROM rps.invn_Sbs_item WHERE upc = '{key}'", oc
        )
    return cache[key]


async def _get_adj_reason_sid(oc: dict) -> Optional[str]:
    return await _oracle_scalar(
        "SELECT SID FROM RPS.PREF_REASON WHERE name = 'MANUALLY'", oc
    )


# ── RetailPro API calls ───────────────────────────────────────────────────────

def _rp_headers(auth_session: str) -> dict:
    return {
        "Accept": "application/json, version=2",
        "Auth-Session": auth_session,
        "Content-Type": "application/json",
    }


async def _create_price_adjustment_doc(
    http: httpx.AsyncClient,
    base_url: str,
    auth_session: str,
    store_sid: str,
    sbs_sid: str,
    price_lvl_sid: str,
    adj_reason_sid: Optional[str] = None,
) -> tuple[Optional[str], dict, dict]:
    """POST /api/backoffice/adjustment with adjtype=1 and pricelvlsid."""
    record: dict = {
        "originapplication": "rProPrismWeb",
        "status": 3,
        "adjtype": 1,
        "sbssid": sbs_sid,
        "creatingdoctype": 8,
        "origstoresid": store_sid,
        "reasonname": "MANUALLY",
        "storesid": store_sid,
        "pricelvlsid": price_lvl_sid,
    }
    if adj_reason_sid:
        record["adjreasonsid"] = adj_reason_sid
    payload = {"data": [record]}
    resp = await http.post(
        f"{base_url}/api/backoffice/adjustment",
        json=payload,
        headers=_rp_headers(auth_session),
    )
    try:
        resp_json = resp.json()
    except Exception:
        resp_json = {"raw": resp.text}

    data = resp_json.get("data") if isinstance(resp_json, dict) else None
    if not isinstance(data, list):
        data = []

    # Non-empty data array means the create call succeeded.
    # Extract adj_sid from data[0]["sid"]; convert to str to handle numeric SIDs.
    raw_sid = data[0].get("sid") if data else None
    adj_sid = str(raw_sid) if raw_sid is not None else None
    return adj_sid, payload, resp_json


async def _post_price_adj_items(
    http: httpx.AsyncClient,
    base_url: str,
    auth_session: str,
    adj_sid: str,
    items: list[dict],
) -> tuple[dict, dict, int]:
    """POST /api/backoffice/adjustment/{adj_sid}/adjitem with price adjvalue.
    Returns (payload_sent, response_json, http_status_code).
    """
    payload = {
        "data": [
            {
                "originapplication": "RProPrismWeb",
                "adjsid": adj_sid,
                "itemsid": item["item_sid"],
                "itempos": 1,
                "adjvalue": item["adj_value"],
            }
            for item in items
            if item.get("item_sid")
        ]
    }
    resp = await http.post(
        f"{base_url}/api/backoffice/adjustment/{adj_sid}/adjitem",
        json=payload,
        headers=_rp_headers(auth_session),
    )
    try:
        resp_json = resp.json()
    except Exception:
        resp_json = {"raw": resp.text}
    return payload, resp_json, resp.status_code


async def _get_adjustment_rowversion(
    http: httpx.AsyncClient,
    base_url: str,
    auth_session: str,
    adj_sid: str,
) -> tuple[Optional[int], dict]:
    resp = await http.get(
        f"{base_url}/api/backoffice/adjustment",
        params={"filter": f"(sid,eq,{adj_sid})"},
        headers=_rp_headers(auth_session),
    )
    try:
        resp_json = resp.json()
    except Exception:
        resp_json = {"raw": resp.text}

    data = resp_json.get("data") if isinstance(resp_json, dict) else None
    if not isinstance(data, list):
        data = []

    # Non-empty data array means the record was found; rowversion may be int or str.
    raw_rv = data[0].get("rowversion") if data else None
    rowversion = int(raw_rv) if raw_rv is not None else None
    return rowversion, resp_json


async def _finalize_adjustment(
    http: httpx.AsyncClient,
    base_url: str,
    auth_session: str,
    adj_sid: str,
    rowversion: int,
) -> tuple[dict, dict, int]:
    """PUT /api/backoffice/adjustment/{adj_sid} → status 4.
    Returns (payload_sent, response_json, http_status_code).
    """
    payload = {"data": [{"rowversion": rowversion, "status": 4}]}
    resp = await http.put(
        f"{base_url}/api/backoffice/adjustment/{adj_sid}",
        json=payload,
        headers=_rp_headers(auth_session),
    )
    try:
        resp_json = resp.json()
    except Exception:
        resp_json = {"raw": resp.text}
    return payload, resp_json, resp.status_code


async def _post_price_adj_comment(
    http: httpx.AsyncClient,
    base_url: str,
    auth_session: str,
    adj_sid: str,
    note: str,
) -> tuple[dict, dict]:
    """POST /api/backoffice/adjustment/{adj_sid}/adjcomment with note as comment."""
    payload = {
        "data": [{
            "originapplication": "RProPrismWeb",
            "adjsid": adj_sid,
            "comments": note,
        }]
    }
    resp = await http.post(
        f"{base_url}/api/backoffice/adjustment/{adj_sid}/adjcomment",
        params={"comments": note},
        json=payload,
        headers=_rp_headers(auth_session),
    )
    try:
        resp_json = resp.json()
    except Exception:
        resp_json = {"raw": resp.text}
    return payload, resp_json


# ── DB persistence ────────────────────────────────────────────────────────────

async def _persist_price_adj_doc(doc_data: dict) -> None:
    from app.db.postgres import get_session
    from app.models.price_adjustment_doc import PriceAdjustmentDoc

    async with get_session() as session:
        async with session.begin():
            doc = PriceAdjustmentDoc(
                id=_uuid.uuid4(),
                source_file=doc_data.get("source_file"),
                store_code=doc_data.get("store_code"),
                store_name=doc_data.get("store_name"),
                store_sid=doc_data.get("store_sid"),
                sbs_sid=doc_data.get("sbs_sid"),
                adj_sid=doc_data.get("adj_sid"),
                note=doc_data.get("note"),
                price_lvl_sid=doc_data.get("price_lvl_sid"),
                item_count=doc_data.get("item_count", 0),
                posted_count=doc_data.get("posted_count", 0),
                error_count=doc_data.get("error_count", 0),
                status=doc_data.get("status", "pending"),
                error_message=doc_data.get("error_message"),
                api_create_payload=doc_data.get("api_create_payload"),
                api_create_response=doc_data.get("api_create_response"),
                api_items_payload=doc_data.get("api_items_payload"),
                api_items_response=doc_data.get("api_items_response"),
                api_get_response=doc_data.get("api_get_response"),
                api_finalize_payload=doc_data.get("api_finalize_payload"),
                api_finalize_response=doc_data.get("api_finalize_response"),
                api_comment_payload=doc_data.get("api_comment_payload"),
                api_comment_response=doc_data.get("api_comment_response"),
                items_data=doc_data.get("items_data"),
                posted_at=now_pkt() if doc_data.get("status") == "posted" else None,
            )
            session.add(doc)


# ── Already-processed guard ────────────────────────────────────────────────────

async def _is_price_note_processed(note: str) -> bool:
    """Return True if a doc with this note was already posted or partially posted."""
    from app.db.postgres import get_session
    from app.models.price_adjustment_doc import PriceAdjustmentDoc
    from sqlalchemy import select

    async with get_session() as session:
        result = await session.execute(
            select(PriceAdjustmentDoc.id).where(
                PriceAdjustmentDoc.note == note,
                PriceAdjustmentDoc.status.in_(["posted", "partial"]),
            ).limit(1)
        )
        return result.scalar() is not None


# ── Note batch processor ───────────────────────────────────────────────────────

async def _process_note_batch(
    note: str,
    rows: list[dict],
    source_file: str,
    base_url: str,
    auth_session: str,
    http: httpx.AsyncClient,
    oc: dict,
    store_sid_cache: dict,
    sbs_sid_cache: dict,
    price_lvl_sid_cache: dict,
    item_sid_cache: dict,
    adj_reason_sid: Optional[str] = None,
) -> dict:
    """Process all rows for a single NOTE as one adjustment document."""
    store_code = str(rows[0].get("STORE_CODE", "")).strip() or "1"
    store_name = str(rows[0].get("STORE_NAME", "")).strip()

    # Determine price level from first row
    price_lvl_raw = rows[0].get("PRICE_LEVEL_NAME", "").strip()
    try:
        price_lvl = int(float(price_lvl_raw)) if price_lvl_raw else DEFAULT_PRICE_LEVEL
    except (ValueError, TypeError):
        price_lvl = DEFAULT_PRICE_LEVEL

    price_lvl_sid = await _get_price_lvl_sid(price_lvl, price_lvl_sid_cache, oc)
    store_sid = await _get_store_sid(store_code, store_sid_cache, oc)
    sbs_sid = await _get_sbs_sid(sbs_sid_cache, oc)

    doc_data: dict = {
        "source_file": source_file,
        "store_code": store_code,
        "store_name": store_name,
        "note": note,
        "store_sid": store_sid,
        "sbs_sid": sbs_sid,
        "adj_sid": None,
        "price_lvl_sid": price_lvl_sid,
        "item_count": len(rows),
        "posted_count": 0,
        "error_count": 0,
        "status": "error",
        "error_message": None,
        "items_data": [],
    }
    items_detail: list[dict] = []  # kept in outer scope for crash-recovery in except

    # Guard: skip notes that were already successfully processed
    if note:
        try:
            if await _is_price_note_processed(note):
                logger.info("[PriceAdj] Note '%s' already processed — skipping", note)
                doc_data["error_message"] = f"{note} already processed"
                doc_data["error_count"] = len(rows)
                await _persist_price_adj_doc(doc_data)
                return doc_data
        except Exception as guard_exc:
            logger.warning("[PriceAdj] Could not check duplicate note '%s': %s", note, guard_exc)

    if not store_sid:
        logger.warning("No store SID for store_code=%s — skipping %d rows", store_code, len(rows))
        doc_data["error_message"] = f"Store SID not found in Oracle for store_code={store_code}"
        doc_data["error_count"] = len(rows)
        await _persist_price_adj_doc(doc_data)
        return doc_data

    try:
        # Step 1: Create adjustment document (adjtype=1, pricelvlsid)
        adj_sid, create_payload, create_resp = await _create_price_adjustment_doc(
            http, base_url, auth_session,
            store_sid, sbs_sid or "", price_lvl_sid or "",
            adj_reason_sid=adj_reason_sid,
        )
        doc_data["api_create_payload"] = create_payload
        doc_data["api_create_response"] = create_resp

        _resp_data = create_resp.get("data") if isinstance(create_resp, dict) else None
        _data_ok = isinstance(_resp_data, list) and len(_resp_data) > 0
        if not _data_ok or adj_sid is None:
            _api_errors = None
            if isinstance(create_resp, dict):
                _api_errors = create_resp.get("errors") or create_resp.get("error") or create_resp.get("message")
            if _data_ok and adj_sid is None:
                _reason = "data array non-empty but 'sid' missing in data[0]"
            elif _api_errors:
                _reason = f"RetailPro error: {_api_errors}"
            else:
                _reason = "Create adjustment returned empty data array (no record was created)"
            doc_data["error_message"] = (
                f"[Step 1 failed] {_reason}. "
                f"store_sid={doc_data.get('store_sid')} sbs_sid={doc_data.get('sbs_sid')} "
                f"price_lvl_sid={doc_data.get('price_lvl_sid')}. "
                f"Full response → see Step 1 Create Adjustment Response in API trace."
            )
            doc_data["error_count"] = len(rows)
            await _persist_price_adj_doc(doc_data)
            return doc_data

        doc_data["adj_sid"] = adj_sid

        # Step 2: Resolve item SIDs from Oracle
        items_for_api: list[dict] = []
        items_detail.clear()

        for row in rows:
            upc = row.get("SCANUPC", "").strip()
            adj_value = _parse_adjvalue(row.get("ADJVALUE", "0") or "0")

            try:
                item_sid = await _get_item_sid(upc, item_sid_cache, oc)
                item_err = None if item_sid else "Item SID not found in Oracle"
            except Exception as oracle_exc:
                item_sid = None
                item_err = f"Oracle error: {oracle_exc}"
                logger.warning("[PriceAdj] Oracle SID lookup failed upc=%s: %s", upc, oracle_exc)

            item_detail = {
                "upc": upc,
                "adj_value": adj_value,
                "item_sid": item_sid,
                "ok": False,
                "error": item_err,
            }
            items_detail.append(item_detail)

            if item_sid:
                items_for_api.append({
                    "item_sid": item_sid,
                    "adj_value": adj_value,
                    "upc": upc,
                })

        doc_data["items_data"] = items_detail

        if not items_for_api:
            oracle_errors = [it["error"] for it in items_detail if it.get("error")]
            first_err = oracle_errors[0] if oracle_errors else "unknown"
            doc_data["error_message"] = (
                f"[Step 2 failed] No item SIDs resolved for any of the {len(rows)} UPCs. "
                f"First error: {first_err}"
            )
            doc_data["error_count"] = len(rows)
            await _persist_price_adj_doc(doc_data)
            return doc_data

        # Step 3: Post adj items (price values)
        items_payload, items_resp, items_status = await _post_price_adj_items(
            http, base_url, auth_session, adj_sid, items_for_api
        )
        doc_data["api_items_payload"] = items_payload
        doc_data["api_items_response"] = items_resp

        _items_ok = items_status < 400
        if _items_ok and isinstance(items_resp, dict):
            _items_errors = items_resp.get("errors") or items_resp.get("error")
            if _items_errors:
                _items_ok = False

        if not _items_ok:
            _items_err_detail = None
            if isinstance(items_resp, dict):
                _items_err_detail = items_resp.get("errors") or items_resp.get("error")
            doc_data["error_message"] = (
                f"[Step 3 failed] POST adjitems returned HTTP {items_status}. "
                + (f"RetailPro error: {_items_err_detail}. " if _items_err_detail else "")
                + "Full response → see Step 3 POST Items Response in API trace."
            )
            doc_data["error_count"] = len(rows)
            logger.warning(
                "[PriceAdj] POST adjitems failed note='%s' adj_sid=%s HTTP %d",
                note, adj_sid, items_status,
            )
            await _persist_price_adj_doc(doc_data)
            return doc_data

        # Step 4: Get rowversion
        rowversion, get_resp = await _get_adjustment_rowversion(
            http, base_url, auth_session, adj_sid
        )
        doc_data["api_get_response"] = get_resp

        if rowversion is None:
            _get_errors = None
            if isinstance(get_resp, dict):
                _get_errors = get_resp.get("errors") or get_resp.get("error") or get_resp.get("message")
            _get_data = get_resp.get("data") if isinstance(get_resp, dict) else None
            if isinstance(_get_data, list) and len(_get_data) == 0:
                _rv_reason = f"GET adjustment returned empty data (adj_sid={adj_sid} not found)"
            elif _get_errors:
                _rv_reason = f"RetailPro error: {_get_errors}"
            else:
                _rv_reason = "'rowversion' field missing or null in GET response"
            doc_data["error_message"] = (
                f"[Step 3 failed] {_rv_reason}. "
                f"Full response → see Step 3 GET Rowversion Response in API trace."
            )
            doc_data["error_count"] = len(rows)
            await _persist_price_adj_doc(doc_data)
            return doc_data

        # Step 5: Finalize (status → 4)
        fin_payload, fin_resp, fin_status = await _finalize_adjustment(
            http, base_url, auth_session, adj_sid, rowversion
        )
        doc_data["api_finalize_payload"] = fin_payload
        doc_data["api_finalize_response"] = fin_resp

        _fin_ok = fin_status < 400
        if _fin_ok and isinstance(fin_resp, dict):
            _fin_errors = fin_resp.get("errors") or fin_resp.get("error")
            if _fin_errors:
                _fin_ok = False

        if not _fin_ok:
            _fin_err_detail = None
            if isinstance(fin_resp, dict):
                _fin_err_detail = fin_resp.get("errors") or fin_resp.get("error")
            doc_data["error_message"] = (
                f"[Step 5 failed] PUT finalize returned HTTP {fin_status}. "
                + (f"RetailPro error: {_fin_err_detail}. " if _fin_err_detail else "")
                + "Full response → see Step 5 Finalize Response in API trace."
            )
            doc_data["error_count"] = len(rows)
            logger.warning(
                "[PriceAdj] Finalize PUT failed note='%s' adj_sid=%s HTTP %d",
                note, adj_sid, fin_status,
            )
            await _persist_price_adj_doc(doc_data)
            return doc_data

        logger.info("[PriceAdj] Finalize PUT succeeded note='%s' adj_sid=%s HTTP %d", note, adj_sid, fin_status)

        for item in doc_data["items_data"]:
            if item.get("item_sid"):
                item["ok"] = True

        posted = sum(1 for it in doc_data["items_data"] if it.get("ok"))
        errors = len(doc_data["items_data"]) - posted
        doc_data.update({
            "posted_count": posted,
            "error_count": errors,
            "status": "posted" if errors == 0 else ("partial" if posted > 0 else "error"),
        })

        # Step 6: Post comment
        if note:
            try:
                comment_payload, comment_resp = await _post_price_adj_comment(
                    http, base_url, auth_session, adj_sid, note
                )
                doc_data["api_comment_payload"] = comment_payload
                doc_data["api_comment_response"] = comment_resp
                logger.info("[PriceAdj] Comment posted for adj_sid=%s", adj_sid)
            except Exception as comment_exc:
                logger.warning(
                    "[PriceAdj] Comment post failed for adj_sid=%s: %s", adj_sid, comment_exc
                )
                doc_data["api_comment_response"] = {"error": str(comment_exc)}

    except Exception as exc:
        logger.exception("Error processing price adj note='%s' store=%s", note, store_code)
        exc_type = type(exc).__name__
        exc_msg  = str(exc) or "(no message)"
        doc_data["error_message"] = (
            f"[Unhandled exception] {exc_type}: {exc_msg}\n"
            f"note={note} store={store_code} adj_sid={doc_data.get('adj_sid')}"
        )
        doc_data["error_count"] = len(rows)
        if not doc_data.get("items_data") and items_detail:
            doc_data["items_data"] = items_detail

    try:
        await _persist_price_adj_doc(doc_data)
    except Exception as db_exc:
        logger.error("[PriceAdj] DB persist failed note='%s' store=%s: %s", note, store_code, db_exc)

    logger.info(
        "[PriceAdj] Note='%s' Store=%s adj_sid=%s items=%d posted=%d errors=%d",
        note, store_code, doc_data.get("adj_sid"),
        doc_data["item_count"], doc_data["posted_count"], doc_data["error_count"],
    )
    return doc_data


# ── Main entry point ──────────────────────────────────────────────────────────

async def process_price_adjustment_csv(
    file_bytes: bytes,
    source_file: str = "price_adjustment.csv",
) -> dict:
    """
    Full pipeline: parse CSV → group by NOTE → one adjustment document per NOTE.
    Supports cooperative cancellation via request_cancel_import().
    """
    from app.services.retailpro_auth import get_auth_session, sit_session, stand_session
    from app.db.settings_store import get_setting

    global _active_import_id
    import_id = str(_uuid.uuid4())
    _active_import_id = import_id
    _cancel_requests.discard(import_id)

    rows = parse_price_adjustment_csv(file_bytes)
    if not rows:
        _active_import_id = None
        return {"ok": False, "error": "No data rows found (all rows missing SCANUPC).", "docs": []}

    oc = {
        "host":         (await get_setting("oracle_host"))         or "",
        "port":         int((await get_setting("oracle_port"))     or "1521"),
        "service_name": (await get_setting("oracle_service_name")) or "",
        "username":     (await get_setting("oracle_username"))     or "",
        "password":     (await get_setting("oracle_password"))     or "",
    }

    base_url = ((await get_setting("retailpro_base_url")) or "").rstrip("/")
    auth_session = await get_auth_session()
    await sit_session(base_url, auth_session)

    # Group rows by NOTE — each unique note = one adjustment document
    note_groups: dict[str, list[dict]] = {}
    for row in rows:
        n = str(row.get("NOTE", "") or "").strip()
        note_groups.setdefault(n, []).append(row)

    store_sid_cache: dict = {}
    sbs_sid_cache: dict = {}
    price_lvl_sid_cache: dict = {}
    item_sid_cache: dict = {}

    adj_reason_sid = await _get_adj_reason_sid(oc)
    if not adj_reason_sid:
        logger.warning("[PriceAdj] Could not resolve adjreasonsid from Oracle (PREF_REASON name=MANUALLY)")

    cancelled = False
    all_docs: list[dict] = []
    try:
        # connect/write stay short; read is generous because the adjitem POST
        # can take several minutes when the RetailPro server is busy.
        _timeout = httpx.Timeout(connect=30.0, read=300.0, write=60.0, pool=30.0)
        async with httpx.AsyncClient(timeout=_timeout, verify=False, follow_redirects=True) as http:
            for note, note_rows in note_groups.items():
                if _is_import_cancelled(import_id):
                    logger.info(
                        "Price adjustment import %s cancelled — stopped after %d notes",
                        import_id, len(all_docs),
                    )
                    cancelled = True
                    break
                doc = await _process_note_batch(
                    note=note,
                    rows=note_rows,
                    source_file=source_file,
                    base_url=base_url,
                    auth_session=auth_session,
                    http=http,
                    oc=oc,
                    store_sid_cache=store_sid_cache,
                    sbs_sid_cache=sbs_sid_cache,
                    price_lvl_sid_cache=price_lvl_sid_cache,
                    item_sid_cache=item_sid_cache,
                    adj_reason_sid=adj_reason_sid,
                )
                all_docs.append(doc)
    finally:
        await stand_session(base_url, auth_session)
        if _active_import_id == import_id:
            _active_import_id = None
        _cancel_requests.discard(import_id)

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
