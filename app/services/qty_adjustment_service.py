"""
Quantity Adjustment import service.

Processing pipeline:
  1. Parse CSV — extract Store Code, scanupc, adjquantity columns
  2. Skip blank rows (no scanupc)
  3. Group rows by Store Code
  4. For each store:
     a. Resolve store SID from Oracle:  SELECT sid FROM rps.store WHERE store_Code = ?
     b. Resolve sbs SID from Oracle:    SELECT sid FROM rps.subsidairy WHERE sbs_no = 1
  5. Chunk the store's rows into batches of 20
  6. For each batch:
     a. POST /api/backoffice/adjustment            → adj_sid
     b. Resolve each item SID from Oracle:
           SELECT sid FROM rps.invn_Sbs_item WHERE upc = ?
     c. POST /api/backoffice/adjustment/{adj_sid}/adjitem
     d. GET  /api/backoffice/adjustment?filter=(sid,eq,{adj_sid})  → rowversion
     e. PUT  /api/backoffice/adjustment/{adj_sid}  → status 4
  7. Persist each adjustment document to qty_adjustment_docs table
"""
import csv
import io
import logging
import uuid as _uuid
from typing import Any, Optional

import httpx

from app.core.timezone import now_pkt

logger = logging.getLogger(__name__)

BATCH_SIZE = 20


# ── CSV parsing ──────────────────────────────────────────────────────────────

def parse_qty_adjustment_csv(file_bytes: bytes) -> list[dict]:
    """
    Parse CSV bytes. Returns list of row dicts with normalised uppercase keys.
    Rows without a 'SCANUPC' value are skipped.
    Blank/header-only rows are skipped.
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
        normalised = {k.strip().upper().replace(" ", "_"): str(v).strip()
                      for k, v in row.items() if k}
        upc = normalised.get("SCANUPC", "").strip()
        if not upc:
            continue
        rows_out.append(normalised)
    return rows_out


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
            f"SELECT sid FROM rps.store WHERE store_Code = '{key}'", oc
        )
    return cache[key]


async def _get_sbs_sid(cache: dict, oc: dict) -> Optional[str]:
    if "__sbs1__" not in cache:
        cache["__sbs1__"] = await _oracle_scalar(
            "SELECT sid FROM rps.subsidairy WHERE sbs_no = 1", oc
        )
    return cache["__sbs1__"]


async def _get_item_sid(upc: str, cache: dict, oc: dict) -> Optional[str]:
    key = str(upc).strip()
    if key not in cache:
        cache[key] = await _oracle_scalar(
            f"SELECT sid FROM rps.invn_Sbs_item WHERE upc = '{key}'", oc
        )
    return cache[key]


# ── RetailPro API calls ───────────────────────────────────────────────────────

def _rp_headers(auth_session: str) -> dict:
    return {
        "Accept": "application/json, version=2",
        "Auth-Session": auth_session,
        "Content-Type": "application/json",
    }


async def _create_adjustment_doc(
    http: httpx.AsyncClient,
    base_url: str,
    auth_session: str,
    store_sid: str,
    sbs_sid: str,
) -> tuple[Optional[str], dict, dict]:
    """
    POST /api/backoffice/adjustment
    Returns (adj_sid, payload_sent, response_json).
    """
    payload = {
        "data": [{
            "originapplication": "rProPrismWeb",
            "status": 3,
            "adjtype": 0,
            "sbssid": sbs_sid,
            "creatingdoctype": 8,
            "origstoresid": store_sid,
            "reasonname": "Manually",
            "storesid": store_sid,
        }]
    }
    resp = await http.post(
        f"{base_url}/api/backoffice/adjustment",
        json=payload,
        headers=_rp_headers(auth_session),
    )
    try:
        resp_json = resp.json()
    except Exception:
        resp_json = {"raw": resp.text}

    data = resp_json.get("data") or [] if isinstance(resp_json, dict) else []
    adj_sid = data[0].get("sid") if data else None
    return adj_sid, payload, resp_json


async def _post_adj_items(
    http: httpx.AsyncClient,
    base_url: str,
    auth_session: str,
    adj_sid: str,
    items: list[dict],  # [{item_sid, adj_value, upc}]
) -> tuple[dict, dict]:
    """
    POST /api/backoffice/adjustment/{adj_sid}/adjitem
    Returns (payload_sent, response_json).
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
    return payload, resp_json


async def _get_adjustment_rowversion(
    http: httpx.AsyncClient,
    base_url: str,
    auth_session: str,
    adj_sid: str,
) -> tuple[Optional[int], dict]:
    """
    GET /api/backoffice/adjustment?filter=(sid,eq,{adj_sid})
    Returns (rowversion, response_json).
    """
    resp = await http.get(
        f"{base_url}/api/backoffice/adjustment",
        params={"filter": f"(sid,eq,{adj_sid})"},
        headers=_rp_headers(auth_session),
    )
    try:
        resp_json = resp.json()
    except Exception:
        resp_json = {"raw": resp.text}

    data = resp_json.get("data") or [] if isinstance(resp_json, dict) else []
    rowversion = data[0].get("rowversion") if data else None
    return rowversion, resp_json


async def _finalize_adjustment(
    http: httpx.AsyncClient,
    base_url: str,
    auth_session: str,
    adj_sid: str,
    rowversion: int,
) -> tuple[dict, dict]:
    """
    PUT /api/backoffice/adjustment/{adj_sid}
    Returns (payload_sent, response_json).
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
    return payload, resp_json


# ── DB persistence ────────────────────────────────────────────────────────────

async def _persist_adj_doc(doc_data: dict) -> None:
    from app.db.postgres import get_session
    from app.models.qty_adjustment_doc import QtyAdjustmentDoc

    async with get_session() as session:
        async with session.begin():
            doc = QtyAdjustmentDoc(
                id=_uuid.uuid4(),
                source_file=doc_data.get("source_file"),
                store_code=doc_data.get("store_code"),
                store_name=doc_data.get("store_name"),
                store_sid=doc_data.get("store_sid"),
                sbs_sid=doc_data.get("sbs_sid"),
                adj_sid=doc_data.get("adj_sid"),
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
                items_data=doc_data.get("items_data"),
                posted_at=now_pkt() if doc_data.get("status") == "posted" else None,
            )
            session.add(doc)


# ── Batch processor ───────────────────────────────────────────────────────────

async def _process_store_batch(
    store_code: str,
    store_name: str,
    rows: list[dict],
    source_file: str,
    base_url: str,
    auth_session: str,
    http: httpx.AsyncClient,
    oc: dict,
    store_sid_cache: dict,
    sbs_sid_cache: dict,
    item_sid_cache: dict,
) -> list[dict]:
    """
    Process all rows for a single store in batches of BATCH_SIZE.
    Returns list of adjustment doc result dicts.
    """
    adj_docs: list[dict] = []

    # Resolve store SID
    store_sid = await _get_store_sid(store_code, store_sid_cache, oc)
    sbs_sid = await _get_sbs_sid(sbs_sid_cache, oc)

    if not store_sid:
        logger.warning("No store SID found for store_code=%s — skipping %d rows", store_code, len(rows))
        doc_data = {
            "source_file": source_file,
            "store_code": store_code,
            "store_name": store_name,
            "store_sid": None,
            "sbs_sid": sbs_sid,
            "adj_sid": None,
            "item_count": len(rows),
            "posted_count": 0,
            "error_count": len(rows),
            "status": "error",
            "error_message": f"Store SID not found in Oracle for store_code={store_code}",
            "items_data": [],
        }
        await _persist_adj_doc(doc_data)
        adj_docs.append(doc_data)
        return adj_docs

    if not sbs_sid:
        logger.warning("No sbs SID found (sbs_no=1) — skipping store %s", store_code)

    # Chunk into batches of BATCH_SIZE
    for batch_start in range(0, len(rows), BATCH_SIZE):
        batch_rows = rows[batch_start: batch_start + BATCH_SIZE]
        doc_data: dict = {
            "source_file": source_file,
            "store_code": store_code,
            "store_name": store_name,
            "store_sid": store_sid,
            "sbs_sid": sbs_sid,
            "adj_sid": None,
            "item_count": len(batch_rows),
            "posted_count": 0,
            "error_count": 0,
            "status": "error",
            "error_message": None,
            "items_data": [],
        }

        try:
            # ── Step 1: Create adjustment document ───────────────────────────
            adj_sid, create_payload, create_resp = await _create_adjustment_doc(
                http, base_url, auth_session, store_sid, sbs_sid or ""
            )
            doc_data["api_create_payload"] = create_payload
            doc_data["api_create_response"] = create_resp

            if not adj_sid:
                doc_data["error_message"] = f"No adj_sid in create response: {create_resp}"
                doc_data["error_count"] = len(batch_rows)
                await _persist_adj_doc(doc_data)
                adj_docs.append(doc_data)
                continue

            doc_data["adj_sid"] = adj_sid

            # ── Step 2: Resolve item SIDs ─────────────────────────────────────
            items_for_api: list[dict] = []
            items_detail: list[dict] = []

            for row in batch_rows:
                upc = row.get("SCANUPC", "").strip()
                try:
                    adj_value = int(float(row.get("ADJQUANTITY", "0") or "0"))
                except (ValueError, TypeError):
                    adj_value = 0

                item_sid = await _get_item_sid(upc, item_sid_cache, oc)
                item_detail = {
                    "upc": upc,
                    "adj_value": adj_value,
                    "item_sid": item_sid,
                    "ok": False,
                    "error": None if item_sid else "Item SID not found in Oracle",
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
                doc_data["error_message"] = "No item SIDs resolved — all UPCs missing from Oracle"
                doc_data["error_count"] = len(batch_rows)
                await _persist_adj_doc(doc_data)
                adj_docs.append(doc_data)
                continue

            # ── Step 3: Post adj items ────────────────────────────────────────
            items_payload, items_resp = await _post_adj_items(
                http, base_url, auth_session, adj_sid, items_for_api
            )
            doc_data["api_items_payload"] = items_payload
            doc_data["api_items_response"] = items_resp

            # ── Step 4: Get rowversion ────────────────────────────────────────
            rowversion, get_resp = await _get_adjustment_rowversion(
                http, base_url, auth_session, adj_sid
            )
            doc_data["api_get_response"] = get_resp

            if rowversion is None:
                doc_data["error_message"] = f"Could not get rowversion from GET response: {get_resp}"
                doc_data["error_count"] = len(batch_rows)
                await _persist_adj_doc(doc_data)
                adj_docs.append(doc_data)
                continue

            # ── Step 5: Finalize (status → 4) ────────────────────────────────
            fin_payload, fin_resp = await _finalize_adjustment(
                http, base_url, auth_session, adj_sid, rowversion
            )
            doc_data["api_finalize_payload"] = fin_payload
            doc_data["api_finalize_response"] = fin_resp

            # Mark items as ok
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

        except Exception as exc:
            logger.exception("Error processing store %s batch starting at %d", store_code, batch_start)
            doc_data["error_message"] = str(exc)
            doc_data["error_count"] = len(batch_rows)

        await _persist_adj_doc(doc_data)
        adj_docs.append(doc_data)
        logger.info(
            "[QtyAdj] Store=%s adj_sid=%s items=%d posted=%d errors=%d",
            store_code, doc_data.get("adj_sid"),
            doc_data["item_count"], doc_data["posted_count"], doc_data["error_count"],
        )

    return adj_docs


# ── Main entry point ──────────────────────────────────────────────────────────

async def process_qty_adjustment_csv(
    file_bytes: bytes,
    source_file: str = "qty_adjustment.csv",
) -> dict:
    """
    Full pipeline: parse CSV → group by store → process in 20-item batches.
    Returns a summary dict.
    """
    from app.services.retailpro_auth import get_auth_session, sit_session, stand_session
    from app.db.settings_store import get_setting

    rows = parse_qty_adjustment_csv(file_bytes)
    if not rows:
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

    # Group rows by store code
    store_groups: dict[str, list[dict]] = {}
    store_names: dict[str, str] = {}
    for row in rows:
        sc = str(row.get("STORE_CODE", "")).strip() or "1"
        store_groups.setdefault(sc, []).append(row)
        if sc not in store_names:
            store_names[sc] = str(row.get("STORE_NAME", "")).strip()

    store_sid_cache: dict = {}
    sbs_sid_cache: dict = {}
    item_sid_cache: dict = {}

    all_docs: list[dict] = []
    try:
        async with httpx.AsyncClient(timeout=30.0, verify=False, follow_redirects=True) as http:
            for store_code, store_rows in store_groups.items():
                docs = await _process_store_batch(
                    store_code=store_code,
                    store_name=store_names.get(store_code, ""),
                    rows=store_rows,
                    source_file=source_file,
                    base_url=base_url,
                    auth_session=auth_session,
                    http=http,
                    oc=oc,
                    store_sid_cache=store_sid_cache,
                    sbs_sid_cache=sbs_sid_cache,
                    item_sid_cache=item_sid_cache,
                )
                all_docs.extend(docs)
    finally:
        await stand_session(base_url, auth_session)

    total_docs   = len(all_docs)
    posted_docs  = sum(1 for d in all_docs if d.get("status") == "posted")
    partial_docs = sum(1 for d in all_docs if d.get("status") == "partial")
    error_docs   = sum(1 for d in all_docs if d.get("status") == "error")
    total_items  = sum(d.get("item_count", 0) for d in all_docs)
    posted_items = sum(d.get("posted_count", 0) for d in all_docs)

    return {
        "ok": True,
        "total_rows": len(rows),
        "total_docs": total_docs,
        "posted_docs": posted_docs,
        "partial_docs": partial_docs,
        "error_docs": error_docs,
        "total_items": total_items,
        "posted_items": posted_items,
    }
