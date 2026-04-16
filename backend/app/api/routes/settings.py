import asyncio
from typing import Dict

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from app.core.security import get_current_user
from app.db.settings_store import get_all_settings, update_settings, get_setting
from app.scheduler import scheduler, setup_scheduler, SALES_EXPORT_JOB_ID, FTP_JOB_ID

router = APIRouter(prefix="/api/settings", tags=["settings"])


class SettingsUpdateRequest(BaseModel):
    updates: Dict[str, str]


class TestConnectionRequest(BaseModel):
    host: str = ""
    port: int = 21
    user: str = ""
    password: str = ""
    service_name: str = ""  # Oracle only
    base_url: str = ""      # RetailPro only
    api_key: str = ""       # RetailPro only


class RetailProAuthRequest(BaseModel):
    base_url: str       # e.g. http://192.168.1.100
    username: str
    password: str


@router.get("")
async def get_settings(_: str = Depends(get_current_user)):
    """Return all settings grouped by category. Sensitive values are redacted."""
    all_settings = await get_all_settings()
    # Redact sensitive values
    for category in all_settings.values():
        for key, meta in category.items():
            if meta.get("is_sensitive") and meta.get("value"):
                meta["value"] = "••••••••"
    return all_settings


@router.get("/raw")
async def get_settings_raw(_: str = Depends(get_current_user)):
    """Return all settings with actual values (for form pre-fill). Sensitive fields included."""
    return await get_all_settings()


@router.put("")
async def save_settings(body: SettingsUpdateRequest, _: str = Depends(get_current_user)):
    """
    Bulk-update settings. Accepts {key: value} pairs.
    Reconfigures scheduler if cron expressions are changed.
    """
    # Filter out redacted sentinel values so they don't overwrite real passwords
    clean_updates = {k: v for k, v in body.updates.items() if v != "••••••••"}
    await update_settings(clean_updates)

    # Reload scheduler if relevant cron keys changed
    cron_keys = {"poll_cron_schedule", "sales_export_cron"}
    if cron_keys & set(clean_updates.keys()):
        poll_cron = await get_setting("poll_cron_schedule", "*/15 * * * *")
        sales_cron = await get_setting("sales_export_cron", "0 2 * * *")
        try:
            setup_scheduler(poll_cron or "*/15 * * * *", sales_cron or "0 2 * * *")
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Invalid cron expression: {exc}")

    return {"message": f"Saved {len(clean_updates)} settings."}


@router.post("/test/ftp")
async def test_ftp(body: TestConnectionRequest, _: str = Depends(get_current_user)):
    """Test FTP connection with provided credentials."""
    from app.services.ftp_service import test_ftp_connection
    result = await asyncio.to_thread(
        test_ftp_connection, body.host, body.port, body.user, body.password
    )
    return result


@router.post("/test/oracle")
async def test_oracle(body: TestConnectionRequest, _: str = Depends(get_current_user)):
    """Test Oracle DB connection with provided credentials."""
    from app.services.oracle_service import test_oracle_connection
    result = await test_oracle_connection(
        body.host, body.port, body.service_name, body.user, body.password
    )
    return result


class OracleQueryRequest(BaseModel):
    host: str = ""
    port: int = 1521
    user: str = ""
    password: str = ""
    service_name: str = ""
    sql: str = ""


@router.post("/oracle/query")
async def run_oracle_query(body: OracleQueryRequest, _: str = Depends(get_current_user)):
    """Execute a read-only SQL query against Oracle and return columns + rows (max 500 rows)."""
    from app.services.oracle_service import run_query
    if not body.sql.strip():
        raise HTTPException(status_code=400, detail="SQL query is required.")
    # Safety: only allow SELECT statements
    first_word = body.sql.strip().split()[0].upper()
    if first_word not in ("SELECT", "WITH"):
        raise HTTPException(status_code=400, detail="Only SELECT / WITH queries are allowed.")
    try:
        df = await run_query(body.host, body.port, body.service_name, body.user, body.password, body.sql)
        df = df.head(500)
        columns = df.columns
        rows = [list(row) for row in df.iter_rows()]
        return {"ok": True, "columns": columns, "rows": rows, "row_count": len(rows)}
    except Exception as exc:
        return {"ok": False, "columns": [], "rows": [], "row_count": 0, "error": str(exc)}


@router.post("/test/retailpro")
async def test_retailpro(body: RetailProAuthRequest, _: str = Depends(get_current_user)):
    """
    Authenticate against the RetailPro Prism API using the 3-step nonce challenge.

    Step 1 – GET /v1/rest/auth           → extract Auth-Nonce from response headers
    Step 2 – Compute Auth-Nonce-Response  = ((Auth-Nonce // 13) % 99999) * 17
    Step 3 – GET /v1/rest/auth?pwd=&usr= with Auth-Nonce + Auth-Nonce-Response headers
             → extract Auth-Session from response headers

    Returns the session token on success, or the full server response on failure.
    """
    import httpx

    base = body.base_url.rstrip("/")
    auth_url = f"{base}/v1/rest/auth"

    try:
        async with httpx.AsyncClient(timeout=15.0, verify=False, follow_redirects=True) as client:
            # ── Step 1: get the nonce challenge ────────────────────────────
            step1 = await client.get(auth_url)
            auth_nonce_raw = step1.headers.get("Auth-Nonce") or step1.headers.get("auth-nonce")

            if auth_nonce_raw is None:
                # Surface the full response so the user can debug
                return {
                    "ok": False,
                    "step": 1,
                    "status_code": step1.status_code,
                    "headers": dict(step1.headers),
                    "body": step1.text,
                    "error": "Auth-Nonce header not found in step-1 response.",
                }

            try:
                auth_nonce = int(auth_nonce_raw)
            except ValueError:
                return {
                    "ok": False,
                    "step": 1,
                    "status_code": step1.status_code,
                    "headers": dict(step1.headers),
                    "body": step1.text,
                    "error": f"Auth-Nonce value is not an integer: {auth_nonce_raw!r}",
                }

            # ── Step 2: compute the nonce response ─────────────────────────
            # Mirrors the C# logic exactly:
            #   truncatedValue     = authNonce / 13      (integer division)
            #   remainder          = truncatedValue % 99999
            #   authNonceResponse  = remainder * 17
            truncated = auth_nonce // 13
            remainder = truncated % 99999
            auth_nonce_response = remainder * 17

            # ── Step 3: send credentials + computed headers ────────────────
            step3 = await client.get(
                f"{auth_url}?pwd={body.password}&usr={body.username}",
                headers={
                    "Auth-Nonce": str(auth_nonce),
                    "Auth-Nonce-Response": str(auth_nonce_response),
                },
            )

            auth_session = (
                step3.headers.get("Auth-Session")
                or step3.headers.get("auth-session")
            )

            if auth_session:
                return {
                    "ok": True,
                    "session": auth_session,
                    "status_code": step3.status_code,
                    "message": "Authentication successful. Session token received.",
                }

            # Auth-Session missing — return full response so user can diagnose
            return {
                "ok": False,
                "step": 3,
                "status_code": step3.status_code,
                "headers": dict(step3.headers),
                "body": step3.text,
                "error": "Auth-Session header not found in step-3 response. Check credentials.",
            }

    except httpx.ConnectError as exc:
        return {"ok": False, "step": 1, "error": f"Cannot connect to RetailPro server: {exc}"}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}
