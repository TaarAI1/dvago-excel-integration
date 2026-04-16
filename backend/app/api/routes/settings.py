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
async def test_retailpro(body: TestConnectionRequest, _: str = Depends(get_current_user)):
    """Test RetailPro API reachability with provided URL and key."""
    import httpx
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                body.base_url.rstrip("/") + "/",
                headers={"Authorization": f"Bearer {body.api_key}"},
            )
        return {"ok": True, "status_code": response.status_code, "error": None}
    except Exception as exc:
        return {"ok": False, "status_code": None, "error": str(exc)}
