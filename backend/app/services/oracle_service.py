"""
Oracle Database service using oracledb in thick mode.
Oracle Instant Client must be installed at /opt/oracle/instantclient (done in Dockerfile).
"""
import asyncio
import logging
from typing import Optional

import oracledb
import polars as pl

logger = logging.getLogger(__name__)

_thick_initialized = False
_thick_error: Optional[str] = None
_INSTANT_CLIENT_DIR = "/opt/oracle/instantclient"


def _ensure_thick_mode() -> None:
    """
    Try to initialize oracledb thick mode.
    Strategy:
      1. If _INSTANT_CLIENT_DIR exists, pass it explicitly.
      2. Otherwise call init_oracle_client() with no args so oracledb can
         auto-discover the libs from LD_LIBRARY_PATH / system paths.
      3. If both fail, fall back to thin mode and record the error.
    oracledb raises an exception if init_oracle_client() is called more than
    once, so we guard with _thick_initialized.
    """
    global _thick_initialized, _thick_error
    if _thick_initialized:
        return
    import os
    _thick_initialized = True
    try:
        if os.path.isdir(_INSTANT_CLIENT_DIR):
            oracledb.init_oracle_client(lib_dir=_INSTANT_CLIENT_DIR)
        else:
            # Let oracledb find the libs via LD_LIBRARY_PATH
            oracledb.init_oracle_client()
        logger.info("oracledb thick mode initialized.")
    except Exception as exc:
        _thick_error = str(exc)
        logger.warning(f"oracledb thick mode init failed, running in thin mode: {exc}")


def _test_connection_sync(host: str, port: int, service_name: str, username: str, password: str) -> dict:
    _ensure_thick_mode()
    try:
        dsn = f"{host}:{port}/{service_name}"
        conn = oracledb.connect(user=username, password=password, dsn=dsn)
        conn.close()
        return {"ok": True, "error": None}
    except Exception as exc:
        error_str = str(exc)
        # DPY-3015 means the DB uses an old password verifier only supported in thick mode.
        # Thick mode requires Oracle Instant Client, which may not be installed.
        if "DPY-3015" in error_str or "password verifier" in error_str:
            hint = (
                "This Oracle server uses an authentication type (password verifier 0x939) "
                "that requires Oracle Instant Client (thick mode). "
                "Ensure Oracle Instant Client is installed and the server is configured to allow "
                "SHA-based authentication (set SQLNET.ALLOWED_LOGON_VERSION_SERVER=11 or lower on the DB side, "
                "or install Oracle Instant Client on this server)."
            )
            return {"ok": False, "error": hint}
        return {"ok": False, "error": error_str}


def _run_query_sync(
    host: str, port: int, service_name: str, username: str, password: str, sql: str
) -> pl.DataFrame:
    _ensure_thick_mode()
    dsn = f"{host}:{port}/{service_name}"
    conn = oracledb.connect(user=username, password=password, dsn=dsn)
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        data = {col: [row[i] for row in rows] for i, col in enumerate(columns)}
        return pl.DataFrame(data)
    finally:
        conn.close()


async def test_oracle_connection(
    host: str, port: int, service_name: str, username: str, password: str
) -> dict:
    """Test Oracle connection. Returns {ok: bool, error: str|None}."""
    return await asyncio.to_thread(
        _test_connection_sync, host, port, service_name, username, password
    )


async def run_query(
    host: str, port: int, service_name: str, username: str, password: str, sql: str
) -> pl.DataFrame:
    """Run a SQL query and return results as a Polars DataFrame."""
    return await asyncio.to_thread(
        _run_query_sync, host, port, service_name, username, password, sql
    )
