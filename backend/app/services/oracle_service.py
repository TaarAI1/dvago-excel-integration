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
    global _thick_initialized, _thick_error
    if _thick_initialized:
        return
    try:
        oracledb.init_oracle_client(lib_dir=_INSTANT_CLIENT_DIR)
        _thick_initialized = True
        logger.info("oracledb thick mode initialized.")
    except Exception as exc:
        _thick_error = str(exc)
        _thick_initialized = True
        logger.warning(f"oracledb thick mode init failed (thin mode may be used): {exc}")


def _test_connection_sync(host: str, port: int, service_name: str, username: str, password: str) -> dict:
    _ensure_thick_mode()
    try:
        dsn = f"{host}:{port}/{service_name}"
        conn = oracledb.connect(user=username, password=password, dsn=dsn)
        conn.close()
        return {"ok": True, "error": None}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


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
