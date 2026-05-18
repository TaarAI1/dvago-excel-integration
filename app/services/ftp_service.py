import ftplib
import io
import os
import tempfile
import logging
from typing import List

logger = logging.getLogger(__name__)


def _nlst_basenames(ftp: ftplib.FTP) -> list[str]:
    """
    Return bare filenames from the current FTP directory.
    Some FTP servers return full absolute paths from nlst(); this normalises
    them to basenames so downstream code always sees just "file.xlsx".
    """
    return [os.path.basename(f) for f in ftp.nlst()]


def list_csv_files(host: str, port: int, user: str, password: str, path: str) -> List[str]:
    """Connect to FTP and return list of .csv filenames in the given path."""
    with ftplib.FTP() as ftp:
        ftp.connect(host, port, timeout=30)
        ftp.login(user, password)
        ftp.cwd(path)
        filenames = [f for f in _nlst_basenames(ftp) if f.lower().endswith(".csv")]
    logger.debug(f"FTP listed {len(filenames)} CSV files in {path}")
    return filenames


def download_csv_file(filename: str, host: str, port: int, user: str, password: str, path: str) -> str:
    """
    Download a single CSV file from FTP to a temp file.
    Returns the local temp file path. Caller is responsible for deleting it.
    """
    filename = os.path.basename(filename)
    suffix = os.path.splitext(filename)[1] or ".csv"
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix, prefix="ftp_")
    tmp_path = tmp.name
    tmp.close()

    with ftplib.FTP() as ftp:
        ftp.connect(host, port, timeout=60)
        ftp.login(user, password)
        ftp.cwd(path)
        with open(tmp_path, "wb") as f:
            ftp.retrbinary(f"RETR {filename}", f.write)

    logger.info(f"Downloaded FTP file: {filename} → {tmp_path}")
    return tmp_path


def list_all_files(host: str, port: int, user: str, password: str, path: str) -> list[str]:
    """Return all .csv and .xlsx filenames in the given FTP path."""
    _EXTS = (".csv", ".xlsx")
    with ftplib.FTP() as ftp:
        ftp.connect(host, port, timeout=30)
        ftp.login(user, password)
        ftp.cwd(path)
        filenames = [f for f in _nlst_basenames(ftp) if f.lower().endswith(_EXTS)]
    logger.debug(f"FTP listed {len(filenames)} importable files in {path}")
    return filenames


def list_excel_files(host: str, port: int, user: str, password: str, path: str) -> list[str]:
    """Return all .xlsx filenames in the given FTP path."""
    with ftplib.FTP() as ftp:
        ftp.connect(host, port, timeout=30)
        ftp.login(user, password)
        ftp.cwd(path)
        filenames = [f for f in _nlst_basenames(ftp) if f.lower().endswith(".xlsx")]
    logger.debug(f"FTP listed {len(filenames)} Excel files in {path}")
    return filenames


def download_excel_file(filename: str, host: str, port: int, user: str, password: str, path: str) -> bytes:
    """Download a single Excel file from FTP and return its raw bytes."""
    filename = os.path.basename(filename)
    buf = io.BytesIO()
    with ftplib.FTP() as ftp:
        ftp.connect(host, port, timeout=60)
        ftp.login(user, password)
        ftp.cwd(path)
        ftp.retrbinary(f"RETR {filename}", buf.write)
    logger.info(f"Downloaded FTP Excel: {filename} ({buf.tell()} bytes)")
    return buf.getvalue()


def move_ftp_file_to_processed(
    filename: str,
    src_path: str,
    dst_path: str,
    host: str,
    port: int,
    user: str,
    password: str,
) -> None:
    """
    Move a file on the FTP server from src_path to dst_path.
    Creates dst_path directory if it does not exist.
    The file is renamed with a timestamp to avoid collisions.
    """
    from datetime import datetime as _dt, timezone as _tz, timedelta as _td
    _PKT = _tz(_td(hours=5))
    stamp = _dt.now(_PKT).strftime("%Y%m%d_%H%M%S")
    filename = os.path.basename(filename)
    base, ext = os.path.splitext(filename)
    dst_filename = f"{base}_{stamp}{ext}"

    src_full = f"{src_path.rstrip('/')}/{filename}"
    dst_full = f"{dst_path.rstrip('/')}/{dst_filename}"

    with ftplib.FTP() as ftp:
        ftp.connect(host, port, timeout=30)
        ftp.login(user, password)
        # Ensure destination directory exists
        try:
            ftp.cwd(dst_path)
        except ftplib.error_perm:
            ftp.mkd(dst_path)
        ftp.rename(src_full, dst_full)

    logger.info(f"Moved FTP file: {src_full} → {dst_full}")


def test_ftp_connection(host: str, port: int, user: str, password: str) -> dict:
    """Test FTP connection credentials. Returns {ok: bool, error: str|None}."""
    try:
        with ftplib.FTP() as ftp:
            ftp.connect(host, port, timeout=10)
            ftp.login(user, password)
        return {"ok": True, "error": None}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def upload_file(
    content: bytes, filename: str, host: str, port: int, user: str, password: str, path: str
) -> None:
    """Upload bytes as a file to the FTP server at the given path."""
    with ftplib.FTP() as ftp:
        ftp.connect(host, port, timeout=60)
        ftp.login(user, password)
        ftp.cwd(path)
        ftp.storbinary(f"STOR {filename}", io.BytesIO(content))
    logger.info(f"Uploaded {filename} to FTP path {path}")


def _ensure_ftp_dir(ftp: ftplib.FTP, path: str) -> None:
    """Navigate to `path` on the FTP server, creating any missing directories."""
    parts = [p for p in path.replace("\\", "/").split("/") if p]
    current = ""
    for part in parts:
        current += f"/{part}"
        try:
            ftp.cwd(current)
        except ftplib.error_perm:
            ftp.mkd(current)
            ftp.cwd(current)


def upload_to_manual_export_subfolder(
    content: bytes,
    filename: str,
    base_path: str,
    module_folder: str,
    host: str,
    port: int,
    user: str,
    password: str,
) -> None:
    """
    Upload `content` as `filename` into `{base_path}/{module_folder}/` on the FTP server.
    Creates the target directory (and any intermediate directories) if they don't exist.
    """
    target_path = f"{base_path.rstrip('/')}/{module_folder}"
    with ftplib.FTP() as ftp:
        ftp.connect(host, port, timeout=60)
        ftp.login(user, password)
        _ensure_ftp_dir(ftp, target_path)
        ftp.storbinary(f"STOR {filename}", io.BytesIO(content))
    logger.info(f"Saved manual import '{filename}' → FTP {target_path}")


async def save_manual_import_to_ftp(
    content: bytes,
    filename: str,
    module_folder: str,
) -> None:
    """
    Async wrapper: reads FTP settings from the DB and uploads `content` to
    `{ftp_manual_export_path}/{module_folder}/{filename}`.
    Silently logs errors so a failed FTP upload never breaks the import response.
    """
    import asyncio
    from app.db.settings_store import get_setting

    try:
        host       = (await get_setting("ftp_host"))               or ""
        port       = int((await get_setting("ftp_port"))           or "21")
        user       = (await get_setting("ftp_user"))               or "anonymous"
        password   = (await get_setting("ftp_password"))           or ""
        base_path  = (await get_setting("ftp_manual_export_path")) or "/exports"

        if not host:
            logger.warning("[FTP manual export] ftp_host not configured — skipping upload.")
            return

        await asyncio.to_thread(
            upload_to_manual_export_subfolder,
            content, filename, base_path, module_folder, host, port, user, password,
        )
    except Exception as exc:
        logger.warning("[FTP manual export] Upload failed for '%s/%s': %s", module_folder, filename, exc)
