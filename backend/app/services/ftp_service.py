import ftplib
import io
import os
import tempfile
import logging
from typing import List

logger = logging.getLogger(__name__)


def list_csv_files(host: str, port: int, user: str, password: str, path: str) -> List[str]:
    """Connect to FTP and return list of .csv filenames in the given path."""
    with ftplib.FTP() as ftp:
        ftp.connect(host, port, timeout=30)
        ftp.login(user, password)
        ftp.cwd(path)
        all_files = ftp.nlst()
        filenames = [f for f in all_files if f.lower().endswith(".csv")]
    logger.debug(f"FTP listed {len(filenames)} CSV files in {path}")
    return filenames


def download_csv_file(filename: str, host: str, port: int, user: str, password: str, path: str) -> str:
    """
    Download a single CSV file from FTP to a temp file.
    Returns the local temp file path. Caller is responsible for deleting it.
    """
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
