import ftplib
import os
import tempfile
import logging
from typing import List, Tuple
from app.core.config import settings

logger = logging.getLogger(__name__)


def list_csv_files() -> List[str]:
    """Connect to FTP and return list of .csv filenames in the base path."""
    filenames = []
    with ftplib.FTP() as ftp:
        ftp.connect(settings.ftp_host, settings.ftp_port, timeout=30)
        ftp.login(settings.ftp_user, settings.ftp_password)
        ftp.cwd(settings.ftp_base_path)
        all_files = ftp.nlst()
        filenames = [f for f in all_files if f.lower().endswith(".csv")]
    logger.debug(f"FTP listed {len(filenames)} CSV files")
    return filenames


def download_csv_file(filename: str) -> str:
    """
    Download a single CSV file from FTP to a temp file.
    Returns the local temp file path. Caller is responsible for deleting it.
    """
    suffix = os.path.splitext(filename)[1] or ".csv"
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix, prefix="ftp_")
    tmp_path = tmp.name
    tmp.close()

    with ftplib.FTP() as ftp:
        ftp.connect(settings.ftp_host, settings.ftp_port, timeout=60)
        ftp.login(settings.ftp_user, settings.ftp_password)
        ftp.cwd(settings.ftp_base_path)
        with open(tmp_path, "wb") as f:
            ftp.retrbinary(f"RETR {filename}", f.write)

    logger.info(f"Downloaded FTP file: {filename} → {tmp_path}")
    return tmp_path
