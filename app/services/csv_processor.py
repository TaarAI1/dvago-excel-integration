import polars as pl
import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


def infer_document_type(filename: str) -> str:
    """
    Infer document type from CSV filename.
    Extend this mapping as needed once real filenames are known.
    """
    name = filename.lower()
    if "item" in name or "master" in name:
        return "item_master"
    if "receiv" in name or "voucher" in name:
        return "receiving_voucher"
    if "adjust" in name or "inventory" in name:
        return "inventory_adjustment"
    return "unknown"


def parse_csv(file_path: str, document_type: str, field_map: Dict[str, str]) -> List[Dict[str, Any]]:
    """
    Parse a CSV file using Polars and apply optional column rename mapping.

    Args:
        file_path: Local path to the CSV file.
        document_type: The document type string (for logging).
        field_map: Dict mapping CSV column names → MongoDB field names.
                   Columns not in the map are kept with their original names.

    Returns:
        List of dicts, one per CSV row, ready for motor.insert_many().
    """
    df = pl.read_csv(file_path, infer_schema_length=1000, null_values=["", "NULL", "null", "N/A"])

    if field_map:
        # Only rename columns that actually exist in the dataframe
        actual_map = {k: v for k, v in field_map.items() if k in df.columns}
        if actual_map:
            df = df.rename(actual_map)

    records = df.to_dicts()
    logger.info(f"Parsed {len(records)} rows from {file_path} (type={document_type})")
    return records
