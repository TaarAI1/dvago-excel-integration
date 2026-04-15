from pydantic import BaseModel, Field
from typing import Optional, Any, Dict
from datetime import datetime
from bson import ObjectId


class DocumentModel(BaseModel):
    document_type: str  # "item_master" | "receiving_voucher" | "inventory_adjustment"
    original_data: Dict[str, Any]
    retailprosid: Optional[str] = None
    posted: bool = False
    has_error: bool = False
    error_message: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    posted_at: Optional[datetime] = None
    source_file: Optional[str] = None  # name of CSV file this row came from


def document_to_response(doc: dict) -> dict:
    """Convert MongoDB document to JSON-serializable dict."""
    doc = dict(doc)
    if "_id" in doc:
        doc["id"] = str(doc.pop("_id"))
    for key, val in doc.items():
        if isinstance(val, ObjectId):
            doc[key] = str(val)
        elif isinstance(val, datetime):
            doc[key] = val.isoformat()
    return doc
