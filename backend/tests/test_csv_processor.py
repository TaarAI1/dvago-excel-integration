import os
import tempfile
import pytest
from app.services.csv_processor import parse_csv, infer_document_type


def _write_csv(content: str) -> str:
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    tmp.write(content)
    tmp.close()
    return tmp.name


def test_parse_csv_basic():
    path = _write_csv("sku,description,price\nABC,Widget,9.99\nXYZ,Gadget,19.99\n")
    try:
        records = parse_csv(path, "item_master", {})
        assert len(records) == 2
        assert records[0]["sku"] == "ABC"
        assert records[1]["price"] == 19.99
    finally:
        os.remove(path)


def test_parse_csv_with_field_map():
    path = _write_csv("ItemCode,ItemName\nA001,Widget\n")
    try:
        records = parse_csv(path, "item_master", {"ItemCode": "sku", "ItemName": "name"})
        assert "sku" in records[0]
        assert records[0]["sku"] == "A001"
        assert records[0]["name"] == "Widget"
    finally:
        os.remove(path)


def test_parse_csv_ignores_unknown_map_keys():
    path = _write_csv("col1,col2\nval1,val2\n")
    try:
        records = parse_csv(path, "item_master", {"nonexistent_col": "new_name"})
        assert records[0]["col1"] == "val1"
    finally:
        os.remove(path)


def test_infer_document_type():
    assert infer_document_type("item_master.csv") == "item_master"
    assert infer_document_type("RECEIVING_VOUCHER_2024.csv") == "receiving_voucher"
    assert infer_document_type("inventory_adjustment.csv") == "inventory_adjustment"
    assert infer_document_type("unknown_file.csv") == "unknown"
