import pytest
import pytest_asyncio
from app.services.retailpro_client import MockRetailProClient, RetailProError


@pytest.mark.asyncio
async def test_mock_client_returns_sid():
    client = MockRetailProClient()
    response = await client.post_document("/items", {"sku": "ABC", "name": "Widget"})
    assert "data" in response
    assert len(response["data"]) > 0
    assert "sid" in response["data"][0]
    sid = response["data"][0]["sid"]
    assert sid.startswith("MOCK-")


@pytest.mark.asyncio
async def test_mock_client_unique_sids():
    client = MockRetailProClient()
    r1 = await client.post_document("/items", {"sku": "A"})
    r2 = await client.post_document("/items", {"sku": "B"})
    assert r1["data"][0]["sid"] != r2["data"][0]["sid"]
