from abc import ABC, abstractmethod
from typing import Dict, Any
import httpx
import logging
import json

logger = logging.getLogger(__name__)


class RetailProClientBase(ABC):
    @abstractmethod
    async def post_document(self, endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Post a document payload to a RetailPro endpoint.
        Returns the parsed JSON response dict.
        Raises RetailProError on HTTP error or unexpected response.
        """
        ...


class RetailProError(Exception):
    def __init__(self, status_code: int, response_body: str):
        self.status_code = status_code
        self.response_body = response_body
        super().__init__(f"RetailPro API error {status_code}: {response_body}")


class MockRetailProClient(RetailProClientBase):
    """Returns a fake successful response for development/testing."""

    async def post_document(self, endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        import asyncio, uuid
        await asyncio.sleep(0.05)
        mock_sid = f"MOCK-{uuid.uuid4().hex[:8].upper()}"
        logger.debug(f"[MOCK] POST {endpoint} → sid={mock_sid}")
        return {"data": [{"sid": mock_sid}], "status": "success"}


class RealRetailProClient(RetailProClientBase):
    """Real HTTP client using httpx."""

    def __init__(self, base_url: str, api_key: str):
        self._client = httpx.AsyncClient(
            base_url=base_url,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
            },
            timeout=30.0,
        )

    async def post_document(self, endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        response = await self._client.post(endpoint, json=payload)
        body = response.text

        if response.status_code not in (200, 201):
            raise RetailProError(status_code=response.status_code, response_body=body)

        try:
            return response.json()
        except Exception:
            raise RetailProError(status_code=response.status_code, response_body=body)

    async def close(self):
        await self._client.aclose()


async def get_client() -> RetailProClientBase:
    """
    Build a RetailPro client using live settings from the DB.
    Returns a fresh client on each call so config changes take effect immediately.
    """
    from app.db.settings_store import get_setting
    client_mode = await get_setting("retailpro_client", default="mock")
    if (client_mode or "mock").lower() == "real":
        base_url = await get_setting("retailpro_base_url", default="")
        api_key = await get_setting("retailpro_api_key", default="")
        return RealRetailProClient(base_url=base_url or "", api_key=api_key or "")
    return MockRetailProClient()


async def close_client():
    """No-op: clients are now short-lived and closed per-job."""
    pass
