from abc import ABC, abstractmethod
from typing import Dict, Any
import httpx
import logging
import json

from app.core.config import settings

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
        await asyncio.sleep(0.05)  # simulate small latency
        mock_sid = f"MOCK-{uuid.uuid4().hex[:8].upper()}"
        logger.debug(f"[MOCK] POST {endpoint} → sid={mock_sid}")
        return {"data": [{"sid": mock_sid}], "status": "success"}


class RealRetailProClient(RetailProClientBase):
    """Real HTTP client using httpx. A single shared client is reused across calls."""

    def __init__(self):
        self._client = httpx.AsyncClient(
            base_url=settings.retailpro_base_url,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {settings.retailpro_api_key}",
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


def get_retailpro_client() -> RetailProClientBase:
    """Factory: returns mock or real client based on RETAILPRO_CLIENT env var."""
    if settings.retailpro_client.lower() == "real":
        return RealRetailProClient()
    return MockRetailProClient()


# Singleton shared across the app lifecycle
_client_instance: RetailProClientBase = None


def get_client() -> RetailProClientBase:
    global _client_instance
    if _client_instance is None:
        _client_instance = get_retailpro_client()
    return _client_instance


async def close_client():
    global _client_instance
    if _client_instance and isinstance(_client_instance, RealRetailProClient):
        await _client_instance.close()
    _client_instance = None
