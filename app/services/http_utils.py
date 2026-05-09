import asyncio
import logging

import httpx

logger = logging.getLogger(__name__)

_RETRYABLE = (
    httpx.RemoteProtocolError,
    httpx.ConnectError,
    httpx.ReadError,
    httpx.ConnectTimeout,
    httpx.ReadTimeout,
)


async def http_call_with_retry(
    fn,
    *args,
    retries: int = 10,
    backoff: float = 2.0,
    max_wait: float = 30.0,
    **kwargs,
):
    """
    Call an httpx coroutine (e.g. http.post, http.get, http.put) and automatically
    retry up to `retries` times on transient network errors, with capped exponential backoff.

    Backoff schedule (default, 10 retries): 2s, 4s, 8s, 16s, 30s, 30s, 30s, 30s, 30s, 30s
    (capped at max_wait=30s per attempt, ~4 min total wait before final failure).
    HTTP 4xx/5xx responses are NOT retried — only connection-level failures are.
    """
    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        try:
            return await fn(*args, **kwargs)
        except _RETRYABLE as exc:
            last_exc = exc
            if attempt < retries:
                wait = min(backoff * (2 ** attempt), max_wait)
                logger.warning(
                    "RetailPro request failed (attempt %d/%d): %s — retrying in %.0fs",
                    attempt + 1, retries + 1, exc, wait,
                )
                await asyncio.sleep(wait)
            else:
                logger.error(
                    "RetailPro request failed after %d attempts — giving up: %s",
                    retries + 1, exc,
                )
    raise last_exc
