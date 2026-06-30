import asyncio
import logging

import httpx

logger = logging.getLogger(__name__)

# Errors that are safe to retry only when the request never reached the server
# (connection-phase failures).  These are safe for ALL HTTP verbs.
_CONNECT_RETRYABLE = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
)

# Errors that are safe to retry only for idempotent verbs (GET, PUT, DELETE).
# They must NOT be retried for POST because the server may have already
# processed the request before the read failed.
_READ_RETRYABLE = (
    httpx.RemoteProtocolError,
    httpx.ReadError,
    httpx.ReadTimeout,
)


async def http_call_with_retry(
    fn,
    *args,
    retries: int = 10,
    backoff: float = 2.0,
    max_wait: float = 30.0,
    idempotent: bool = True,
    **kwargs,
):
    """
    Call an httpx coroutine (e.g. http.post, http.get, http.put) and automatically
    retry up to `retries` times on transient network errors, with capped exponential backoff.

    Backoff schedule (default, 10 retries): 2s, 4s, 8s, 16s, 30s, 30s, 30s, 30s, 30s, 30s
    (capped at max_wait=30s per attempt, ~4 min total wait before final failure).

    idempotent=True  (default, use for GET / PUT / DELETE):
        Retries on both connect-phase AND read-phase failures.

    idempotent=False (use for POST calls that create/append data):
        Only retries on connect-phase failures (ConnectError, ConnectTimeout).
        Read-phase failures (ReadTimeout, ReadError, RemoteProtocolError) are NOT
        retried because the server may have already processed the request.
    """
    retryable = _CONNECT_RETRYABLE + (_READ_RETRYABLE if idempotent else ())
    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        try:
            return await fn(*args, **kwargs)
        except retryable as exc:
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

