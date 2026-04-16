"""
RetailPro authentication service.

Handles the 3-step nonce challenge and returns an Auth-Session token.
Call get_auth_session() once per import batch — do not call per row.
"""
import logging
from typing import Optional

import httpx

logger = logging.getLogger(__name__)


async def get_auth_session(
    base_url: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> str:
    """
    Authenticate with RetailPro Prism using the 3-step nonce challenge.

    Step 1 – GET /v1/rest/auth              → extract Auth-Nonce header
    Step 2 – compute Auth-Nonce-Response    = ((nonce // 13) % 99999) * 17
    Step 3 – GET /v1/rest/auth?pwd=&usr=    → extract Auth-Session header

    Parameters can be passed explicitly or are read from app_settings if omitted.
    Returns the Auth-Session token string.
    Raises RuntimeError on any failure.
    """
    if not all([base_url, username, password]):
        from app.db.settings_store import get_setting
        base_url = base_url or (await get_setting("retailpro_base_url")) or ""
        username = username or (await get_setting("retailpro_username")) or ""
        password = password or (await get_setting("retailpro_password")) or ""

    if not base_url:
        raise RuntimeError("RetailPro base_url is not configured in settings.")

    base = base_url.rstrip("/")
    auth_url = f"{base}/v1/rest/auth"

    async with httpx.AsyncClient(timeout=15.0, verify=False, follow_redirects=True) as client:
        # ── Step 1 ────────────────────────────────────────────────────────────
        step1 = await client.get(auth_url)
        nonce_raw = step1.headers.get("Auth-Nonce") or step1.headers.get("auth-nonce")
        if nonce_raw is None:
            raise RuntimeError(
                f"Auth-Nonce header missing (HTTP {step1.status_code}). "
                f"Response: {step1.text[:300]}"
            )
        try:
            auth_nonce = int(nonce_raw)
        except ValueError:
            raise RuntimeError(f"Auth-Nonce is not an integer: {nonce_raw!r}")

        # ── Step 2 ────────────────────────────────────────────────────────────
        auth_nonce_response = ((auth_nonce // 13) % 99999) * 17

        # ── Step 3 ────────────────────────────────────────────────────────────
        step3 = await client.get(
            f"{auth_url}?pwd={password}&usr={username}",
            headers={
                "Auth-Nonce": str(auth_nonce),
                "Auth-Nonce-Response": str(auth_nonce_response),
            },
        )
        session = (
            step3.headers.get("Auth-Session")
            or step3.headers.get("auth-session")
        )
        if not session:
            raise RuntimeError(
                f"Auth-Session header missing (HTTP {step3.status_code}). "
                f"Check credentials. Response: {step3.text[:300]}"
            )

    logger.info("RetailPro Auth-Session obtained.")
    return session
