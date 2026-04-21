from fastapi import APIRouter
import httpx

router = APIRouter(prefix="/api/network", tags=["network"])

_IP_LOOKUP_URLS = [
    "https://api.ipify.org?format=json",
    "https://api64.ipify.org?format=json",
    "https://ifconfig.me/ip",
]


@router.get("/egress-ip")
async def egress_ip():
    """Return the server's current egress public IP address."""
    async with httpx.AsyncClient(timeout=10) as client:
        for url in _IP_LOOKUP_URLS:
            try:
                resp = await client.get(url)
                resp.raise_for_status()
                if "ipify" in url:
                    data = resp.json()
                    return {"ip": data.get("ip")}
                else:
                    return {"ip": resp.text.strip()}
            except Exception:
                continue
    return {"ip": None, "error": "Could not determine egress IP"}
