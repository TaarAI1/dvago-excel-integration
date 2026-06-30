from fastapi import APIRouter, Query
import httpx
import asyncio

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


@router.get("/ping")
async def ping_host(
    host: str = Query(..., description="IP address or hostname to probe"),
    port: int = Query(80, description="TCP port to connect to"),
    timeout: float = Query(5.0, description="Connection timeout in seconds"),
):
    """
    Attempt a TCP connection to host:port and report whether it is reachable.
    Useful for verifying Railway can reach private/on-premise servers.
    """
    reachable = False
    error: str | None = None
    try:
        _, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=timeout,
        )
        writer.close()
        await writer.wait_closed()
        reachable = True
    except asyncio.TimeoutError:
        error = f"Timed out after {timeout}s — host is unreachable or port is firewalled"
    except ConnectionRefusedError:
        error = "Connection refused — host is reachable but port is closed"
    except OSError as exc:
        error = f"OS error: {exc}"

    return {
        "host": host,
        "port": port,
        "reachable": reachable,
        "error": error,
    }
