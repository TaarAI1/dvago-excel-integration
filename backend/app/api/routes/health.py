from fastapi import APIRouter
from app.db.mongodb import get_db, get_client
from app.scheduler import scheduler

router = APIRouter(prefix="/api", tags=["health"])


@router.get("/health")
async def health_check():
    """System health check — DB connectivity and scheduler status."""
    db_ok = False
    try:
        client = get_client()
        if client:
            await client.admin.command("ping")
            db_ok = True
    except Exception:
        pass

    return {
        "status": "ok" if db_ok else "degraded",
        "mongodb": "connected" if db_ok else "disconnected",
        "scheduler": "running" if scheduler.running else "stopped",
    }
