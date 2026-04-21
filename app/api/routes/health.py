from fastapi import APIRouter
import app.db.postgres as _pg
from app.scheduler import scheduler

router = APIRouter(prefix="/api", tags=["health"])


@router.get("/health")
async def health_check():
    """Lightweight health probe. Always returns HTTP 200."""
    db_ok = False
    try:
        engine = _pg.engine
        if engine is not None:
            from sqlalchemy import text
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
            db_ok = True
    except Exception:
        pass

    return {
        "status": "ok" if db_ok else "degraded",
        "database": "connected" if db_ok else "disconnected",
        "scheduler": "running" if scheduler.running else "stopped",
    }
