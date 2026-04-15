from fastapi import APIRouter
from app.db.postgres import engine
from app.scheduler import scheduler

router = APIRouter(prefix="/api", tags=["health"])


@router.get("/health")
async def health_check():
    db_ok = False
    try:
        if engine:
            async with engine.connect() as conn:
                await conn.execute(__import__("sqlalchemy").text("SELECT 1"))
            db_ok = True
    except Exception:
        pass

    return {
        "status": "ok" if db_ok else "degraded",
        "database": "connected" if db_ok else "disconnected",
        "scheduler": "running" if scheduler.running else "stopped",
    }
