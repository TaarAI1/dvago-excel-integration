import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.db.postgres import connect_db, close_db
from app.scheduler import scheduler, setup_scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


async def _seed_users():
    """Ensure the admin user from env vars exists with a fresh hash."""
    from sqlalchemy import select
    from app.db.postgres import get_session
    from app.models.user import User
    from app.core.security import get_password_hash
    import uuid

    new_hash = get_password_hash(settings.dashboard_password)

    async with get_session() as session:
        result = await session.execute(
            select(User).where(User.username == settings.dashboard_username)
        )
        existing = result.scalar_one_or_none()

    if not existing:
        async with get_session() as session:
            async with session.begin():
                session.add(User(
                    id=uuid.uuid4(),
                    username=settings.dashboard_username,
                    hashed_password=new_hash,
                    is_active=True,
                ))
        logger.info(f"Seeded admin user: {settings.dashboard_username}")
    else:
        # Re-hash with the current bcrypt implementation to fix any passlib leftovers
        async with get_session() as session:
            async with session.begin():
                user = await session.get(User, existing.id)
                user.hashed_password = new_hash
                user.is_active = True
        logger.info(f"Refreshed password hash for admin user: {settings.dashboard_username}")


async def _seed_settings():
    """Populate app_settings with env-var defaults for any missing keys."""
    from app.db.settings_store import seed_defaults
    env_overrides = {
        "ftp_host": settings.ftp_host,
        "ftp_port": str(settings.ftp_port),
        "ftp_user": settings.ftp_user,
        "ftp_password": settings.ftp_password,
        "ftp_import_path": settings.ftp_base_path,
        "retailpro_base_url": settings.retailpro_base_url,
        "retailpro_api_key": settings.retailpro_api_key,
        "retailpro_client": settings.retailpro_client,
        "document_type_endpoints": settings.document_type_endpoints,
        "document_type_field_maps": settings.document_type_field_maps,
        "poll_cron_schedule": settings.poll_cron_schedule,
    }
    await seed_defaults(env_overrides)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up...")

    # ── Database ──────────────────────────────────────────────────────────
    try:
        await connect_db(settings.get_async_database_url())
    except Exception as exc:
        logger.error(f"DB connect failed: {exc}. App will start but DB ops will fail.")

    # ── Bootstrap data ────────────────────────────────────────────────────
    try:
        await _seed_users()
    except Exception as exc:
        logger.error(f"User seed failed: {exc}")

    try:
        await _seed_settings()
    except Exception as exc:
        logger.error(f"Settings seed failed: {exc}")

    # ── Scheduler ─────────────────────────────────────────────────────────
    poll_cron    = settings.poll_cron_schedule
    sales_cron   = "0 2 * * *"
    sales_cron_2 = ""
    try:
        from app.db.settings_store import get_setting
        poll_cron    = (await get_setting("poll_cron_schedule")) or poll_cron
        sales_cron   = (await get_setting("sales_export_cron"))  or sales_cron
        sales_cron_2 = (await get_setting("sales_export_cron_2") or "")
    except Exception as exc:
        logger.warning(f"Could not load cron from DB, using defaults: {exc}")

    try:
        setup_scheduler(poll_cron, sales_cron, sales_cron_2)
        scheduler.start()
        logger.info(f"Scheduler started. FTP cron: {poll_cron}, Sales cron: {sales_cron}, Sales cron 2: {sales_cron_2 or 'not set'}")
    except Exception as exc:
        logger.error(f"Scheduler start failed: {exc}")

    logger.info("Startup complete — accepting requests.")
    yield

    # ── Shutdown ──────────────────────────────────────────────────────────
    logger.info("Shutting down...")
    try:
        scheduler.shutdown(wait=False)
    except Exception:
        pass
    try:
        await close_db()
    except Exception:
        pass


app = FastAPI(
    title="RetailPro Prism Integration",
    description="CSV to RetailPro sync via FTP polling — PostgreSQL backend",
    version="3.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from app.api.routes import auth, schedule, process, documents, logs, stream, health
from app.api.routes import users, settings as settings_routes, sales_export, item_master, qty_adjustment
from app.api.routes import price_adjustment, network

app.include_router(auth.router)
app.include_router(users.router)
app.include_router(settings_routes.router)
app.include_router(sales_export.router)
app.include_router(schedule.router)
app.include_router(process.router)
app.include_router(documents.router)
app.include_router(logs.router)
app.include_router(stream.router)
app.include_router(health.router)
app.include_router(item_master.router)
app.include_router(qty_adjustment.router)
app.include_router(price_adjustment.router)
app.include_router(network.router)


@app.get("/")
async def root():
    return {"message": "RetailPro Integration API v3", "docs": "/docs"}
