import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.db.mongodb import connect_db, close_db
from app.services.retailpro_client import close_client
from app.scheduler import scheduler, setup_scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting up...")
    await connect_db()

    # Load cron from DB if previously configured, else use env default
    from app.db.mongodb import get_db
    db = get_db()
    stored = await db.system_config.find_one({"key": "poll_cron_schedule"})
    cron = stored["value"] if stored else settings.poll_cron_schedule

    setup_scheduler(cron)
    scheduler.start()
    logger.info(f"Scheduler started with cron: {cron}")

    yield

    # Shutdown
    logger.info("Shutting down...")
    scheduler.shutdown(wait=False)
    await close_client()
    await close_db()


app = FastAPI(
    title="RetailPro Prism Integration",
    description="CSV to RetailPro sync via FTP polling",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routers
from app.api.routes import auth, schedule, process, documents, logs, stream, health

app.include_router(auth.router)
app.include_router(schedule.router)
app.include_router(process.router)
app.include_router(documents.router)
app.include_router(logs.router)
app.include_router(stream.router)
app.include_router(health.router)


@app.get("/")
async def root():
    return {"message": "RetailPro Integration API", "docs": "/docs"}
