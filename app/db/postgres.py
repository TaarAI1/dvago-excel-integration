from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
import logging

logger = logging.getLogger(__name__)

engine = None
AsyncSessionFactory: async_sessionmaker[AsyncSession] = None


class Base(DeclarativeBase):
    pass


async def connect_db(database_url: str):
    global engine, AsyncSessionFactory

    engine = create_async_engine(
        database_url,
        echo=False,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
    )
    AsyncSessionFactory = async_sessionmaker(engine, expire_on_commit=False)

    # Create all tables if they don't exist
    async with engine.begin() as conn:
        import app.models  # noqa: registers all models with Base.metadata
        await conn.run_sync(Base.metadata.create_all)
        # Additive column migrations — safe to run on every startup
        await conn.execute(text(
            "ALTER TABLE sales_export_runs ADD COLUMN IF NOT EXISTS error_message TEXT"
        ))
        # price_adjustment_docs — new columns
        await conn.execute(text(
            "ALTER TABLE price_adjustment_docs ADD COLUMN IF NOT EXISTS note VARCHAR(500)"
        ))
        await conn.execute(text(
            "ALTER TABLE price_adjustment_docs ADD COLUMN IF NOT EXISTS api_comment_payload JSONB"
        ))
        await conn.execute(text(
            "ALTER TABLE price_adjustment_docs ADD COLUMN IF NOT EXISTS api_comment_response JSONB"
        ))
        # qty_adjustment_docs — new columns
        await conn.execute(text(
            "ALTER TABLE qty_adjustment_docs ADD COLUMN IF NOT EXISTS note VARCHAR(500)"
        ))
        await conn.execute(text(
            "ALTER TABLE qty_adjustment_docs ADD COLUMN IF NOT EXISTS api_comment_payload JSONB"
        ))
        await conn.execute(text(
            "ALTER TABLE qty_adjustment_docs ADD COLUMN IF NOT EXISTS api_comment_response JSONB"
        ))
        # transfer_slip_docs — verify step columns
        await conn.execute(text(
            "ALTER TABLE transfer_slip_docs ADD COLUMN IF NOT EXISTS api_verify_get_response JSONB"
        ))
        await conn.execute(text(
            "ALTER TABLE transfer_slip_docs ADD COLUMN IF NOT EXISTS api_verify_payload JSONB"
        ))
        await conn.execute(text(
            "ALTER TABLE transfer_slip_docs ADD COLUMN IF NOT EXISTS api_verify_response JSONB"
        ))
        # transfer_slip_docs + grn_docs — full error traceback
        await conn.execute(text(
            "ALTER TABLE transfer_slip_docs ADD COLUMN IF NOT EXISTS error_traceback TEXT"
        ))
        await conn.execute(text(
            "ALTER TABLE grn_docs ADD COLUMN IF NOT EXISTS error_traceback TEXT"
        ))
        # sales_export_stores — file type column
        await conn.execute(text(
            "ALTER TABLE sales_export_stores ADD COLUMN IF NOT EXISTS file_type VARCHAR(30) DEFAULT 'sales'"
        ))
        # raw_rows for retry support across all *_docs tables
        await conn.execute(text(
            "ALTER TABLE qty_adjustment_docs ADD COLUMN IF NOT EXISTS raw_rows JSONB"
        ))
        await conn.execute(text(
            "ALTER TABLE price_adjustment_docs ADD COLUMN IF NOT EXISTS raw_rows JSONB"
        ))
        await conn.execute(text(
            "ALTER TABLE transfer_slip_docs ADD COLUMN IF NOT EXISTS raw_rows JSONB"
        ))
        await conn.execute(text(
            "ALTER TABLE grn_docs ADD COLUMN IF NOT EXISTS raw_rows JSONB"
        ))

    logger.info("Connected to PostgreSQL and tables ensured.")


async def close_db():
    global engine
    if engine:
        await engine.dispose()
        logger.info("PostgreSQL connection closed.")


def get_session() -> AsyncSession:
    """Return a new async session. Use as: async with get_session() as session:"""
    return AsyncSessionFactory()
