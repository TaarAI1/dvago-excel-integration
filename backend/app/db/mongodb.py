from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo import ASCENDING, DESCENDING
from app.core.config import settings
import logging

logger = logging.getLogger(__name__)

_client: AsyncIOMotorClient = None
_db: AsyncIOMotorDatabase = None


def get_client() -> AsyncIOMotorClient:
    return _client


def get_db() -> AsyncIOMotorDatabase:
    return _db


async def connect_db():
    global _client, _db
    _client = AsyncIOMotorClient(settings.mongodb_url)
    _db = _client[settings.mongodb_db_name]
    await _create_indexes()
    logger.info("Connected to MongoDB and indexes ensured.")


async def close_db():
    global _client
    if _client:
        _client.close()
        logger.info("MongoDB connection closed.")


async def _create_indexes():
    db = _db

    # documents collection
    await db.documents.create_index(
        [("document_type", ASCENDING), ("posted", ASCENDING), ("has_error", ASCENDING)],
        name="doc_type_status",
    )
    await db.documents.create_index(
        [("posted", ASCENDING), ("created_at", DESCENDING)],
        name="posted_created",
    )
    await db.documents.create_index(
        [("has_error", ASCENDING), ("posted", ASCENDING)],
        name="error_posted",
    )

    # activity_logs collection
    await db.activity_logs.create_index([("timestamp", DESCENDING)], name="timestamp_desc")
    await db.activity_logs.create_index(
        [("activity_type", ASCENDING), ("timestamp", DESCENDING)],
        name="type_timestamp",
    )

    # ftp_seen_files collection
    await db.ftp_seen_files.create_index([("filename", ASCENDING)], unique=True, name="filename_unique")

    # system_config collection
    await db.system_config.create_index([("key", ASCENDING)], unique=True, name="config_key_unique")
