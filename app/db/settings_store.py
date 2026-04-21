"""
DB-backed settings store.
All runtime configuration (FTP, Oracle, RetailPro, scheduler, sales export)
is stored in the app_settings table and editable via the Config UI.

DATABASE_URL and JWT_SECRET_KEY remain env-only and are never stored here.
"""
import logging
from typing import Optional

from sqlalchemy import select
from app.db.postgres import get_session
from app.models.app_setting import AppSetting
from app.core.timezone import now_pkt

logger = logging.getLogger(__name__)

# Master definition of every setting: key, category, label, default, is_sensitive
SETTING_DEFINITIONS = [
    # FTP (shared server, separate paths)
    ("ftp_host",                       "ftp",  "FTP Host",                        "localhost", False),
    ("ftp_port",                       "ftp",  "FTP Port",                        "21",        False),
    ("ftp_user",                       "ftp",  "FTP Username",                    "anonymous", False),
    ("ftp_password",                   "ftp",  "FTP Password",                    "",          True),
    ("ftp_import_path",                "ftp",  "Item Master Import Path",         "/",         False),
    ("ftp_qty_adjust_import_path",     "ftp",  "Quantity Adjust Import Path",     "/",         False),
    ("ftp_price_adj_import_path",      "ftp",  "Price Adjustment Import Path",    "/",         False),
    ("ftp_transfers_import_path",      "ftp",  "Transfers Import Path",           "/",         False),
    ("ftp_grn_import_path",            "ftp",  "GRN Import Path",                 "/",         False),
    ("ftp_export_path",                "ftp",  "FTP Export Path",                 "/exports",  False),
    # RetailPro
    ("retailpro_base_url",        "retailpro",    "RetailPro Base URL",          "http://your-retailpro-server", False),
    ("retailpro_username",        "retailpro",    "RetailPro Username",          "",             False),
    ("retailpro_password",        "retailpro",    "RetailPro Password",          "",             True),
    ("retailpro_api_key",         "retailpro",    "RetailPro API Key",           "mock-key",     True),
    ("retailpro_client",          "retailpro",    "Client Mode (mock/real)",     "mock",         False),
    ("document_type_endpoints",   "retailpro",    "Document Type Endpoints (JSON)",
     '{"item_master": "/items", "receiving_voucher": "/receiving", "inventory_adjustment": "/inventory"}', False),
    ("document_type_field_maps",  "retailpro",    "Document Field Maps (JSON)",  "{}",           False),
    # Oracle DB
    ("oracle_host",               "oracle",       "Oracle Host",                 "",             False),
    ("oracle_port",               "oracle",       "Oracle Port",                 "1521",         False),
    ("oracle_service_name",       "oracle",       "Oracle Service Name",         "",             False),
    ("oracle_username",           "oracle",       "Oracle Username",             "",             False),
    ("oracle_password",           "oracle",       "Oracle Password",             "",             True),
    # Scheduler
    ("poll_cron_schedule",        "scheduler",    "FTP Import Cron",             "*/15 * * * *", False),
    ("sales_export_cron",         "scheduler",    "Sales Export Cron",           "0 2 * * *",    False),
    # Sales Export
    ("sales_export_sql",              "sales_export", "Sales SQL Query",              "SELECT * FROM sales WHERE ROWNUM <= 1000", False),
    ("sales_export_filename_prefix",  "sales_export", "Output Filename Prefix",       "sales_export", False),
    # Sales Export — dedicated Oracle connection (separate from the shared Oracle DB)
    ("sales_oracle_host",             "sales_export", "Oracle Host (Sales Export)",   "",     False),
    ("sales_oracle_port",             "sales_export", "Oracle Port (Sales Export)",   "1521", False),
    ("sales_oracle_service_name",     "sales_export", "Service Name (Sales Export)",  "",     False),
    ("sales_oracle_username",         "sales_export", "Username (Sales Export)",      "",     False),
    ("sales_oracle_password",         "sales_export", "Password (Sales Export)",      "",     True),
    # SMTP
    ("smtp_host",                 "smtp",         "SMTP Host",                   "",             False),
    ("smtp_port",                 "smtp",         "SMTP Port",                   "587",          False),
    ("smtp_username",             "smtp",         "SMTP Username",               "",             False),
    ("smtp_password",             "smtp",         "SMTP Password",               "",             True),
    ("smtp_use_tls",              "smtp",         "Use TLS",                     "true",         False),
    ("smtp_from_email",           "smtp",         "From Email",                  "",             False),
    ("smtp_to_email",             "smtp",         "To Email",                    "",             False),
    ("smtp_reply_to",             "smtp",         "Reply To",                    "",             False),
    ("smtp_cc_email",             "smtp",         "CC Email",                    "",             False),
]


async def seed_defaults(env_overrides: dict) -> None:
    """
    Insert settings that don't yet exist in the DB.
    env_overrides: dict of key → value from environment variables (used as initial defaults).
    """
    async with get_session() as session:
        async with session.begin():
            for key, category, label, default_val, is_sensitive in SETTING_DEFINITIONS:
                existing = await session.get(AppSetting, key)
                if existing is None:
                    value = env_overrides.get(key, default_val)
                    session.add(AppSetting(
                        key=key,
                        value=value if value is not None else default_val,
                        category=category,
                        label=label,
                        is_sensitive=is_sensitive,
                        updated_at=now_pkt(),
                    ))
    logger.info("App settings seeded.")


async def get_setting(key: str, default: Optional[str] = None) -> Optional[str]:
    """Read a single setting value from the DB."""
    try:
        async with get_session() as session:
            row = await session.get(AppSetting, key)
            return row.value if row else default
    except Exception as exc:
        logger.warning(f"get_setting({key}) failed: {exc}")
        return default


async def get_settings_by_category(category: str) -> dict:
    """Return all settings in a category as {key: {value, label, is_sensitive}}."""
    async with get_session() as session:
        result = await session.execute(
            select(AppSetting).where(AppSetting.category == category)
        )
        rows = result.scalars().all()
    return {
        r.key: {
            "value": r.value,
            "label": r.label,
            "is_sensitive": r.is_sensitive,
            "updated_at": r.updated_at.isoformat() if r.updated_at else None,
        }
        for r in rows
    }


async def get_all_settings() -> dict:
    """Return all settings grouped by category."""
    categories = {d[1] for d in SETTING_DEFINITIONS}
    result = {}
    for cat in categories:
        result[cat] = await get_settings_by_category(cat)
    return result


async def update_settings(updates: dict) -> None:
    """Bulk-update setting values. updates = {key: new_value}."""
    async with get_session() as session:
        async with session.begin():
            for key, value in updates.items():
                row = await session.get(AppSetting, key)
                if row is not None:
                    row.value = str(value)
                    row.updated_at = now_pkt()
    logger.info(f"Updated {len(updates)} settings.")
