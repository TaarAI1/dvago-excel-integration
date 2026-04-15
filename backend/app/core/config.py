from pydantic_settings import BaseSettings
from pydantic import Field
import json


class Settings(BaseSettings):
    # PostgreSQL — Railway sets DATABASE_URL automatically when you add a Postgres plugin
    database_url: str = Field(
        default="postgresql+asyncpg://postgres:postgres@localhost:5432/retailpro_integration",
        alias="DATABASE_URL",
    )

    # FTP
    ftp_host: str = Field(default="localhost", alias="FTP_HOST")
    ftp_port: int = Field(default=21, alias="FTP_PORT")
    ftp_user: str = Field(default="anonymous", alias="FTP_USER")
    ftp_password: str = Field(default="", alias="FTP_PASSWORD")
    ftp_base_path: str = Field(default="/", alias="FTP_BASE_PATH")

    # RetailPro
    retailpro_base_url: str = Field(default="https://api.retailpro.example.com", alias="RETAILPRO_BASE_URL")
    retailpro_api_key: str = Field(default="mock-key", alias="RETAILPRO_API_KEY")
    retailpro_client: str = Field(default="mock", alias="RETAILPRO_CLIENT")

    # JWT
    jwt_secret_key: str = Field(default="change-me-in-production-use-long-random-string", alias="JWT_SECRET_KEY")
    jwt_algorithm: str = Field(default="HS256", alias="JWT_ALGORITHM")
    jwt_expire_minutes: int = Field(default=480, alias="JWT_EXPIRE_MINUTES")

    # Scheduler
    poll_cron_schedule: str = Field(default="*/15 * * * *", alias="POLL_CRON_SCHEDULE")

    # Dashboard credentials
    dashboard_username: str = Field(default="admin", alias="DASHBOARD_USERNAME")
    dashboard_password: str = Field(default="admin123", alias="DASHBOARD_PASSWORD")

    # Document type → endpoint mapping (JSON string)
    document_type_endpoints: str = Field(
        default='{"item_master": "/items", "receiving_voucher": "/receiving", "inventory_adjustment": "/inventory"}',
        alias="DOCUMENT_TYPE_ENDPOINTS",
    )

    # Field mappings per document type (JSON string)
    document_type_field_maps: str = Field(default="{}", alias="DOCUMENT_TYPE_FIELD_MAPS")

    model_config = {"env_file": ".env", "populate_by_name": True}

    def get_async_database_url(self) -> str:
        """Ensure URL uses asyncpg driver prefix."""
        url = self.database_url
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql+asyncpg://", 1)
        elif url.startswith("postgresql://") and "+asyncpg" not in url:
            url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
        return url

    def get_document_type_endpoints(self) -> dict:
        return json.loads(self.document_type_endpoints)

    def get_document_type_field_maps(self) -> dict:
        return json.loads(self.document_type_field_maps)


settings = Settings()
