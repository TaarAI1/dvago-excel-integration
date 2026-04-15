from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional
import json


class Settings(BaseSettings):
    # MongoDB
    mongodb_url: str = Field(default="mongodb://localhost:27017", alias="MONGODB_URL")
    mongodb_db_name: str = Field(default="retailpro_integration", alias="MONGODB_DB_NAME")

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
    # e.g. '{"item_master": "/items", "receiving_voucher": "/receiving", "inventory_adjustment": "/inventory"}'
    document_type_endpoints: str = Field(
        default='{"item_master": "/items", "receiving_voucher": "/receiving", "inventory_adjustment": "/inventory"}',
        alias="DOCUMENT_TYPE_ENDPOINTS",
    )

    # Field mappings per document type (JSON string)
    # e.g. '{"item_master": {"CSV_COL": "mongo_field"}, ...}'
    document_type_field_maps: str = Field(default="{}", alias="DOCUMENT_TYPE_FIELD_MAPS")

    model_config = {"env_file": ".env", "populate_by_name": True}

    def get_document_type_endpoints(self) -> dict:
        return json.loads(self.document_type_endpoints)

    def get_document_type_field_maps(self) -> dict:
        return json.loads(self.document_type_field_maps)


settings = Settings()
