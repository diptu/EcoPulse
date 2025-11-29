"""Configuration settings for the Ingestion microservice."""

from pathlib import Path
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent.parent
ENV_FILE = BASE_DIR / ".env"


class Settings(BaseSettings):
    """App settings loaded from environment variables."""

    # General App Config
    SERVICE_NAME: str = "Analytics Service"
    SERVICE_VERSION: str = "0.0.1"
    APP_ENV: str = "development"
    DEBUG: bool = True

    # Database
    DATABASE_URL: Optional[str] = None
    OPENELECTRICITY_API_KEY: Optional[str] = None

    # Pydantic v2 config
    model_config = SettingsConfigDict(env_file=str(ENV_FILE), extra="ignore")


settings = Settings()

# Require DB on startup
if not settings.DATABASE_URL:
    raise ValueError("DATABASE_URL must be set in environment variables")
