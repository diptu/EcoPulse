from pathlib import Path

from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict

# Load local .env if available
BASE_DIR = Path(__file__).resolve().parent.parent.parent
ENV_FILE = BASE_DIR / ".env"
load_dotenv(dotenv_path=ENV_FILE, override=False)


class Settings(BaseSettings):
    DATABASE_URL: str | None = None  # <-- optional
    SYNC_DATABASE_URL: str | None = None
    OPENELECTRICITY_API_KEY: str | None = None
    BATCH_SIZE: int = 100
    FACILITY_SIZE: int = 25

    model_config = SettingsConfigDict(extra="ignore", env_file=None)


settings = Settings()
settings = Settings()
