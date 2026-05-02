from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    SECRET_KEY: SecretStr = SecretStr("dev-secret-key-change-in-production")
    UPLOAD_DIR: str = "uploaded_files"
    LOG_DIR: str = "logs"
    INVALID_ROWS_DIR: str = "invalid_rows"

    # ETL defaults
    DEFAULT_BATCH_SIZE: int = 10_000
    MAX_RETRIES: int = 3
    RETRY_DELAY_SECONDS: float = 2.0

    # Redis / Celery
    REDIS_URL: str = "redis://localhost:6379/0"
    CELERY_BROKER_URL: str = "redis://localhost:6379/0"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/1"


settings = Settings()
