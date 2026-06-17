from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "pricing-platform-sdk"
    database_url: str = "postgresql://postgres:postgres@localhost:5432/pricing"

    model_config = SettingsConfigDict(env_file=".env", env_prefix="", extra="ignore")


settings = Settings()
