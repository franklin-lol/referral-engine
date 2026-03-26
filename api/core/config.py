import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = os.getenv(
        "DATABASE_URL",
        "postgresql://re_user:re_pass@localhost:5432/referral_engine",
    )
    engine_config_path: str = os.getenv("ENGINE_CONFIG", "config.example.yaml")

    class Config:
        env_file = ".env"
