"""EP worker configuration — reads from environment variables."""

from __future__ import annotations

from functools import lru_cache

from pydantic import AliasChoices, Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class EPSettings(BaseSettings):
    """Database settings for the execution-plane worker."""

    model_config = SettingsConfigDict(extra="ignore")

    # Database — accepts either APP_DATABASE_URL or DATABASE_URL
    database_url: str = Field(
        validation_alias=AliasChoices("APP_DATABASE_URL", "DATABASE_URL"),
    )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def database_url_asyncpg(self) -> str:
        """asyncpg-compatible URL (strips the +asyncpg SQLAlchemy driver prefix)."""
        return self.database_url.replace("postgresql+asyncpg://", "postgresql://")


@lru_cache
def get_ep_settings() -> EPSettings:
    """Load and cache execution-plane settings from the environment."""
    return EPSettings()
