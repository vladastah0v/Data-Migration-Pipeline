from typing import Annotated, Self

from pydantic import BaseModel, Field, SecretStr, StringConstraints, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import URL

NonEmptyText = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class DatabaseSettings(BaseModel):
    host: NonEmptyText
    port: Annotated[int, Field(ge=1, le=65535)]
    name: NonEmptyText
    user: NonEmptyText
    password: Annotated[SecretStr, Field(min_length=1)]

    @property
    def url(self) -> URL:
        return URL.create(
            "postgresql+asyncpg",
            username=self.user,
            password=self.password.get_secret_value(),
            host=self.host,
            port=self.port,
            database=self.name,
        )


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        extra="ignore",
        hide_input_in_errors=True,
    )

    legacy_db: DatabaseSettings
    target_db: DatabaseSettings
    db_timeout_seconds: Annotated[float, Field(gt=0, le=60)] = 5

    @model_validator(mode="after")
    def validate_database_endpoints(self) -> Self:
        legacy = self.legacy_db
        target = self.target_db
        if (legacy.host.casefold(), legacy.port, legacy.name) == (
            target.host.casefold(),
            target.port,
            target.name,
        ):
            raise ValueError("Для legacy и target укажите разные базы PostgreSQL.")
        return self
