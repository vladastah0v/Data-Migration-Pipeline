from collections.abc import Callable

from alembic import command
from alembic.config import Config
from alembic.util import CommandError
from sqlalchemy import Connection, text

from migration_pipeline.config import DatabaseSide, SchemaSettings
from migration_pipeline.database.connections import POSTGRES_MAJOR, create_database_engine

# The lock coordinates concurrent schema changes within one PostgreSQL database.
SCHEMA_LOCK_KEY = 581_564_284_644_221_778


def revision_config(side: DatabaseSide) -> Config:
    config = Config(ini_section=side)
    config.set_main_option("script_location", "migration_pipeline:migrations")
    config.set_main_option("version_locations", f"migration_pipeline:migrations/{side}/versions")
    config.set_main_option("path_separator", "newline")
    return config


async def with_schema_connection(
    settings: SchemaSettings, side: DatabaseSide, apply: Callable[[Connection], None]
) -> None:
    engine = create_database_engine(settings.admin_database(side), settings.db_timeout_seconds)
    try:
        async with engine.begin() as connection:
            server_version = connection.dialect.server_version_info
            if server_version[0] != POSTGRES_MAJOR:
                raise CommandError(f"Для {side} требуется PostgreSQL {POSTGRES_MAJOR}.")
            await connection.execute(
                text("SELECT pg_advisory_xact_lock(:lock_key)"), {"lock_key": SCHEMA_LOCK_KEY}
            )
            await connection.run_sync(apply)
    finally:
        await engine.dispose()


async def upgrade_schema(settings: SchemaSettings, side: DatabaseSide) -> None:
    config = revision_config(side)
    config.attributes["app_role"] = settings.target_db.user

    def apply(connection: Connection) -> None:
        config.attributes["connection"] = connection
        command.upgrade(config, "head")

    await with_schema_connection(settings, side, apply)
