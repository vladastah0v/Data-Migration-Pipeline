import asyncio
from typing import cast

from alembic import context
from alembic.util import CommandError
from sqlalchemy import Connection

from migration_pipeline.config import DatabaseSide, SchemaSettings, Settings
from migration_pipeline.database.schema import with_schema_connection
from migration_pipeline.models import legacy, migration, target

config = context.config
if config.config_ini_section not in {"legacy", "target"}:
    raise CommandError("Выберите конфигурацию Alembic: -n legacy или -n target.")

side = cast(DatabaseSide, config.config_ini_section)
metadata = (
    legacy.LegacyBase.metadata
    if side == "legacy"
    else (target.TargetBase.metadata, migration.MigrationBase.metadata)
)


def run_online(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=metadata, compare_server_default=True)
    with context.begin_transaction():
        context.run_migrations()


if context.is_offline_mode():
    if side == "target" and "app_role" not in config.attributes:
        config.attributes["app_role"] = Settings().target_db.user
    context.configure(dialect_name="postgresql", target_metadata=metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()
elif provided_connection := config.attributes.get("connection"):
    run_online(provided_connection)
else:
    settings = SchemaSettings()
    config.attributes["app_role"] = settings.target_db.user
    asyncio.run(with_schema_connection(settings, side, run_online))
