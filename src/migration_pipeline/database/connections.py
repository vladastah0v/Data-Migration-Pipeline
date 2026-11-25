from collections.abc import Callable

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

from migration_pipeline.config import DatabaseSettings

POSTGRES_MAJOR = 16


def create_database_engine(
    database: DatabaseSettings,
    timeout: float,
    *,
    json_deserializer: Callable[[str], object] | None = None,
) -> AsyncEngine:
    return create_async_engine(
        database.url,
        connect_args={"timeout": timeout, "command_timeout": timeout},
        poolclass=NullPool,
        hide_parameters=True,
        json_deserializer=json_deserializer,
    )


async def read_server_version(database: DatabaseSettings, timeout: float) -> int:
    engine = create_database_engine(database, timeout)
    try:
        async with engine.connect() as connection:
            return int(await connection.scalar(text("SHOW server_version_num")))
    finally:
        await engine.dispose()
