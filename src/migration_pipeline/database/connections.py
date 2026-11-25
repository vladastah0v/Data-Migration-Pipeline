from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from migration_pipeline.config import DatabaseSettings


async def read_server_version(database: DatabaseSettings, timeout: float) -> int:
    engine = create_async_engine(
        database.url,
        connect_args={"timeout": timeout, "command_timeout": timeout},
        poolclass=NullPool,
        hide_parameters=True,
    )
    try:
        async with engine.connect() as connection:
            return int(await connection.scalar(text("SHOW server_version_num")))
    finally:
        await engine.dispose()
