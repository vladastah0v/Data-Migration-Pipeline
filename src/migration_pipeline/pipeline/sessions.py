from collections.abc import Callable
from contextlib import AbstractAsyncContextManager, AsyncExitStack, suppress
from uuid import UUID

from asyncpg import InterfaceError, PostgresError
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from migration_pipeline.database.legacy import LegacySource
from migration_pipeline.database.schema import require_current_schema

# This session lock protects the entire target from concurrent migrations.
MIGRATION_LOCK_KEY = 581_564_284_644_221_779


class MigrationBusyError(RuntimeError):
    """На целевой базе уже работает другая миграция."""


class MigrationSessions:
    def __init__(
        self,
        engine: AsyncEngine,
        open_source: Callable[[UUID | None], AbstractAsyncContextManager[LegacySource]],
        expected_snapshot_id: UUID | None = None,
    ) -> None:
        self.engine = engine
        self.open_source = open_source
        self.snapshot_id = expected_snapshot_id
        self._source: LegacySource | None = None
        self._target: AsyncConnection | None = None
        self._source_stack = AsyncExitStack()

    async def source(self) -> LegacySource:
        if self._source is None:
            self._source = await self._source_stack.enter_async_context(
                self.open_source(self.snapshot_id)
            )
            self.snapshot_id = self._source.snapshot.id
        return self._source

    async def target(self) -> AsyncConnection:
        if self._target is not None and (self._target.closed or self._target.invalidated):
            await self.reset_target()
        if self._target is None:
            stack = AsyncExitStack()
            async with stack:
                connection = await stack.enter_async_context(self.engine.connect())
                async with connection.begin():
                    locked = await connection.scalar(
                        text("SELECT pg_try_advisory_lock(:key)"), {"key": MIGRATION_LOCK_KEY}
                    )
                    if not locked:
                        raise MigrationBusyError("В target уже выполняется другой запуск миграции.")
                    await require_current_schema(connection, "target")
                self._target = connection
                stack.pop_all()
        return self._target

    async def reset_source(self) -> None:
        self._source = None
        with suppress(OSError, SQLAlchemyError, PostgresError, InterfaceError):
            await self._source_stack.aclose()

    async def reset_target(self) -> None:
        connection, self._target = self._target, None
        if connection is not None:
            with suppress(OSError, SQLAlchemyError, PostgresError, InterfaceError):
                await connection.close()

    async def close(self) -> None:
        try:
            await self.reset_source()
        finally:
            try:
                await self.reset_target()
            finally:
                await self.engine.dispose()
