import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from functools import partial
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncConnection

from migration_pipeline.config import LegacySettings
from migration_pipeline.database.connections import create_database_engine
from migration_pipeline.database.schema import DatabaseSchemaError, require_current_schema
from migration_pipeline.models.legacy import LegacySourceSnapshot


class LegacySourceError(RuntimeError):
    """Настройки или состояние legacy не позволяют работать с источником."""


@dataclass(frozen=True, slots=True)
class SourceSnapshot:
    id: UUID
    sealed_at: datetime


@dataclass(frozen=True, slots=True)
class LegacySource:
    connection: AsyncConnection
    snapshot: SourceSnapshot


async def require_legacy_schema(connection: AsyncConnection) -> None:
    try:
        await require_current_schema(connection, "legacy")
    except DatabaseSchemaError as exc:
        raise LegacySourceError(str(exc)) from exc


async def read_source_snapshot(
    connection: AsyncConnection, *, expected_id: UUID | None = None
) -> SourceSnapshot:
    result = await connection.execute(
        select(LegacySourceSnapshot.snapshot_id, LegacySourceSnapshot.sealed_at).where(
            LegacySourceSnapshot.singleton == 1
        )
    )
    row = result.one_or_none()
    if row is None:
        raise LegacySourceError("В legacy отсутствует служебная запись снимка.")
    if row.snapshot_id is None:
        raise LegacySourceError("Снимок legacy ещё не зафиксирован.")
    if expected_id is not None and row.snapshot_id != expected_id:
        raise LegacySourceError(
            f"UUID снимка legacy изменился. Ожидался {expected_id}, получен {row.snapshot_id}."
        )
    return SourceSnapshot(row.snapshot_id, row.sealed_at)


@asynccontextmanager
async def open_legacy_source(
    settings: LegacySettings, *, expected_snapshot_id: UUID | None = None
) -> AsyncIterator[LegacySource]:
    engine = create_database_engine(
        settings.legacy_db,
        settings.db_timeout_seconds,
        json_deserializer=partial(json.loads, parse_float=Decimal),
    )
    try:
        async with engine.connect() as connection:
            await connection.execution_options(
                isolation_level="AUTOCOMMIT",
                schema_translate_map={None: "public"},
            )
            await require_legacy_schema(connection)
            snapshot = await read_source_snapshot(connection, expected_id=expected_snapshot_id)
            yield LegacySource(connection, snapshot)
    finally:
        await engine.dispose()
