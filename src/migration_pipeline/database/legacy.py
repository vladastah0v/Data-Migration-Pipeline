import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from functools import partial
from uuid import UUID

from alembic.script import ScriptDirectory
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncConnection

from migration_pipeline.config import LegacySettings
from migration_pipeline.database.connections import POSTGRES_MAJOR, create_database_engine
from migration_pipeline.database.schema import revision_config
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
    if connection.dialect.server_version_info[0] != POSTGRES_MAJOR:
        raise LegacySourceError(f"Для legacy требуется PostgreSQL {POSTGRES_MAJOR}.")
    if await connection.scalar(text("SELECT to_regclass('public.alembic_version')")) is None:
        raise LegacySourceError("Схема legacy отсутствует. Выполните migration-pipeline init.")
    installed = frozenset(
        await connection.scalars(text("SELECT version_num FROM public.alembic_version"))
    )
    expected = frozenset(ScriptDirectory.from_config(revision_config("legacy")).get_heads())
    if installed != expected:
        raise LegacySourceError(
            "Схема legacy требует обновления. Выполните migration-pipeline init."
        )


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
