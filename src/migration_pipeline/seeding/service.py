from collections.abc import Callable
from dataclasses import dataclass
from itertools import islice
from uuid import UUID

from sqlalchemy import exists, func, select, text, update
from sqlalchemy.ext.asyncio import AsyncConnection

from migration_pipeline.config import LegacyAdminSettings
from migration_pipeline.database.connections import create_database_engine
from migration_pipeline.database.legacy import LegacySourceError, require_legacy_schema
from migration_pipeline.database.schema import SCHEMA_LOCK_KEY
from migration_pipeline.models.legacy import LegacySourceSnapshot
from migration_pipeline.seeding.data import LegacyDataGenerator, SeedOptions, SourceRows

type SeedProgress = Callable[[str, int, int], None]


class SeedError(RuntimeError):
    """Предсказуемая ошибка подготовки исходной базы."""


@dataclass(frozen=True, slots=True)
class TableSummary:
    name: str
    rows: int
    invalid_rows: int


@dataclass(frozen=True, slots=True)
class SeedResult:
    snapshot_id: UUID
    tables: tuple[TableSummary, ...]
    duplicate_contacts: int


async def seed_legacy(
    settings: LegacyAdminSettings, options: SeedOptions, progress: SeedProgress
) -> SeedResult:
    engine = create_database_engine(settings.legacy_admin_database, settings.db_timeout_seconds)
    try:
        async with engine.begin() as connection:
            return await _seed_source(connection, options, progress)
    finally:
        await engine.dispose()


async def _seed_source(
    connection: AsyncConnection, options: SeedOptions, progress: SeedProgress
) -> SeedResult:
    generator = LegacyDataGenerator(options)
    tables = generator.tables()
    await _require_open_source(connection, tables)

    # COPY uses the same asyncpg connection and transaction as the SQL checks and snapshot update.
    driver = (await connection.get_raw_connection()).driver_connection
    summaries = []
    for entry in tables:
        progress(entry.table.name, 0, entry.count)
        copied = 0
        while batch := list(islice(entry.rows, options.batch_size)):
            await driver.copy_records_to_table(
                entry.table.name, schema_name="public", columns=entry.columns, records=batch
            )
            copied += len(batch)
            progress(entry.table.name, copied, entry.count)

        actual = await connection.scalar(select(func.count()).select_from(entry.table))
        if actual != entry.count:
            raise SeedError(
                f"{entry.table.name} — ожидалось {entry.count} строк, получено {actual}."
            )
        summaries.append(TableSummary(entry.table.name, actual, entry.invalid_count))

    snapshot_id = await connection.scalar(
        update(LegacySourceSnapshot)
        .where(LegacySourceSnapshot.singleton == 1)
        .values(snapshot_id=func.gen_random_uuid(), sealed_at=func.clock_timestamp())
        .returning(LegacySourceSnapshot.snapshot_id)
    )
    return SeedResult(snapshot_id, tuple(summaries), generator.duplicate_contacts)


async def _require_open_source(connection: AsyncConnection, tables: tuple[SourceRows, ...]) -> None:
    await connection.execute(
        text("SELECT pg_advisory_xact_lock(:lock_key)"), {"lock_key": SCHEMA_LOCK_KEY}
    )
    try:
        await require_legacy_schema(connection)
    except LegacySourceError as exc:
        raise SeedError(str(exc)) from exc

    snapshot = await connection.execute(
        select(LegacySourceSnapshot.snapshot_id)
        .where(LegacySourceSnapshot.singleton == 1)
        .with_for_update()
    )
    row = snapshot.one_or_none()
    if row is None:
        raise SeedError("В legacy отсутствует служебная запись снимка.")
    if row.snapshot_id is not None:
        raise SeedError("Снимок legacy уже зафиксирован. Для нового набора нужна отдельная база.")
    for entry in tables:
        if await connection.scalar(select(exists().select_from(entry.table))):
            raise SeedError(
                f"Таблица {entry.table.name} уже содержит данные. Нужна пустая legacy-БД."
            )
