import asyncio
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from itertools import batched

from asyncpg import PostgresError
from sqlalchemy import Text, cast, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection

from migration_pipeline.database.schema import SCHEMA_LOCK_KEY
from migration_pipeline.loaders.batch import insert_rows
from migration_pipeline.loaders.copy import copy_rows
from migration_pipeline.loaders.json_values import equivalent_json
from migration_pipeline.loaders.records import (
    MAX_BIND_PARAMETERS,
    LoadConflict,
    LoadError,
    TableSpec,
    TargetRow,
    groups_for_batch,
)
from migration_pipeline.loaders.upsert import upsert_rows
from migration_pipeline.transformers.base import TransformedRecord
from migration_pipeline.transformers.customers import CustomerData
from migration_pipeline.transformers.orders import OrderData
from migration_pipeline.transformers.payments import PaymentData
from migration_pipeline.transformers.status_history import StatusEventData


class LoadStrategy(StrEnum):
    INSERT = "insert"
    BATCH_INSERT = "batch-insert"
    UPSERT = "upsert"
    COPY = "copy"


class ConflictPolicy(StrEnum):
    VERIFY = "verify"
    UPDATE = "update"


@dataclass(frozen=True, slots=True)
class TableLoadResult:
    table: str
    inserted: int
    updated: int
    unchanged: int


@dataclass(frozen=True, slots=True)
class LoadResult:
    tables: tuple[TableLoadResult, ...]

    @property
    def inserted(self) -> int:
        return self.tables[0].inserted if self.tables else 0

    @property
    def updated(self) -> int:
        return self.tables[0].updated if self.tables else 0

    @property
    def unchanged(self) -> int:
        return self.tables[0].unchanged if self.tables else 0


async def load_batch(
    connection: AsyncConnection,
    records: Sequence[TransformedRecord],
    *,
    strategy: LoadStrategy,
    conflict_policy: ConflictPolicy = ConflictPolicy.VERIFY,
    transaction_timeout: float = 60,
) -> LoadResult:
    """Записать партию внутри транзакции вызывающей стороны."""
    strategy = LoadStrategy(strategy)
    conflict_policy = ConflictPolicy(conflict_policy)
    if not connection.in_transaction():
        raise ValueError("Для загрузки партии требуется открытая транзакция target.")
    if not math.isfinite(transaction_timeout) or transaction_timeout <= 0:
        raise ValueError("Таймаут загрузки партии должен быть положительным конечным числом.")
    if conflict_policy is ConflictPolicy.UPDATE and strategy in {
        LoadStrategy.INSERT,
        LoadStrategy.BATCH_INSERT,
    }:
        raise ValueError("Политика update доступна для UPSERT и COPY.")
    if not records:
        return LoadResult(())
    record_type = type(records[0])
    if record_type not in (CustomerData, OrderData, PaymentData, StatusEventData) or any(
        type(record) is not record_type for record in records
    ):
        raise ValueError("Партия должна содержать нормализованные записи одной сущности.")

    results = []
    try:
        async with asyncio.timeout(transaction_timeout), connection.begin_nested():
            await connection.execute(
                text("SELECT pg_advisory_xact_lock(:lock_key)"), {"lock_key": SCHEMA_LOCK_KEY}
            )
            async for spec, rows in groups_for_batch(connection, records):
                results.append(
                    await _write_group(connection, spec, rows, strategy, conflict_policy)
                )
    except (IntegrityError, PostgresError) as exc:
        cause = exc.orig if isinstance(exc, IntegrityError) else exc
        state = getattr(cause, "sqlstate", None)
        if state == "23505":
            raise LoadConflict(
                "PostgreSQL обнаружил конфликт уникального ключа в партии."
            ) from None
        if state and state.startswith("23"):
            raise LoadError(
                f"PostgreSQL отклонил партию по ограничению (SQLSTATE {state})."
            ) from None
        raise
    return LoadResult(tuple(results))


async def _write_group(
    connection: AsyncConnection,
    spec: TableSpec,
    rows: list[TargetRow],
    strategy: LoadStrategy,
    policy: ConflictPolicy,
) -> TableLoadResult:
    if not rows:
        return TableLoadResult(spec.table.name, 0, 0, 0)
    by_key = {}
    for row in rows:
        key = tuple(row[name] for name in spec.key)
        earlier = by_key.setdefault(key, row)
        if earlier is not row and _changed(row, earlier):
            raise LoadConflict(
                f"{spec.table.name} — внутри партии различаются записи с одним ключом."
            )

    existing = await _existing_rows(connection, spec, by_key, tuple(rows[0]))
    inserts, updates = [], []
    for key, row in by_key.items():
        found = existing.get(key)
        if found is None:
            inserts.append(row)
        elif changes := _changed(row, found):
            if policy is ConflictPolicy.VERIFY or not changes.issubset(spec.mutable):
                raise LoadConflict(
                    f"{spec.table.name} — целевая запись с тем же ключом отличается."
                )
            updates.append(row)

    inserted = await _write_rows(connection, spec, inserts, strategy, update=False)
    updated = await _write_rows(connection, spec, updates, strategy, update=True)
    if inserted != len(inserts) or updated != len(updates):
        raise LoadConflict(f"{spec.table.name} — обнаружено конкурентное изменение партии.")
    return TableLoadResult(spec.table.name, inserted, updated, len(rows) - inserted - updated)


async def _existing_rows(
    connection: AsyncConnection,
    spec: TableSpec,
    rows: Mapping[tuple[object, ...], TargetRow],
    columns: tuple[str, ...],
) -> dict[tuple[object, ...], Mapping[str, object]]:
    leading_keys = {key[0] for key in rows}
    selected = [
        cast(spec.table.c[name], Text).label(name) if name == "value" else spec.table.c[name]
        for name in columns
    ]
    result = {}
    for chunk in batched(sorted(leading_keys), MAX_BIND_PARAMETERS):
        query = (
            select(*selected)
            .where(spec.table.c[spec.key[0]].in_(chunk))
            .with_for_update(key_share=True)
        )
        async with connection.stream(query) as response:
            async for row in response.mappings():
                key = tuple(row[name] for name in spec.key)
                if key in rows:
                    result[key] = row
    return result


def _changed(row: TargetRow, earlier: Mapping[str, object]) -> set[str]:
    return {
        name
        for name, value in row.items()
        if not (
            equivalent_json(value, earlier[name]) if name == "value" else value == earlier[name]
        )
    }


async def _write_rows(
    connection: AsyncConnection,
    spec: TableSpec,
    rows: list[TargetRow],
    strategy: LoadStrategy,
    *,
    update: bool,
) -> int:
    if not rows:
        return 0
    if strategy is LoadStrategy.COPY:
        return await copy_rows(connection, spec, rows, update=update)
    if strategy is LoadStrategy.UPSERT:
        return await upsert_rows(connection, spec, rows, update=update)
    return await insert_rows(connection, spec, rows, single=strategy is LoadStrategy.INSERT)
