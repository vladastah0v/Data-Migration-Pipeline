import asyncio
import json
from collections.abc import Callable
from hashlib import sha256
from time import perf_counter
from typing import Annotated, Self
from uuid import UUID

from pydantic import BaseModel, Field, model_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from migration_pipeline.config import Settings
from migration_pipeline.database.connections import create_database_engine
from migration_pipeline.database.legacy import open_legacy_source
from migration_pipeline.database.schema import lock_schema, require_current_schema
from migration_pipeline.extractors.base import (
    DEFAULT_BATCH_SIZE,
    MAX_BATCH_SIZE,
    ExtractedBatch,
    LegacyEntity,
    extract_batches,
)
from migration_pipeline.loaders.base import ConflictPolicy, LoadError, LoadStrategy, load_batch
from migration_pipeline.loaders.records import CUSTOMERS, ORDERS, PAYMENTS
from migration_pipeline.models.migration import MigrationError
from migration_pipeline.pipeline.checkpoint import CheckpointError, read_checkpoints
from migration_pipeline.pipeline.state import (
    MigrationStateError,
    RunState,
    confirm_batch,
    create_run,
    fail_run,
    finish_loading,
    read_run,
)
from migration_pipeline.transformers.base import PARENT_KEYS, BatchReferences, transform_batch
from migration_pipeline.transformers.errors import ErrorPolicy, StrictTransformError

type MigrationObserver = Callable[[str, dict[str, object]], None]


class MigrationOptions(BaseModel):
    model_config = {"frozen": True}

    batch_size: Annotated[int, Field(ge=1, le=MAX_BATCH_SIZE)] = DEFAULT_BATCH_SIZE
    strategy: LoadStrategy = LoadStrategy.UPSERT
    policy: ErrorPolicy = ErrorPolicy.STRICT
    conflict_policy: ConflictPolicy = ConflictPolicy.VERIFY
    transaction_timeout: Annotated[float, Field(gt=0, allow_inf_nan=False)] = 60.0

    @model_validator(mode="after")
    def validate_conflict_policy(self) -> Self:
        if self.conflict_policy is ConflictPolicy.UPDATE and self.strategy not in {
            LoadStrategy.UPSERT,
            LoadStrategy.COPY,
        }:
            raise ValueError("Политика update доступна для UPSERT и COPY.")
        return self

    def fingerprint(self) -> str:
        encoded = json.dumps(self.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
        return sha256(encoded.encode("utf-8")).hexdigest()


class MigrationFailed(RuntimeError):
    def __init__(self, run_id: UUID, message: str, *, audited: bool) -> None:
        self.run_id = run_id
        self.audited = audited
        super().__init__(message)


class MigrationOrchestrator:
    def __init__(
        self,
        settings: Settings,
        options: MigrationOptions,
        observer: MigrationObserver | None = None,
    ) -> None:
        self.settings = settings
        self.options = options
        self.observer = observer

    async def run(self, *, expected_snapshot_id: UUID | None = None) -> RunState:
        engine = target_engine(self.settings)
        try:
            async with (
                open_legacy_source(
                    self.settings, expected_snapshot_id=expected_snapshot_id
                ) as source,
                engine.connect() as target,
            ):
                configuration = self.options.model_dump(mode="json")
                async with target.begin():
                    await lock_schema(target)
                    await require_current_schema(target, "target")
                    run_id = await create_run(
                        target, source.snapshot.id, configuration, self.options.fingerprint()
                    )

                active_batch = None
                try:
                    self._emit(
                        "migration_started",
                        run_id=str(run_id),
                        snapshot_id=str(source.snapshot.id),
                        configuration=configuration,
                    )
                    async with target.begin():
                        checkpoints = {
                            cursor.entity: cursor
                            for cursor in await read_checkpoints(target, run_id)
                        }
                    for entity in LegacyEntity:
                        saved = checkpoints.get(entity)
                        async for batch in extract_batches(
                            source,
                            entity,
                            batch_size=self.options.batch_size,
                            after_id=saved.last_id if saved else None,
                            start_batch_number=saved.next_batch if saved else 1,
                        ):
                            active_batch = batch
                            metrics = await self._commit_batch(target, run_id, batch)
                            active_batch = None
                            self._emit("migration_batch_committed", run_id=str(run_id), **metrics)

                    async with target.begin():
                        await lock_schema(target)
                        result = await finish_loading(target, run_id)
                except Exception as exc:
                    message = _failure_message(exc)
                    audited = await _audit_failure(engine, run_id, message, active_batch, exc)
                    raise MigrationFailed(run_id, message, audited=audited) from exc
                self._emit("migration_loaded", **run_fields(result))
                return result
        finally:
            await engine.dispose()

    async def _commit_batch(
        self, target: AsyncConnection, run_id: UUID, batch: ExtractedBatch
    ) -> dict[str, object]:
        started = perf_counter()
        async with asyncio.timeout(self.options.transaction_timeout), target.begin():
            await lock_schema(target)
            references = await _target_references(target, run_id, batch)
            transform_started = perf_counter()
            transformed = transform_batch(
                batch.entity,
                batch.rows,
                batch_number=batch.number,
                references=references,
                policy=self.options.policy,
            )
            load_started = perf_counter()
            loaded = await load_batch(
                target,
                transformed.records,
                strategy=self.options.strategy,
                conflict_policy=self.options.conflict_policy,
                transaction_timeout=self.options.transaction_timeout,
            )
            load_duration = perf_counter() - load_started
            await confirm_batch(target, run_id, batch, loaded, transformed.issues)
        return {
            **batch.log_fields(),
            "records_inserted": loaded.inserted,
            "records_updated": loaded.updated,
            "records_unchanged": loaded.unchanged,
            "records_rejected": len(transformed.issues),
            "transform_duration_seconds": round(load_started - transform_started, 6),
            "load_duration_seconds": round(load_duration, 6),
            "transaction_duration_seconds": round(perf_counter() - started, 6),
        }

    def _emit(self, event: str, **fields: object) -> None:
        if self.observer is not None:
            self.observer(event, fields)


async def _target_references(
    connection: AsyncConnection, run_id: UUID, batch: ExtractedBatch
) -> BatchReferences:
    field = PARENT_KEYS.get(batch.entity)
    if field is None:
        return BatchReferences()
    if batch.entity is LegacyEntity.ORDERS:
        parent, parent_entity = CUSTOMERS, LegacyEntity.CUSTOMERS
    else:
        parent, parent_entity = ORDERS, LegacyEntity.ORDERS
    key = parent.table.c[parent.key[0]]
    rejected = (
        select(MigrationError.id)
        .where(
            MigrationError.migration_run_id == run_id,
            MigrationError.entity_type == parent_entity.value,
            MigrationError.legacy_record_id == key,
        )
        .exists()
    )
    parent_ids = frozenset(
        await connection.scalars(
            select(key)
            .where(key.in_({row[field] for row in batch.rows}), ~rejected)
            .with_for_update(read=True, key_share=True, of=parent.table)
        )
    )
    if batch.entity is not LegacyEntity.PAYMENTS:
        return BatchReferences(parent_ids)

    requested = {
        value.strip()
        for row in batch.rows
        if isinstance(value := row["payment_reference"], str) and value.strip()
    }
    owners = await connection.execute(
        select(PAYMENTS.table.c.payment_reference, PAYMENTS.table.c.legacy_payment_id)
        .where(PAYMENTS.table.c.payment_reference.in_(requested))
        .with_for_update(key_share=True)
    )
    return BatchReferences(parent_ids, {reference: legacy_id for reference, legacy_id in owners})


def target_engine(settings: Settings) -> AsyncEngine:
    engine = create_database_engine(settings.target_db, settings.db_timeout_seconds)
    engine.update_execution_options(schema_translate_map={None: "public"})
    return engine


async def migration_status(settings: Settings, run_id: UUID | None = None) -> RunState | None:
    engine = target_engine(settings)
    try:
        async with engine.connect() as connection:
            await connection.execution_options(isolation_level="REPEATABLE READ")
            await require_current_schema(connection, "target")
            return await read_run(connection, run_id)
    finally:
        await engine.dispose()


def run_fields(run: RunState) -> dict[str, object]:
    return {
        "run_id": str(run.id),
        "snapshot_id": str(run.source_snapshot_id),
        "status": run.status,
        "records_processed": run.records_processed,
        "records_inserted": run.records_inserted,
        "records_updated": run.records_updated,
        "records_unchanged": run.records_unchanged,
        "records_rejected": run.records_rejected,
    }


async def _audit_failure(
    engine: AsyncEngine,
    run_id: UUID,
    message: str,
    active_batch: ExtractedBatch | None,
    exception: Exception,
) -> bool:
    issue = exception.issue if isinstance(exception, StrictTransformError) else None
    try:
        async with engine.begin() as connection:
            await lock_schema(connection)
            await fail_run(connection, run_id, message, active_batch, issue)
    except Exception:
        return False
    return True


def _failure_message(exc: Exception) -> str:
    if isinstance(exc, (StrictTransformError, LoadError, CheckpointError, MigrationStateError)):
        return str(exc)
    if isinstance(exc, TimeoutError):
        return "Превышен таймаут миграции."
    return f"Сбой миграции ({type(exc).__name__})."
