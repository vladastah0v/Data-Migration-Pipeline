import asyncio
import json
from collections.abc import Awaitable, Callable
from contextlib import aclosing
from hashlib import sha256
from time import perf_counter
from typing import Annotated, Self
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, ValidationError, model_validator
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
from migration_pipeline.pipeline.retry import (
    DEFAULT_MAX_RETRIES,
    DEFAULT_RETRY_DELAY,
    RetryEntry,
    is_connection_failure,
    sqlstate,
    with_retry,
)
from migration_pipeline.pipeline.sessions import MigrationBusyError, MigrationSessions
from migration_pipeline.pipeline.state import (
    COMPLETED_STATES,
    MigrationStateError,
    RunState,
    batch_already_committed,
    confirm_batch,
    create_run,
    fail_run,
    finish_loading,
    reactivate_run,
    read_run,
    save_retries,
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
    max_retries: Annotated[int, Field(ge=0)] = DEFAULT_MAX_RETRIES
    retry_delay: Annotated[float, Field(ge=0, allow_inf_nan=False)] = DEFAULT_RETRY_DELAY

    @model_validator(mode="after")
    def validate_conflict_policy(self) -> Self:
        if self.conflict_policy is ConflictPolicy.UPDATE and self.strategy not in {
            LoadStrategy.UPSERT,
            LoadStrategy.COPY,
        }:
            raise ValueError("Политика update доступна для UPSERT и COPY.")
        return self

    def fingerprint(self) -> str:
        return _configuration_hash(self.model_dump(mode="json"))


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
        sessions = self._sessions(expected_snapshot_id)
        execution = _Execution(sessions, uuid4(), self.options, self._emit)
        try:
            source = await execution.attempt("open_source", sessions.source, source=True)
            configuration = self.options.model_dump(mode="json")

            async def start() -> None:
                target = await sessions.target()
                async with target.begin():
                    await lock_schema(target)
                    await create_run(
                        target,
                        source.snapshot.id,
                        configuration,
                        self.options.fingerprint(),
                        run_id=execution.run_id,
                    )
                    await save_retries(target, execution.run_id, execution.pending)

            await execution.attempt("create_run", start)
            execution.pending.clear()
            self._emit(
                "migration_started",
                run_id=str(execution.run_id),
                snapshot_id=str(source.snapshot.id),
                configuration=configuration,
            )
            return await self._process(execution)
        finally:
            await sessions.close()

    async def resume(self, run_id: UUID) -> RunState:
        sessions = self._sessions(None)
        execution = _Execution(sessions, run_id, self.options, self._emit)
        try:

            async def read_saved() -> RunState | None:
                target = await sessions.target()
                async with target.begin():
                    return await read_run(target, run_id)

            previous = await execution.attempt("read_run", read_saved)
            if previous is None:
                raise MigrationStateError(f"Запуск {run_id} не найден.")
            if previous.status in COMPLETED_STATES:
                self._emit("migration_resume_noop", **run_fields(previous))
                return previous
            execution.options = _stored_options(previous)
            sessions.snapshot_id = previous.source_snapshot_id
            await execution.attempt("open_source", sessions.source, source=True)

            async def activate() -> RunState | None:
                target = await sessions.target()
                async with target.begin():
                    await lock_schema(target)
                    current = await read_run(target, run_id)
                    if current is None:
                        raise MigrationStateError(f"Запуск {run_id} не найден.")
                    if current.status in COMPLETED_STATES:
                        return current
                    if (
                        current.source_snapshot_id != previous.source_snapshot_id
                        or current.configuration_hash != previous.configuration_hash
                        or current.configuration != previous.configuration
                    ):
                        raise MigrationStateError("Конфигурация запуска изменилась.")
                    await reactivate_run(target, run_id)
                    await save_retries(target, run_id, execution.pending)
                    return None

            completed = await execution.attempt("resume_run", activate)
            if completed is not None:
                self._emit("migration_resume_noop", **run_fields(completed))
                return completed
            execution.pending.clear()
            self._emit(
                "migration_resumed",
                run_id=str(run_id),
                snapshot_id=str(previous.source_snapshot_id),
                configuration=previous.configuration,
                records_processed=previous.records_processed,
            )
            return await self._process(execution)
        finally:
            await sessions.close()

    def _sessions(self, snapshot_id: UUID | None) -> MigrationSessions:
        return MigrationSessions(
            target_engine(self.settings),
            lambda expected: open_legacy_source(self.settings, expected_snapshot_id=expected),
            snapshot_id,
        )

    async def _process(self, execution: "_Execution") -> RunState:
        run_id = execution.run_id
        sessions = execution.sessions
        active_batch = None

        async def read_saved_checkpoints():
            target = await sessions.target()
            async with target.begin():
                return await read_checkpoints(target, run_id)

        async def finish() -> RunState:
            target = await sessions.target()
            async with target.begin():
                await lock_schema(target)
                await save_retries(target, run_id, execution.pending)
                return await finish_loading(target, run_id)

        try:
            checkpoints = {
                cursor.entity: cursor
                for cursor in await execution.attempt("read_checkpoints", read_saved_checkpoints)
            }
            for entity in LegacyEntity:
                saved = checkpoints.get(entity)
                after = saved.last_id if saved else None
                number = saved.next_batch if saved else 1
                while True:
                    batch_started = perf_counter()
                    batch = await execution.attempt(
                        "extract_batch",
                        lambda entity=entity, after=after, number=number: _read_batch(
                            sessions, execution.options.batch_size, entity, after, number
                        ),
                        source=True,
                        entity=entity,
                        batch_number=number,
                    )
                    if batch is None:
                        break
                    active_batch = batch
                    metrics = await execution.attempt(
                        "load_batch",
                        lambda batch=batch: self._commit_batch(execution, batch),
                        entity=entity,
                        batch_number=number,
                    )
                    execution.pending.clear()
                    active_batch = None
                    self._emit(
                        "migration_batch_committed",
                        run_id=str(run_id),
                        batch_duration_seconds=round(perf_counter() - batch_started, 6),
                        **metrics,
                    )
                    if len(batch.rows) < execution.options.batch_size:
                        break
                    after, number = batch.last_id, batch.number + 1
            result = await execution.attempt("finish_loading", finish)
        except MigrationBusyError:
            raise
        except Exception as exc:
            message = _failure_message(exc)
            audited = await _audit_failure(execution, message, active_batch, exc)
            raise MigrationFailed(run_id, message, audited=audited) from exc
        self._emit("migration_loaded", **run_fields(result))
        return result

    async def _commit_batch(
        self, execution: "_Execution", batch: ExtractedBatch
    ) -> dict[str, object]:
        target = await execution.sessions.target()
        options = execution.options
        run_id = execution.run_id
        started = perf_counter()
        async with asyncio.timeout(options.transaction_timeout), target.begin():
            await lock_schema(target)
            if await batch_already_committed(target, run_id, batch):
                await save_retries(target, run_id, execution.pending)
                return {**batch.log_fields(), "reconciled": True}
            references = await _target_references(target, run_id, batch)
            transform_started = perf_counter()
            transformed = transform_batch(
                batch.entity,
                batch.rows,
                batch_number=batch.number,
                references=references,
                policy=options.policy,
            )
            load_started = perf_counter()
            loaded = await load_batch(
                target,
                transformed.records,
                strategy=options.strategy,
                conflict_policy=options.conflict_policy,
                transaction_timeout=options.transaction_timeout,
            )
            load_duration = perf_counter() - load_started
            await confirm_batch(target, run_id, batch, loaded, transformed.issues)
            await save_retries(target, run_id, execution.pending)
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


class _Execution:
    def __init__(
        self,
        sessions: MigrationSessions,
        run_id: UUID,
        options: MigrationOptions,
        emit: Callable,
    ) -> None:
        self.sessions = sessions
        self.run_id = run_id
        self.options = options
        self.emit = emit
        self.pending: list[RetryEntry] = []

    async def attempt[T](
        self,
        name: str,
        operation: Callable[[], Awaitable[T]],
        *,
        source: bool = False,
        entity: LegacyEntity | None = None,
        batch_number: int | None = None,
    ) -> T:
        async def on_retry(error: Exception, attempt: int, delay: float) -> None:
            reason = sqlstate(error) or type(error).__name__
            entry = RetryEntry(
                uuid4(), name, entity.value if entity else None, batch_number, attempt, reason
            )
            self.pending.append(entry)
            self.emit(
                "migration_retry",
                run_id=str(self.run_id),
                retry_id=str(entry.id),
                operation=name,
                entity_type=entry.entity_type,
                batch_number=batch_number,
                attempt=attempt,
                reason=reason,
                delay_seconds=delay,
            )
            if source:
                await self.sessions.reset_source()
            elif is_connection_failure(error):
                await self.sessions.reset_target()

        return await with_retry(
            operation,
            max_retries=self.options.max_retries,
            delay=self.options.retry_delay,
            on_retry=on_retry,
        )


async def _read_batch(
    sessions: MigrationSessions, size: int, entity: LegacyEntity, after: int | None, number: int
) -> ExtractedBatch | None:
    source = await sessions.source()
    async with aclosing(
        extract_batches(source, entity, batch_size=size, after_id=after, start_batch_number=number)
    ) as iterator:
        return await anext(iterator, None)


def _configuration_hash(configuration: dict[str, object]) -> str:
    encoded = json.dumps(configuration, sort_keys=True, separators=(",", ":"))
    return sha256(encoded.encode("utf-8")).hexdigest()


def _stored_options(run: RunState) -> MigrationOptions:
    if (
        run.configuration is None
        or _configuration_hash(run.configuration) != run.configuration_hash
    ):
        raise MigrationStateError("Хеш сохранённой конфигурации запуска не совпадает.")
    try:
        return MigrationOptions.model_validate(run.configuration)
    except ValidationError as exc:
        raise MigrationStateError("Сохранённая конфигурация запуска недопустима.") from exc


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
            MigrationError.is_batch_rejection.is_(True),
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
        "verified_at": run.verified_at.isoformat() if run.verified_at is not None else None,
        "records_processed": run.records_processed,
        "records_inserted": run.records_inserted,
        "records_updated": run.records_updated,
        "records_unchanged": run.records_unchanged,
        "records_rejected": run.records_rejected,
        "retry_count": run.retry_count,
    }


async def _audit_failure(
    execution: _Execution,
    message: str,
    active_batch: ExtractedBatch | None,
    exception: Exception,
) -> bool:
    issue = exception.issue if isinstance(exception, StrictTransformError) else None
    try:
        if is_connection_failure(exception):
            await execution.sessions.reset_target()
        connection = await execution.sessions.target()
        async with connection.begin():
            await lock_schema(connection)
            await save_retries(connection, execution.run_id, execution.pending)
            await fail_run(connection, execution.run_id, message, active_batch, issue)
    except Exception:
        return False
    return True


def _failure_message(exc: Exception) -> str:
    if isinstance(exc, (StrictTransformError, LoadError, CheckpointError, MigrationStateError)):
        return str(exc)
    if isinstance(exc, TimeoutError):
        return "Превышен таймаут миграции."
    return f"Сбой миграции ({type(exc).__name__})."
