from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from sqlalchemy import func, insert, select, update
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from migration_pipeline.extractors.base import ExtractedBatch
from migration_pipeline.loaders.base import LoadResult
from migration_pipeline.models.migration import (
    MigrationBatch,
    MigrationCheckpoint,
    MigrationError,
    MigrationRetry,
    MigrationRun,
)
from migration_pipeline.pipeline.checkpoint import Checkpoint, advance_checkpoint, read_checkpoints
from migration_pipeline.pipeline.retry import RetryEntry
from migration_pipeline.transformers.errors import MESSAGES, RecordIssue

COMPLETED_STATES = {"LOADED", "SUCCESS", "SUCCESS_WITH_REJECTIONS", "FAILED_VERIFICATION"}


class MigrationStateError(RuntimeError):
    """Статус запуска или партии не позволяет выполнить операцию."""


@dataclass(frozen=True, slots=True)
class RunState:
    id: UUID
    source_snapshot_id: UUID
    started_at: datetime
    finished_at: datetime | None
    verified_at: datetime | None
    verification_report: dict[str, object] | None
    status: str
    configuration_hash: str
    configuration: dict[str, object] | None
    records_processed: int
    records_inserted: int
    records_updated: int
    records_unchanged: int
    records_rejected: int
    peak_rss_bytes: int | None
    error_message: str | None
    retry_count: int
    checkpoints: tuple[Checkpoint, ...]


async def create_run(
    connection: AsyncConnection,
    snapshot_id: UUID,
    configuration: dict[str, object],
    configuration_hash: str,
    *,
    run_id: UUID | None = None,
) -> UUID:
    stmt = postgres_insert(MigrationRun).values(
        source_snapshot_id=snapshot_id,
        configuration=configuration,
        configuration_hash=configuration_hash,
    )
    if run_id is not None:
        stmt = stmt.values(id=run_id).on_conflict_do_nothing(index_elements=[MigrationRun.id])
    created = await connection.scalar(stmt.returning(MigrationRun.id))
    if created is not None:
        return created
    existing = await connection.scalar(
        select(MigrationRun.id).where(
            MigrationRun.id == run_id,
            MigrationRun.source_snapshot_id == snapshot_id,
            MigrationRun.configuration_hash == configuration_hash,
            MigrationRun.status == "RUNNING",
        )
    )
    if existing is None:
        raise MigrationStateError("Состояние уже созданного запуска отличается.")
    return existing


async def read_run(connection: AsyncConnection, run_id: UUID | None = None) -> RunState | None:
    retries = (
        select(func.count(MigrationRetry.id))
        .where(MigrationRetry.migration_run_id == MigrationRun.id)
        .scalar_subquery()
    )
    query = select(MigrationRun.__table__, retries.label("retry_count"))
    if run_id is None:
        query = query.order_by(MigrationRun.started_at.desc(), MigrationRun.id.desc()).limit(1)
    else:
        query = query.where(MigrationRun.id == run_id)
    row = (await connection.execute(query)).mappings().one_or_none()
    if row is None:
        return None
    checkpoints = await read_checkpoints(connection, row["id"])
    return RunState(**row, checkpoints=checkpoints)


async def reactivate_run(connection: AsyncConnection, run_id: UUID) -> None:
    active = await connection.scalar(
        update(MigrationRun)
        .where(MigrationRun.id == run_id, MigrationRun.status.in_(("RUNNING", "FAILED_MIGRATION")))
        .values(status="RUNNING", finished_at=None, error_message=None)
        .returning(MigrationRun.id)
    )
    if active is None:
        raise MigrationStateError("Запуск недоступен для возобновления.")


async def batch_already_committed(
    connection: AsyncConnection, run_id: UUID, batch: ExtractedBatch
) -> bool:
    saved = (
        await connection.execute(
            select(
                MigrationBatch.first_legacy_id,
                MigrationBatch.last_legacy_id,
                MigrationBatch.records_count,
            ).where(
                MigrationBatch.migration_run_id == run_id,
                MigrationBatch.entity_type == batch.entity.value,
                MigrationBatch.batch_number == batch.number,
                MigrationBatch.status == "COMMITTED",
            )
        )
    ).one_or_none()
    if saved is None:
        active = await connection.scalar(
            select(MigrationRun.id).where(
                MigrationRun.id == run_id, MigrationRun.status == "RUNNING"
            )
        )
        if active is None:
            raise MigrationStateError("Запуск уже завершён или требует явного возобновления.")
        return False
    checkpoint = await connection.scalar(
        select(MigrationCheckpoint.last_processed_legacy_id).where(
            MigrationCheckpoint.migration_run_id == run_id,
            MigrationCheckpoint.entity_type == batch.entity.value,
        )
    )
    if (
        tuple(saved) != (batch.first_id, batch.last_id, len(batch.rows))
        or checkpoint is None
        or checkpoint < batch.last_id
    ):
        raise MigrationStateError(
            "Подтверждённая партия не согласована с диапазоном или checkpoint."
        )
    return True


async def save_retries(
    connection: AsyncConnection, run_id: UUID, entries: Sequence[RetryEntry]
) -> None:
    if entries:
        await connection.execute(
            postgres_insert(MigrationRetry).on_conflict_do_nothing(
                index_elements=[MigrationRetry.id]
            ),
            [
                {
                    "id": event.id,
                    "migration_run_id": run_id,
                    "operation": event.operation,
                    "entity_type": event.entity_type,
                    "batch_number": event.batch_number,
                    "attempt": event.attempt,
                    "reason": event.reason,
                }
                for event in entries
            ],
        )


async def save_peak_rss(connection: AsyncConnection, run_id: UUID, peak: int | None) -> None:
    if peak is not None:
        await connection.execute(
            update(MigrationRun)
            .where(
                MigrationRun.id == run_id,
                MigrationRun.peak_rss_bytes.is_(None) | (MigrationRun.peak_rss_bytes < peak),
            )
            .values(peak_rss_bytes=peak)
        )


async def confirm_batch(
    connection: AsyncConnection,
    run_id: UUID,
    batch: ExtractedBatch,
    loaded: LoadResult,
    issues: Sequence[RecordIssue],
) -> None:
    if loaded.inserted + loaded.updated + loaded.unchanged + len(issues) != len(batch.rows):
        raise MigrationStateError("Счётчики загрузки не совпали с размером исходной партии.")
    await save_issues(connection, run_id, issues)
    stmt = postgres_insert(MigrationBatch).values(
        **_batch_values(run_id, batch), status="COMMITTED", committed_at=func.clock_timestamp()
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[
            MigrationBatch.migration_run_id,
            MigrationBatch.entity_type,
            MigrationBatch.batch_number,
        ],
        set_={
            "first_legacy_id": stmt.excluded.first_legacy_id,
            "last_legacy_id": stmt.excluded.last_legacy_id,
            "records_count": stmt.excluded.records_count,
            "status": stmt.excluded.status,
            "started_at": stmt.excluded.started_at,
            "committed_at": stmt.excluded.committed_at,
        },
        where=MigrationBatch.status.in_(("FAILED", "STARTED")),
    ).returning(MigrationBatch.id)
    if await connection.scalar(stmt) is None:
        raise MigrationStateError("Партия с таким номером уже подтверждена или выполняется.")

    await advance_checkpoint(connection, run_id, batch)
    counted = await connection.scalar(
        _update_running(run_id)
        .values(
            records_processed=MigrationRun.records_processed + len(batch.rows),
            records_inserted=MigrationRun.records_inserted + loaded.inserted,
            records_updated=MigrationRun.records_updated + loaded.updated,
            records_unchanged=MigrationRun.records_unchanged + loaded.unchanged,
            records_rejected=MigrationRun.records_rejected + len(issues),
        )
        .returning(MigrationRun.id)
    )
    if counted is None:
        raise MigrationStateError("Запуск уже завершён.")


async def save_issues(
    connection: AsyncConnection,
    run_id: UUID,
    issues: Sequence[RecordIssue],
    *,
    committed: bool = True,
) -> None:
    if issues:
        stmt = postgres_insert(MigrationError)
        await connection.execute(
            stmt.on_conflict_do_update(
                index_elements=[
                    MigrationError.migration_run_id,
                    MigrationError.entity_type,
                    MigrationError.legacy_record_id,
                    MigrationError.error_code,
                ],
                set_={
                    "error_message": stmt.excluded.error_message,
                    "is_quarantined": stmt.excluded.is_quarantined,
                    "is_batch_rejection": stmt.excluded.is_batch_rejection,
                },
                where=~MigrationError.is_batch_rejection | stmt.excluded.is_batch_rejection,
            ),
            [
                {
                    "migration_run_id": run_id,
                    "entity_type": issue.entity.value,
                    "legacy_record_id": issue.legacy_record_id,
                    "error_code": issue.code.value,
                    "error_message": f"{issue.field} — {MESSAGES[issue.code]}",
                    "is_quarantined": issue.is_quarantined,
                    "is_batch_rejection": committed,
                }
                for issue in issues
            ],
        )


async def finish_loading(connection: AsyncConnection, run_id: UUID) -> RunState:
    finished = await connection.scalar(
        _update_running(run_id)
        .values(status="LOADED", finished_at=func.clock_timestamp())
        .returning(MigrationRun.id)
    )
    result = await read_run(connection, run_id)
    if result is None or (finished is None and result.status not in COMPLETED_STATES):
        raise MigrationStateError("Запуск уже завершён или удалён.")
    return result


async def save_verification(
    connection: AsyncConnection, run_id: UUID, status: str, report: dict[str, object]
) -> RunState:
    failed = [name for name, passed in report["checks"].items() if not passed]
    updated = await connection.scalar(
        update(MigrationRun)
        .where(MigrationRun.id == run_id, MigrationRun.status.in_(COMPLETED_STATES))
        .values(
            status=status,
            verified_at=func.clock_timestamp(),
            verification_report=report,
            error_message=f"Расхождения в проверках — {', '.join(failed)}." if failed else None,
        )
        .returning(MigrationRun.id)
    )
    if updated is None:
        raise MigrationStateError("Запуск недоступен для записи результата проверки.")
    return await read_run(connection, updated)


async def fail_run(
    connection: AsyncConnection,
    run_id: UUID,
    message: str,
    batch: ExtractedBatch | None,
    issue: RecordIssue | None,
) -> None:
    changed = await connection.scalar(
        _update_running(run_id)
        .values(
            status="FAILED_MIGRATION", finished_at=func.clock_timestamp(), error_message=message
        )
        .returning(MigrationRun.id)
    )
    if changed is None:
        return
    if batch is not None:
        await connection.execute(
            postgres_insert(MigrationBatch)
            .values(**_batch_values(run_id, batch), status="FAILED")
            .on_conflict_do_nothing(
                index_elements=[
                    MigrationBatch.migration_run_id,
                    MigrationBatch.entity_type,
                    MigrationBatch.batch_number,
                ]
            )
        )
    if issue is not None:
        await save_issues(connection, run_id, (issue,), committed=False)
    else:
        await connection.execute(
            insert(MigrationError).values(
                migration_run_id=run_id,
                entity_type=batch.entity.value if batch is not None else "run",
                legacy_record_id=None,
                error_code="migration_failed",
                error_message=message,
                is_batch_rejection=False,
            )
        )


def _update_running(run_id: UUID):
    return update(MigrationRun).where(MigrationRun.id == run_id, MigrationRun.status == "RUNNING")


def _batch_values(run_id: UUID, batch: ExtractedBatch) -> dict[str, object]:
    return {
        "migration_run_id": run_id,
        "entity_type": batch.entity.value,
        "batch_number": batch.number,
        "first_legacy_id": batch.first_id,
        "last_legacy_id": batch.last_id,
        "records_count": len(batch.rows),
    }
