from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from sqlalchemy import func, insert, select, update
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from migration_pipeline.extractors.base import ExtractedBatch
from migration_pipeline.loaders.base import LoadResult
from migration_pipeline.models.migration import MigrationBatch, MigrationError, MigrationRun
from migration_pipeline.pipeline.checkpoint import Checkpoint, advance_checkpoint, read_checkpoints
from migration_pipeline.transformers.errors import RecordIssue


class MigrationStateError(RuntimeError):
    """Статус запуска или партии не позволяет выполнить операцию."""


@dataclass(frozen=True, slots=True)
class RunState:
    id: UUID
    source_snapshot_id: UUID
    started_at: datetime
    finished_at: datetime | None
    status: str
    configuration_hash: str
    configuration: dict[str, object] | None
    records_processed: int
    records_inserted: int
    records_updated: int
    records_unchanged: int
    records_rejected: int
    error_message: str | None
    checkpoints: tuple[Checkpoint, ...]


async def create_run(
    connection: AsyncConnection,
    snapshot_id: UUID,
    configuration: dict[str, object],
    configuration_hash: str,
) -> UUID:
    return await connection.scalar(
        insert(MigrationRun)
        .values(
            source_snapshot_id=snapshot_id,
            configuration=configuration,
            configuration_hash=configuration_hash,
        )
        .returning(MigrationRun.id)
    )


async def read_run(connection: AsyncConnection, run_id: UUID | None = None) -> RunState | None:
    query = select(MigrationRun.__table__)
    if run_id is None:
        query = query.order_by(MigrationRun.started_at.desc(), MigrationRun.id.desc()).limit(1)
    else:
        query = query.where(MigrationRun.id == run_id)
    row = (await connection.execute(query)).mappings().one_or_none()
    if row is None:
        return None
    checkpoints = await read_checkpoints(connection, row["id"])
    return RunState(**row, checkpoints=checkpoints)


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
        where=MigrationBatch.status == "FAILED",
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
    connection: AsyncConnection, run_id: UUID, issues: Sequence[RecordIssue]
) -> None:
    if issues:
        await connection.execute(
            insert(MigrationError),
            [
                {
                    "migration_run_id": run_id,
                    "entity_type": issue.entity.value,
                    "legacy_record_id": issue.legacy_record_id,
                    "error_code": issue.code.value,
                    "error_message": f"{issue.field} — {issue.log_fields()['error_message']}",
                    "is_quarantined": issue.is_quarantined,
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
    if finished is None or (result := await read_run(connection, run_id)) is None:
        raise MigrationStateError("Запуск уже завершён или удалён.")
    return result


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
        await save_issues(connection, run_id, (issue,))
    else:
        await connection.execute(
            insert(MigrationError).values(
                migration_run_id=run_id,
                entity_type=batch.entity.value if batch is not None else "run",
                legacy_record_id=None,
                error_code="migration_failed",
                error_message=message,
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
