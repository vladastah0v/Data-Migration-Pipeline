from enum import StrEnum
from uuid import UUID

from asyncpg import InterfaceError, PostgresError
from sqlalchemy import func, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncConnection

from migration_pipeline.config import Settings
from migration_pipeline.database.legacy import LegacySourceError
from migration_pipeline.database.schema import require_current_schema
from migration_pipeline.models.migration import MigrationBatch, MigrationError, MigrationRetry
from migration_pipeline.monitoring.database import count_snapshot
from migration_pipeline.pipeline.orchestrator import target_engine
from migration_pipeline.pipeline.state import MigrationStateError, RunState, read_run

NOT_MEASURED = "NOT_MEASURED"


class ReportFormat(StrEnum):
    MARKDOWN = "markdown"
    JSON = "json"


async def migration_report(settings: Settings, run_id: UUID) -> dict[str, object]:
    engine = target_engine(settings)
    try:
        async with engine.connect() as connection:
            await connection.execution_options(isolation_level="REPEATABLE READ")
            async with connection.begin():
                await connection.execute(text("SET TRANSACTION READ ONLY"))
                await require_current_schema(connection, "target")
                run = await read_run(connection, run_id)
                if run is None:
                    raise MigrationStateError(f"Запуск {run_id} не найден.")
                snapshot_at = await connection.scalar(select(func.clock_timestamp()))
                errors = await _error_counts(connection, run_id)
                retries = await _retry_counts(connection, run_id)
                batches = await _batch_counts(connection, run_id)
    finally:
        await engine.dispose()

    source_total, source_count_basis = await _source_count(settings, run)
    options = run.configuration or {}
    duration = max(0.0, ((run.finished_at or snapshot_at) - run.started_at).total_seconds())
    return {
        "format_version": 1,
        "run_id": str(run.id),
        "source_snapshot_id": str(run.source_snapshot_id),
        "status": run.status,
        "started_at": run.started_at.isoformat(),
        "finished_at": run.finished_at.isoformat() if run.finished_at else None,
        "observed_at": snapshot_at.isoformat(),
        "strategy": options.get("strategy"),
        "batch_size": options.get("batch_size"),
        "policy": options.get("policy"),
        "records_source": source_total,
        "source_count_basis": source_count_basis,
        "records_processed": run.records_processed,
        "records_transferred": run.records_inserted + run.records_updated + run.records_unchanged,
        "records_inserted": run.records_inserted,
        "records_updated": run.records_updated,
        "records_unchanged": run.records_unchanged,
        "records_rejected": run.records_rejected,
        "error_count": sum(error["count"] for error in errors),
        "error_message": run.error_message,
        "duration_seconds": duration,
        "records_per_second": run.records_processed / duration if duration > 0 else None,
        "peak_rss_bytes": run.peak_rss_bytes,
        "peak_rss_measurement": "MEASURED" if run.peak_rss_bytes is not None else NOT_MEASURED,
        "verified_at": run.verified_at.isoformat() if run.verified_at else None,
        "verification_report": run.verification_report,
        "batches": batches,
        "checkpoints": [
            {
                "entity_type": item.entity.value,
                "last_legacy_id": item.last_id,
                "records_processed": item.processed,
                "next_batch": item.next_batch,
            }
            for item in run.checkpoints
        ],
        "retry_count": run.retry_count,
        "retries": retries,
        "errors": errors,
    }


async def _source_count(settings: Settings, run: RunState) -> tuple[int | None, str]:
    if run.verification_report is not None:
        records = run.verification_report["records"]
        return sum(entity["source"] for entity in records.values()), "VERIFICATION"
    if run.status in {"LOADED", "SUCCESS", "SUCCESS_WITH_REJECTIONS"}:
        return run.records_processed, "COMPLETED_LOAD"
    try:
        return await count_snapshot(settings, run.source_snapshot_id), "LEGACY_SNAPSHOT"
    except (LegacySourceError, OSError, SQLAlchemyError, PostgresError, InterfaceError):
        return None, "UNAVAILABLE"


async def _error_counts(connection: AsyncConnection, run_id: UUID) -> list[dict[str, object]]:
    group = (
        MigrationError.entity_type,
        MigrationError.error_code,
        MigrationError.is_quarantined,
        MigrationError.is_batch_rejection,
    )
    result = await connection.execute(
        select(*group, func.count().label("count"))
        .where(MigrationError.migration_run_id == run_id)
        .group_by(*group)
        .order_by(*group)
    )
    return [dict(row) for row in result.mappings()]


async def _retry_counts(connection: AsyncConnection, run_id: UUID) -> list[dict[str, object]]:
    group = (MigrationRetry.operation, MigrationRetry.reason)
    result = await connection.execute(
        select(*group, func.count().label("count"))
        .where(MigrationRetry.migration_run_id == run_id)
        .group_by(*group)
        .order_by(*group)
    )
    return [dict(row) for row in result.mappings()]


async def _batch_counts(connection: AsyncConnection, run_id: UUID) -> dict[str, int]:
    result = await connection.execute(
        select(MigrationBatch.status, func.count())
        .where(MigrationBatch.migration_run_id == run_id)
        .group_by(MigrationBatch.status)
        .order_by(MigrationBatch.status)
    )
    return {status: count for status, count in result}
