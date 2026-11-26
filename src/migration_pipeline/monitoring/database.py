import asyncio
from dataclasses import dataclass
from datetime import timedelta
from uuid import UUID

from sqlalchemy import func, select, text, union_all
from sqlalchemy.ext.asyncio import AsyncConnection

from migration_pipeline.config import Settings
from migration_pipeline.database.connections import create_database_engine
from migration_pipeline.database.legacy import open_legacy_source
from migration_pipeline.extractors.base import SOURCE_TABLES
from migration_pipeline.models.migration import MigrationBatch, MigrationCheckpoint, MigrationRun

BATCH_BUCKETS = (0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300)
RATE_WINDOW_SECONDS = 60


@dataclass(frozen=True, slots=True)
class BatchHistogram:
    entity: str
    count: int
    seconds: float
    buckets: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class CurrentRun:
    snapshot_id: UUID
    status: str
    processed: int
    records_per_second: float


@dataclass(frozen=True, slots=True)
class TargetMetrics:
    processed: int
    inserted: int
    rejected: int
    errors: int
    retries: int
    batches: tuple[BatchHistogram, ...]
    recent: CurrentRun | None


async def load_target_metrics(settings: Settings) -> TargetMetrics:
    engine = create_database_engine(settings.target_db, settings.db_timeout_seconds)
    try:
        async with asyncio.timeout(settings.db_timeout_seconds), engine.connect() as connection:
            connection = await connection.execution_options(
                isolation_level="REPEATABLE READ", schema_translate_map={None: "public"}
            )
            async with connection.begin():
                await connection.execute(text("SET TRANSACTION READ ONLY"))
                return await read_target_metrics(connection)
    finally:
        await engine.dispose()


async def read_target_metrics(connection: AsyncConnection) -> TargetMetrics:
    totals = (
        await connection.execute(
            text("""
                SELECT COALESCE(sum(records_processed), 0),
                       COALESCE(sum(records_inserted), 0),
                       COALESCE(sum(records_rejected), 0),
                       (SELECT count(*) FROM public.migration_errors),
                       (SELECT count(*) FROM public.migration_retries)
                FROM public.migration_runs
                """)
        )
    ).one()
    return TargetMetrics(
        *map(int, totals),
        batches=await _batch_histograms(connection),
        recent=await _recent_run(connection),
    )


async def _batch_histograms(connection: AsyncConnection) -> tuple[BatchHistogram, ...]:
    bucket_counts = ", ".join(
        f"count(*) FILTER (WHERE seconds <= :bucket_{index})" for index in range(len(BATCH_BUCKETS))
    )
    rows = await connection.execute(
        text(f"""
            WITH committed AS (
                SELECT entity_type, EXTRACT(EPOCH FROM committed_at - started_at)::float8 AS seconds
                FROM public.migration_batches
                WHERE status = 'COMMITTED'
            )
            SELECT entity_type, count(*), sum(seconds), {bucket_counts}
            FROM committed
            GROUP BY entity_type
            """),
        {f"bucket_{index}": value for index, value in enumerate(BATCH_BUCKETS)},
    )
    return tuple(
        BatchHistogram(entity, count, seconds, tuple(buckets))
        for entity, count, seconds, *buckets in rows
    )


async def _recent_run(connection: AsyncConnection) -> CurrentRun | None:
    activity = func.greatest(
        MigrationRun.started_at, MigrationRun.finished_at, func.max(MigrationCheckpoint.updated_at)
    )
    recent = (
        await connection.execute(
            select(
                MigrationRun.id,
                MigrationRun.source_snapshot_id,
                MigrationRun.status,
                MigrationRun.records_processed,
                MigrationRun.started_at,
                func.coalesce(MigrationRun.finished_at, func.now()).label("window_end"),
            )
            .outerjoin(MigrationCheckpoint, MigrationCheckpoint.migration_run_id == MigrationRun.id)
            .group_by(MigrationRun.id)
            .order_by(activity.desc(), MigrationRun.started_at.desc(), MigrationRun.id.desc())
            .limit(1)
        )
    ).one_or_none()
    if recent is None:
        return None
    window_end = recent.window_end
    window_start = max(recent.started_at, window_end - timedelta(seconds=RATE_WINDOW_SECONDS))
    duration = (window_end - window_start).total_seconds()
    processed = await connection.scalar(
        select(func.coalesce(func.sum(MigrationBatch.records_count), 0)).where(
            MigrationBatch.migration_run_id == recent.id,
            MigrationBatch.status == "COMMITTED",
            MigrationBatch.committed_at >= window_start,
            MigrationBatch.committed_at <= window_end,
        )
    )
    return CurrentRun(
        recent.source_snapshot_id,
        recent.status,
        recent.records_processed,
        int(processed) / duration if duration > 0 else 0,
    )


async def count_snapshot(settings: Settings, snapshot_id: UUID) -> int:
    async with (
        asyncio.timeout(settings.db_timeout_seconds),
        open_legacy_source(settings, expected_snapshot_id=snapshot_id) as source,
    ):
        totals = union_all(
            *(
                select(func.count().label("records")).select_from(table)
                for table in SOURCE_TABLES.values()
            )
        ).subquery()
        return int(await source.connection.scalar(select(func.sum(totals.c.records))))
