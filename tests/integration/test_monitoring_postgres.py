from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import insert

from migration_pipeline.config import SchemaSettings
from migration_pipeline.database.connections import create_database_engine
from migration_pipeline.models.migration import (
    MigrationBatch,
    MigrationCheckpoint,
    MigrationError,
    MigrationRetry,
    MigrationRun,
)
from migration_pipeline.monitoring.database import BATCH_BUCKETS, read_target_metrics


@pytest.mark.asyncio
async def test_metrics_read_committed_audit_and_follow_latest_loading_activity(
    settings: SchemaSettings,
) -> None:
    engine = create_database_engine(settings.target_db, settings.db_timeout_seconds)
    try:
        async with engine.connect() as connection, connection.begin() as transaction:
            baseline = await read_target_metrics(connection)
            older, newer, snapshot = uuid4(), uuid4(), uuid4()
            current_time = datetime.now(UTC)
            await connection.execute(
                insert(MigrationRun),
                [
                    {
                        "id": older,
                        "started_at": current_time - timedelta(seconds=600),
                        "finished_at": None,
                        "status": "RUNNING",
                        "configuration_hash": "0" * 64,
                        "source_snapshot_id": snapshot,
                        "records_processed": 15,
                        "records_inserted": 10,
                        "records_rejected": 1,
                    },
                    {
                        "id": newer,
                        "started_at": current_time - timedelta(seconds=40),
                        "finished_at": current_time - timedelta(seconds=20),
                        "status": "LOADED",
                        "configuration_hash": "0" * 64,
                        "source_snapshot_id": uuid4(),
                        "records_processed": 0,
                        "records_inserted": 0,
                        "records_rejected": 0,
                    },
                ],
            )
            for number, records, age, duration, status in (
                (1, 5, 90, 0.04, "COMMITTED"),
                (2, 10, 4, 1.2, "COMMITTED"),
                (3, 12, 3, 0, "FAILED"),
            ):
                end = current_time - timedelta(seconds=age)
                await connection.execute(
                    insert(MigrationBatch).values(
                        migration_run_id=older,
                        entity_type="orders",
                        batch_number=number,
                        first_legacy_id=1,
                        last_legacy_id=number,
                        records_count=records,
                        started_at=end - timedelta(seconds=duration),
                        committed_at=end if status == "COMMITTED" else None,
                        status=status,
                    )
                )
            await connection.execute(
                insert(MigrationCheckpoint).values(
                    migration_run_id=older,
                    entity_type="orders",
                    last_processed_legacy_id=2,
                    processed_count=15,
                    updated_at=current_time - timedelta(seconds=4),
                )
            )
            await connection.execute(
                insert(MigrationError).values(
                    migration_run_id=older,
                    entity_type="orders",
                    legacy_record_id=1,
                    error_code="test",
                    error_message="Synthetic failure",
                )
            )
            await connection.execute(
                insert(MigrationRetry).values(
                    id=uuid4(),
                    migration_run_id=older,
                    operation="load_batch",
                    entity_type="orders",
                    batch_number=2,
                    attempt=1,
                    reason="40001",
                )
            )

            result = await read_target_metrics(connection)
            assert result.processed == baseline.processed + 15
            assert result.inserted == baseline.inserted + 10
            assert result.rejected == baseline.rejected + 1
            assert result.errors == baseline.errors + 1
            assert result.retries == baseline.retries + 1
            assert result.recent is not None
            assert result.recent.snapshot_id == snapshot
            assert result.recent.status == "RUNNING"
            assert result.recent.processed == 15
            assert result.recent.records_per_second == pytest.approx(10 / 60)
            observed = next(item for item in result.batches if item.entity == "orders")
            existing = next((item for item in baseline.batches if item.entity == "orders"), None)
            assert observed.count == (existing.count if existing else 0) + 2
            assert observed.seconds == pytest.approx((existing.seconds if existing else 0) + 1.24)
            increments = tuple(int(limit >= 0.04) + int(limit >= 1.2) for limit in BATCH_BUCKETS)
            assert observed.buckets == tuple(
                old + new
                for old, new in zip(
                    existing.buckets if existing else (0,) * len(BATCH_BUCKETS),
                    increments,
                    strict=True,
                )
            )
            await transaction.rollback()
    finally:
        await engine.dispose()
