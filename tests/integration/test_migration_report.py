from uuid import uuid4

import pytest
from sqlalchemy import delete, func, insert, update

from migration_pipeline.config import SchemaSettings
from migration_pipeline.models.migration import MigrationCheckpoint, MigrationError, MigrationRun
from migration_pipeline.pipeline.orchestrator import MigrationOptions, target_engine
from migration_pipeline.pipeline.retry import RetryEntry
from migration_pipeline.pipeline.state import (
    create_run,
    save_peak_rss,
    save_retries,
    save_verification,
)
from migration_pipeline.reporting.render import render_report
from migration_pipeline.reporting.service import ReportFormat, migration_report


@pytest.mark.asyncio
async def test_report_uses_durable_peak_and_verification_with_legacy_unavailable(
    settings: SchemaSettings,
) -> None:
    engine = target_engine(settings)
    options = MigrationOptions(batch_size=10)
    snapshot_id = uuid4()
    run_id = uuid4()
    try:
        async with engine.begin() as connection:
            await create_run(
                connection,
                snapshot_id,
                options.model_dump(mode="json"),
                options.fingerprint(),
                run_id=run_id,
            )
            await connection.execute(
                update(MigrationRun)
                .where(MigrationRun.id == run_id)
                .values(
                    status="LOADED",
                    finished_at=func.clock_timestamp(),
                    records_processed=2,
                    records_inserted=1,
                    records_rejected=1,
                )
            )

        offline = settings.model_copy(
            update={"legacy_db": settings.legacy_db.model_copy(update={"port": 1})}
        )
        before = await migration_report(offline, run_id)
        assert before["records_source"] == 2
        assert before["peak_rss_bytes"] is None
        assert "NOT_MEASURED" in render_report(before, ReportFormat.MARKDOWN)
        async with engine.begin() as connection:
            await save_peak_rss(connection, run_id, 40 * 2**20)
            await save_peak_rss(connection, run_id, 32 * 2**20)
            await save_retries(
                connection, run_id, [RetryEntry(uuid4(), "load_batch", "orders", 1, 1, "40001")]
            )
            await connection.execute(
                insert(MigrationError).values(
                    migration_run_id=run_id,
                    entity_type="orders",
                    legacy_record_id=4,
                    error_code="invalid_money",
                    error_message="Некорректное значение",
                    is_quarantined=True,
                    is_batch_rejection=True,
                )
            )
            await connection.execute(
                insert(MigrationCheckpoint).values(
                    migration_run_id=run_id,
                    entity_type="orders",
                    last_processed_legacy_id=4,
                    processed_count=2,
                )
            )
            await save_verification(
                connection,
                run_id,
                "SUCCESS_WITH_REJECTIONS",
                {
                    "passed": True,
                    "checks": {"financial": True},
                    "records": {"orders": {"source": 2}},
                },
            )

        report = await migration_report(offline, run_id)
        assert report["source_snapshot_id"] == str(snapshot_id)
        assert report["records_source"] == 2 and report["records_transferred"] == 1
        assert report["records_rejected"] == report["error_count"] == report["retry_count"] == 1
        assert report["checkpoints"][0]["last_legacy_id"] == 4
        assert report["peak_rss_bytes"] == 40 * 2**20
        assert report["duration_seconds"] >= 0
        assert report["verification_report"]["checks"]["financial"]
        markdown = render_report(report, ReportFormat.MARKDOWN)
        assert "40,000" in markdown
        assert "invalid_money" in markdown
        assert "40001" in markdown
        assert "Денежные суммы" in markdown
    finally:
        from migration_pipeline.models.migration import MigrationRetry

        async with engine.begin() as connection:
            for table in (MigrationRetry, MigrationError, MigrationCheckpoint):
                await connection.execute(delete(table).where(table.migration_run_id == run_id))
            await connection.execute(delete(MigrationRun).where(MigrationRun.id == run_id))
        await engine.dispose()
