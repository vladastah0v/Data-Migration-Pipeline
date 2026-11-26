from contextlib import asynccontextmanager
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from sqlalchemy import delete, func, insert, select, text, update

from migration_pipeline.config import SchemaSettings
from migration_pipeline.database.connections import create_database_engine
from migration_pipeline.database.legacy import LegacySource, read_source_snapshot
from migration_pipeline.models.legacy import LegacyCustomer, LegacySourceSnapshot
from migration_pipeline.models.migration import (
    MigrationBatch,
    MigrationCheckpoint,
    MigrationError,
    MigrationRetry,
    MigrationRun,
)
from migration_pipeline.models.target import Customer
from migration_pipeline.pipeline import orchestrator
from migration_pipeline.pipeline.sessions import MIGRATION_LOCK_KEY, MigrationBusyError

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("failure", ["disconnect", "lost_commit", "source"])
async def test_retry_commits_only_one_copy_of_a_batch(
    settings: SchemaSettings, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    customer_key = uuid4().int % (2**62) + 1
    run_ids = []
    events = []
    failed = False
    target = create_database_engine(settings.target_db, settings.db_timeout_seconds)
    target_owner = create_database_engine(
        settings.admin_database("target"), settings.db_timeout_seconds
    )
    legacy = create_database_engine(settings.legacy_admin_database, settings.db_timeout_seconds)
    original_confirm = orchestrator.confirm_batch
    original_commit = orchestrator.MigrationOrchestrator._commit_batch
    original_read = orchestrator._read_batch

    def observe(event, fields):
        events.append((event, fields))
        if event == "migration_started":
            run_ids.append(UUID(fields["run_id"]))

    async def disconnect_during_batch(connection, run_id, batch, loaded, issues):
        nonlocal failed
        await original_confirm(connection, run_id, batch, loaded, issues)
        if not failed:
            failed = True
            pid = await connection.scalar(text("SELECT pg_backend_pid()"))
            async with target_owner.connect() as admin:
                assert await admin.scalar(
                    text("SELECT pg_terminate_backend(:pid, 5000)"), {"pid": pid}
                )
            await connection.execute(text("SELECT 1"))

    async def lose_commit_reply(self, execution, batch):
        nonlocal failed
        metrics = await original_commit(self, execution, batch)
        if not failed:
            failed = True
            raise ConnectionResetError
        return metrics

    async def disconnect_before_read(*args):
        nonlocal failed
        if not failed:
            failed = True
            raise ConnectionResetError
        return await original_read(*args)

    if failure == "disconnect":
        monkeypatch.setattr(orchestrator, "confirm_batch", disconnect_during_batch)
    elif failure == "lost_commit":
        monkeypatch.setattr(orchestrator.MigrationOrchestrator, "_commit_batch", lose_commit_reply)
    else:
        monkeypatch.setattr(orchestrator, "_read_batch", disconnect_before_read)

    try:
        async with target.connect() as connection:
            assert (
                await connection.scalar(
                    select(Customer.id).where(Customer.legacy_id == customer_key)
                )
                is None
            )
        async with legacy.connect() as source, source.begin() as source_transaction:
            now = datetime.now(UTC)
            await source.execute(
                insert(LegacyCustomer).values(
                    legacy_id=customer_key,
                    full_name="Synthetic Customer",
                    customer_status="active",
                    created_at=now,
                    updated_at=now,
                )
            )
            await source.execute(
                update(LegacySourceSnapshot)
                .where(LegacySourceSnapshot.singleton == 1)
                .values(snapshot_id=func.gen_random_uuid(), sealed_at=func.clock_timestamp())
            )

            @asynccontextmanager
            async def open_source(_settings, *, expected_snapshot_id):
                snapshot = await read_source_snapshot(source, expected_id=expected_snapshot_id)
                yield LegacySource(source, snapshot)

            monkeypatch.setattr(orchestrator, "open_legacy_source", open_source)
            options = orchestrator.MigrationOptions(batch_size=10, retry_delay=0, max_retries=1)
            result = await orchestrator.MigrationOrchestrator(settings, options, observe).run()
            await source_transaction.rollback()
        assert failed and result.retry_count == 1
        assert (result.status, result.records_processed, result.records_inserted) == (
            "LOADED",
            1,
            1,
        )
        assert [checkpoint.last_id for checkpoint in result.checkpoints] == [customer_key]
        retries = [fields for event, fields in events if event == "migration_retry"]
        assert len(retries) == 1 and retries[0]["attempt"] == 1
        if failure == "lost_commit":
            assert [
                fields.get("reconciled")
                for event, fields in events
                if event == "migration_batch_committed"
            ] == [True]
        async with target.connect() as connection:
            assert (
                await connection.scalar(
                    select(Customer.id).where(Customer.legacy_id == customer_key)
                )
                is not None
            )
            assert list(
                await connection.scalars(
                    select(MigrationBatch.status).where(
                        MigrationBatch.migration_run_id == result.id
                    )
                )
            ) == ["COMMITTED"]
    finally:
        if run_ids:
            async with target.begin() as connection:
                await connection.execute(delete(Customer).where(Customer.legacy_id == customer_key))
                for model in (MigrationRetry, MigrationError, MigrationCheckpoint, MigrationBatch):
                    await connection.execute(
                        delete(model).where(model.migration_run_id.in_(run_ids))
                    )
                await connection.execute(delete(MigrationRun).where(MigrationRun.id.in_(run_ids)))
        await legacy.dispose()
        await target_owner.dispose()
        await target.dispose()


async def test_resume_does_not_take_over_live_target(settings: SchemaSettings) -> None:
    engine = create_database_engine(settings.target_db, settings.db_timeout_seconds)
    try:
        async with engine.connect() as current:
            await current.execute(
                text("SELECT pg_advisory_lock(:key)"), {"key": MIGRATION_LOCK_KEY}
            )
            with pytest.raises(MigrationBusyError, match="выполняется другой запуск"):
                await orchestrator.MigrationOrchestrator(
                    settings, orchestrator.MigrationOptions()
                ).resume(uuid4())
    finally:
        await engine.dispose()
