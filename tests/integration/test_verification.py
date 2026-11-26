from contextlib import asynccontextmanager
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from sqlalchemy import delete, func, insert, select, text, update

from migration_pipeline.config import SchemaSettings
from migration_pipeline.database.connections import create_database_engine
from migration_pipeline.database.legacy import LegacySource, LegacySourceError, read_source_snapshot
from migration_pipeline.models.legacy import (
    LegacyCustomer,
    LegacyOrder,
    LegacyPayment,
    LegacySourceSnapshot,
    LegacyStatusHistory,
)
from migration_pipeline.models.migration import (
    MigrationBatch,
    MigrationCheckpoint,
    MigrationError,
    MigrationRetry,
    MigrationRun,
)
from migration_pipeline.models.target import Customer, Order, OrderItem, OrderStatusHistory, Payment
from migration_pipeline.pipeline import orchestrator
from migration_pipeline.pipeline.orchestrator import (
    MigrationFailed,
    MigrationOptions,
    MigrationOrchestrator,
    migration_status,
)
from migration_pipeline.pipeline.state import MigrationStateError
from migration_pipeline.transformers.errors import ErrorPolicy
from migration_pipeline.verification import service

pytestmark = pytest.mark.asyncio
NOW = datetime(2025, 3, 10, 12, 30, tzinfo=UTC)


@pytest_asyncio.fixture
async def verification_case(settings: SchemaSettings, monkeypatch: pytest.MonkeyPatch):
    legacy = create_database_engine(settings.legacy_admin_database, settings.db_timeout_seconds)
    target = create_database_engine(settings.target_db, settings.db_timeout_seconds)
    run_ids = []

    def observe(event, fields):
        if event == "migration_started":
            run_ids.append(UUID(fields["run_id"]))

    async with legacy.connect() as source, source.begin() as source_transaction:

        @asynccontextmanager
        async def open_source(_settings, *, expected_snapshot_id):
            snapshot = await read_source_snapshot(source, expected_id=expected_snapshot_id)
            yield LegacySource(source, snapshot)

        monkeypatch.setattr(orchestrator, "open_legacy_source", open_source)
        monkeypatch.setattr(service, "open_legacy_source", open_source)
        try:
            yield source, target, observe
        finally:
            await source_transaction.rollback()
            async with target.begin() as connection:
                for model, key in (
                    (Payment, Payment.legacy_payment_id),
                    (OrderStatusHistory, OrderStatusHistory.legacy_history_id),
                    (Order, Order.legacy_order_id),
                    (Customer, Customer.legacy_id),
                ):
                    await connection.execute(delete(model).where(key.in_((-9, 0, 7, 8, 99))))
                for model in (MigrationRetry, MigrationError, MigrationCheckpoint, MigrationBatch):
                    await connection.execute(
                        delete(model).where(model.migration_run_id.in_(run_ids))
                    )
                await connection.execute(delete(MigrationRun).where(MigrationRun.id.in_(run_ids)))
    await target.dispose()
    await legacy.dispose()


async def _populate_legacy(connection, *, invalid_customer: bool = False) -> None:
    ids = (-9, 0)
    await connection.execute(
        insert(LegacyCustomer),
        [
            {
                "legacy_id": key,
                "full_name": f"Имя Клиент{key}",
                "email": "defective" if invalid_customer and key == -9 else f"c{key}@example.org",
                "city": "Москва",
                "country": "RU",
                "phone": "+79991234567",
                "customer_status": "active",
                "extra_data": {"a": {"b": [1, None, True]}},
                "created_at": NOW,
                "updated_at": NOW,
            }
            for key in ids
        ],
    )
    await connection.execute(
        insert(LegacyOrder),
        [
            {
                "legacy_order_id": key,
                "customer_legacy_id": key,
                "order_status": "completed",
                "order_date_text": NOW.isoformat(),
                "currency": " rub ",
                "total_amount": "20.00",
                "items_json": [{"product_name": "Товар", "quantity": 2, "unit_price": "10.0"}],
                "created_at": NOW,
            }
            for key in ids
        ],
    )
    await connection.execute(
        insert(LegacyPayment),
        [
            {
                "legacy_payment_id": key,
                "legacy_order_id": key,
                "payment_reference": f"ref-{key}",
                "payment_status": "completed",
                "payment_date_text": NOW.isoformat(),
                "amount": "20.00",
            }
            for key in ids
        ],
    )
    await connection.execute(
        insert(LegacyStatusHistory),
        [
            {
                "id": key,
                "legacy_order_id": key,
                "old_status": "pending",
                "new_status": "completed",
                "changed_at_text": NOW.isoformat(),
            }
            for key in ids
        ],
    )
    await connection.execute(
        update(LegacySourceSnapshot)
        .where(LegacySourceSnapshot.singleton == 1)
        .values(snapshot_id=func.gen_random_uuid(), sealed_at=func.clock_timestamp())
    )


async def test_verification_persists_success_and_hashes_across_repeated_load(
    settings: SchemaSettings, verification_case
) -> None:
    source, target, observe = verification_case
    await _populate_legacy(source)
    first = await MigrationOrchestrator(settings, MigrationOptions(batch_size=1), observe).run()
    verified = await service.verify_run(settings, first.id)
    assert verified.run.status == "SUCCESS"
    assert all(verified.report["checks"].values())
    assert len(verified.report["checksums"]) == 8
    assert verified.report["financial"]["totals_by_currency"]["payments"]["RUB"] == {
        "source": "40.00",
        "target": "40.00",
        "difference": "0.00",
    }
    async with target.connect() as connection:
        order_ids = (
            await connection.scalars(select(Order.id).order_by(Order.legacy_order_id))
        ).all()

    repeat = await MigrationOrchestrator(settings, MigrationOptions(batch_size=2), observe).run()
    assert repeat.records_unchanged == 8
    assert not repeat.records_inserted and not repeat.records_updated
    verified_repeat = await service.verify_run(settings, repeat.id)
    assert verified_repeat.report["checksums"] == verified.report["checksums"]
    async with target.connect() as connection:
        assert (
            await connection.scalars(select(Order.id).order_by(Order.legacy_order_id))
        ).all() == order_ids

    async with target.begin() as connection:
        await connection.execute(
            update(MigrationRun)
            .where(MigrationRun.id == repeat.id)
            .values(source_snapshot_id=uuid4())
        )
    with pytest.raises(LegacySourceError, match="UUID снимка"):
        await service.verify_run(settings, repeat.id)
    persisted = await migration_status(settings, repeat.id)
    assert persisted.verified_at == verified_repeat.run.verified_at
    assert persisted.verification_report == verified_repeat.report
    assert persisted.status == "SUCCESS"


@pytest.mark.parametrize("policy", [ErrorPolicy.SKIP, ErrorPolicy.QUARANTINE, ErrorPolicy.STRICT])
async def test_verification_distinguishes_confirmed_rejections_from_failed_load(
    settings: SchemaSettings, verification_case, policy: ErrorPolicy
) -> None:
    source, target, observe = verification_case
    await _populate_legacy(source, invalid_customer=True)
    runner = MigrationOrchestrator(settings, MigrationOptions(policy=policy), observe)
    if policy is ErrorPolicy.STRICT:
        with pytest.raises(MigrationFailed) as failed:
            await runner.run()
        with pytest.raises(MigrationStateError, match="FAILED_MIGRATION"):
            await service.verify_run(settings, failed.value.run_id)
        return

    async with target.begin() as connection:
        await connection.execute(
            insert(Customer).values(
                legacy_id=-9,
                first_name="Выверенный клиент",
                status="active",
                created_at=NOW,
                updated_at=NOW,
            )
        )
    run = await runner.run()
    result = await service.verify_run(settings, run.id)
    assert result.run.status == "SUCCESS_WITH_REJECTIONS"
    assert result.run.records_rejected == 4
    for entity in result.report["records"].values():
        assert (entity["source"], entity["matched"], entity["rejected"]) == (2, 1, 1)
        assert entity["quarantined"] == (1 if policy is ErrorPolicy.QUARANTINE else 0)
    assert result.report["records"]["customers"]["rejected_present_in_target"] == 1
    assert result.report["rejection_audit"]["matched_to_source"] == 4

    async with target.begin() as connection:
        await connection.execute(
            update(MigrationError)
            .where(
                MigrationError.migration_run_id == run.id, MigrationError.entity_type == "orders"
            )
            .values(legacy_record_id=99)
        )
    result = await service.verify_run(settings, run.id)
    assert result.run.status == "FAILED_VERIFICATION"
    assert result.report["rejection_audit"]["matched_to_source"] == 3
    assert not result.report["checks"]["completeness"]


async def test_verification_detects_independent_content_money_and_reference_changes(
    settings: SchemaSettings, verification_case
) -> None:
    source, target, observe = verification_case
    await _populate_legacy(source)
    loaded = await MigrationOrchestrator(settings, MigrationOptions(), observe).run()

    async with target.begin() as connection:
        await connection.execute(
            update(Customer).where(Customer.legacy_id == -9).values(first_name="Изменено")
        )
    result = await service.verify_run(settings, loaded.id)
    assert result.run.status == "FAILED_VERIFICATION"
    assert result.report["checks"] == {
        "record_counts": True,
        "completeness": True,
        "foreign_keys": True,
        "financial": True,
        "checksums": False,
    }

    async with target.begin() as connection:
        await connection.execute(
            update(Order).where(Order.legacy_order_id == -9).values(total_amount=21)
        )
        await connection.execute(
            update(Order).where(Order.legacy_order_id == 0).values(total_amount=19)
        )
        await connection.execute(
            update(OrderItem)
            .where(
                OrderItem.order_id
                == select(Order.id).where(Order.legacy_order_id == -9).scalar_subquery()
            )
            .values(unit_price=11, line_total=22)
        )
        await connection.execute(
            update(Payment).where(Payment.legacy_payment_id == -9).values(amount=21)
        )
    result = await service.verify_run(settings, loaded.id)
    assert not result.report["checks"]["financial"]
    financial = result.report["financial"]
    assert financial["totals_by_currency"]["orders"]["RUB"]["difference"] == "0.00"
    assert financial["value_mismatches"] == {"orders": 2, "order_items": 1, "payments": 1}
    assert financial["invalid_order_totals"] == 2

    owner = create_database_engine(settings.admin_database("target"), settings.db_timeout_seconds)
    try:
        async with owner.begin() as connection:
            await connection.execute(text("SET LOCAL session_replication_role = replica"))
            await connection.execute(
                update(OrderStatusHistory)
                .where(OrderStatusHistory.legacy_history_id == -9)
                .values(order_id=uuid4())
            )
            await connection.execute(
                update(OrderStatusHistory)
                .where(OrderStatusHistory.legacy_history_id == 0)
                .values(legacy_history_id=99)
            )
        result = await service.verify_run(settings, loaded.id)
        assert result.report["foreign_key_violations"]["order_status_history"] == 1
        assert result.report["records"]["status_history"]["missing"] == 1
        assert result.report["records"]["status_history"]["unexpected"] == 1
        assert not result.report["checks"]["foreign_keys"]
        assert not result.report["checks"]["record_counts"]
        persisted = await migration_status(settings, loaded.id)
        assert persisted.status == "FAILED_VERIFICATION"
        assert persisted.verification_report == result.report
    finally:
        await owner.dispose()
