from collections.abc import AsyncIterator
from contextlib import aclosing, asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from alembic.script import ScriptDirectory
from sqlalchemy import func, insert, select, text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.sql import Executable

from migration_pipeline.config import DatabaseSettings, SchemaSettings
from migration_pipeline.database.connections import create_database_engine, read_server_version
from migration_pipeline.database.legacy import (
    LegacySource,
    LegacySourceError,
    open_legacy_source,
    read_source_snapshot,
)
from migration_pipeline.database.schema import revision_config, upgrade_schema
from migration_pipeline.extractors.base import BIGINT_MAX, BIGINT_MIN, LegacyEntity, extract_batches
from migration_pipeline.models.legacy import (
    LegacyCustomer,
    LegacyOrder,
    LegacyPayment,
    LegacySourceSnapshot,
    LegacyStatusHistory,
)
from migration_pipeline.models.migration import MigrationBatch, MigrationCheckpoint, MigrationRun
from migration_pipeline.models.target import (
    Address,
    Customer,
    CustomerContact,
    Order,
    OrderItem,
    Payment,
)
from migration_pipeline.seeding.data import SeedOptions
from migration_pipeline.seeding.service import SeedError, _seed_source, seed_legacy
from migration_pipeline.transformers import preview
from migration_pipeline.transformers.errors import ErrorPolicy

pytestmark = pytest.mark.asyncio
SEAL_SOURCE = (
    update(LegacySourceSnapshot)
    .where(LegacySourceSnapshot.singleton == 1)
    .values(snapshot_id=func.gen_random_uuid(), sealed_at=func.clock_timestamp())
    .returning(LegacySourceSnapshot.snapshot_id)
)


@pytest.fixture
def legacy_id() -> int:
    return uuid4().int % (2**62) + 1


@asynccontextmanager
async def transaction(database: DatabaseSettings, timeout: float) -> AsyncIterator[AsyncConnection]:
    engine = create_database_engine(database, timeout)
    try:
        async with engine.connect() as connection, connection.begin() as active_transaction:
            yield connection
            await active_transaction.rollback()
    finally:
        await engine.dispose()


async def assert_rejected(
    connection: AsyncConnection, statement: Executable, sqlstate: str
) -> None:
    with pytest.raises(DBAPIError) as result:
        async with connection.begin_nested():
            await connection.execute(statement)
    assert result.value.orig.sqlstate == sqlstate


async def test_independent_revisions_and_application_grants(settings: SchemaSettings) -> None:
    async with transaction(settings.legacy_db, settings.db_timeout_seconds) as legacy:
        head = ScriptDirectory.from_config(revision_config("legacy")).get_current_head()
        assert await legacy.scalar(text("SELECT version_num FROM alembic_version")) == head
        assert await legacy.scalar(text("SELECT to_regclass('customers')")) is None
        await assert_rejected(legacy, text("DELETE FROM legacy_customers WHERE false"), "42501")

    async with transaction(settings.target_db, settings.db_timeout_seconds) as target:
        head = ScriptDirectory.from_config(revision_config("target")).get_current_head()
        assert await target.scalar(text("SELECT version_num FROM alembic_version")) == head
        assert await target.scalar(text("SELECT to_regclass('legacy_customers')")) is None
        await assert_rejected(target, text("DELETE FROM alembic_version WHERE false"), "42501")
        await assert_rejected(
            target, text("CREATE TABLE public.schema_privilege_test (id int)"), "42501"
        )


async def test_repeated_upgrade_and_runtime_connections_succeed(settings: SchemaSettings) -> None:
    await upgrade_schema(settings, "legacy")
    await upgrade_schema(settings, "target")

    for database in (settings.legacy_db, settings.target_db):
        version = await read_server_version(database, settings.db_timeout_seconds)
        assert version // 10_000 == 16


async def test_legacy_accepts_dirty_fields_but_checks_relations(
    settings: SchemaSettings, legacy_id: int
) -> None:
    now = datetime.now(UTC)
    async with transaction(settings.admin_database("legacy"), settings.db_timeout_seconds) as conn:
        await conn.execute(
            insert(LegacyCustomer).values(
                legacy_id=legacy_id, email="invalid", created_at=now, updated_at=now
            )
        )
        order = insert(LegacyOrder).values(
            legacy_order_id=legacy_id,
            customer_legacy_id=legacy_id,
            order_date_text="not-a-date",
            total_amount="not-money",
            items_json={"invalid": "shape"},
            created_at=now,
        )
        await assert_rejected(conn, order.values(customer_legacy_id=legacy_id + 1), "23503")
        await conn.execute(order)


async def test_seal_prevents_writes_and_preserves_snapshot_id(
    settings: SchemaSettings, legacy_id: int
) -> None:
    now = datetime.now(UTC)
    async with transaction(settings.admin_database("legacy"), settings.db_timeout_seconds) as conn:
        assert await conn.scalar(select(LegacySourceSnapshot.snapshot_id)) is None
        row = insert(LegacyCustomer).values(legacy_id=legacy_id, created_at=now, updated_at=now)
        await conn.execute(row)
        snapshot_id = await conn.scalar(SEAL_SOURCE)
        assert snapshot_id is not None
        await assert_rejected(conn, row.values(legacy_id=legacy_id + 1), "55000")
        await assert_rejected(conn, text("UPDATE legacy_orders SET currency = 'USD'"), "55000")
        await assert_rejected(conn, text("DELETE FROM legacy_payments"), "55000")
        await assert_rejected(conn, text("TRUNCATE legacy_status_history"), "55000")
        await assert_rejected(conn, text("TRUNCATE legacy_source_snapshot"), "55000")
        await assert_rejected(conn, SEAL_SOURCE, "55000")
        assert await conn.scalar(select(LegacySourceSnapshot.snapshot_id)) == snapshot_id


async def test_seal_waits_for_open_legacy_write(settings: SchemaSettings, legacy_id: int) -> None:
    now = datetime.now(UTC)
    owner = settings.admin_database("legacy")
    async with transaction(owner, settings.db_timeout_seconds) as sealer:
        await sealer.execute(text("SET LOCAL lock_timeout = '250ms'"))
        async with transaction(owner, settings.db_timeout_seconds) as writer:
            await writer.execute(
                insert(LegacyCustomer).values(legacy_id=legacy_id, created_at=now, updated_at=now)
            )
            await assert_rejected(sealer, SEAL_SOURCE, "55P03")
        assert await sealer.scalar(SEAL_SOURCE) is not None


def ignore_progress(_table: str, _done: int, _total: int) -> None:
    pass


async def test_seed_copies_and_seals_in_one_transaction(settings: SchemaSettings) -> None:
    options = SeedOptions(
        batch_size=337, invalid_rate=Decimal("0.02"), duplicate_contact_rate=Decimal("0.1")
    )
    async with transaction(settings.legacy_admin_database, settings.db_timeout_seconds) as conn:
        result = await _seed_source(conn, options, ignore_progress)

        assert [table.rows for table in result.tables] == [1_000, 5_000, 5_000, 10_000]
        assert [table.invalid_rows for table in result.tables] == [20, 100, 100, 200]
        assert await conn.scalar(select(LegacySourceSnapshot.snapshot_id)) == result.snapshot_id
        duplicates = await conn.scalar(
            text("""
                SELECT count(*) - count(DISTINCT lower(btrim(email))) FROM legacy_customers
                WHERE email LIKE '%@%'
                """)
        )
        assert duplicates == result.duplicate_contacts == 97
        assert (
            await conn.scalar(
                text("SELECT count(*) FROM legacy_orders WHERE jsonb_typeof(items_json) = 'object'")
            )
            == 20
        )
        with pytest.raises(SeedError, match="уже зафиксирован"):
            await _seed_source(conn, options, ignore_progress)


async def test_failed_seed_rolls_back_copied_rows(settings: SchemaSettings) -> None:
    class Interrupted(RuntimeError):
        pass

    def fail_after_first_batch(_table: str, done: int, _total: int) -> None:
        if done:
            raise Interrupted

    with pytest.raises(Interrupted):
        await seed_legacy(settings, SeedOptions(batch_size=10), fail_after_first_batch)

    async with transaction(settings.legacy_db, settings.db_timeout_seconds) as conn:
        assert await conn.scalar(select(func.count()).select_from(LegacyCustomer)) == 0
        assert await conn.scalar(select(LegacySourceSnapshot.snapshot_id)) is None


async def test_seed_preserves_existing_data(settings: SchemaSettings, legacy_id: int) -> None:
    now = datetime.now(UTC)
    async with transaction(settings.legacy_admin_database, settings.db_timeout_seconds) as conn:
        await conn.execute(
            insert(LegacyCustomer).values(legacy_id=legacy_id, created_at=now, updated_at=now)
        )

        with pytest.raises(SeedError, match="уже содержит данные"):
            await _seed_source(conn, SeedOptions(), ignore_progress)

        assert await conn.scalar(select(LegacyCustomer.legacy_id)) == legacy_id
        assert await conn.scalar(select(LegacySourceSnapshot.snapshot_id)) is None


async def test_open_source_rejects_an_unsealed_database(settings: SchemaSettings) -> None:
    with pytest.raises(LegacySourceError, match="ещё не зафиксирован"):
        async with open_legacy_source(settings):
            pytest.fail("Открытый снимок недоступен для извлечения.")


async def test_extract_resumes_across_sparse_signed_ids(
    settings: SchemaSettings, caplog: pytest.LogCaptureFixture
) -> None:
    now = datetime.now(UTC)
    keys = [BIGINT_MIN, -7, 0, 41, BIGINT_MAX]
    caplog.set_level("INFO", logger="migration_pipeline.extractors.base")

    async with transaction(settings.legacy_admin_database, settings.db_timeout_seconds) as conn:
        await conn.execute(
            insert(LegacyCustomer),
            [
                {"legacy_id": key, "created_at": now, "updated_at": now, "extra_data": {"raw": key}}
                for key in reversed(keys)
            ],
        )
        await conn.execute(SEAL_SOURCE)
        source = LegacySource(conn, await read_source_snapshot(conn))

        async with aclosing(
            extract_batches(source, LegacyEntity.CUSTOMERS, batch_size=2)
        ) as batches:
            first = await anext(batches)
        assert (first.first_id, first.last_id, len(first.rows)) == (BIGINT_MIN, -7, 2)
        assert first.rows[0]["extra_data"] == {"raw": BIGINT_MIN}
        resumed = [
            batch
            async for batch in extract_batches(
                source,
                LegacyEntity.CUSTOMERS,
                batch_size=2,
                after_id=first.last_id,
                start_batch_number=first.number + 1,
            )
        ]
        assert [(batch.number, batch.first_id, batch.last_id) for batch in resumed] == [
            (2, 0, 41),
            (3, BIGINT_MAX, BIGINT_MAX),
        ]
        assert [row["legacy_id"] for batch in (first, *resumed) for row in batch.rows] == keys
        assert [
            row["legacy_id"]
            async for batch in extract_batches(source, LegacyEntity.CUSTOMERS, after_id=0)
            for row in batch.rows
        ] == [41, BIGINT_MAX]
        assert [
            batch
            async for batch in extract_batches(source, LegacyEntity.CUSTOMERS, after_id=BIGINT_MAX)
        ] == []

    first_log = next(record for record in caplog.records if record.message == "extract_batch")
    assert first_log.entity_type == "customers"
    assert first_log.batch_number == 1
    assert first_log.records_extracted == 2
    assert first_log.last_processed_id == -7
    assert first_log.extraction_duration_seconds >= 0


async def test_extract_uses_each_entity_primary_key_and_checks_snapshot(
    settings: SchemaSettings, legacy_id: int
) -> None:
    now = datetime.now(UTC)
    async with transaction(settings.legacy_admin_database, settings.db_timeout_seconds) as conn:
        await conn.execute(
            insert(LegacyCustomer).values(legacy_id=legacy_id, created_at=now, updated_at=now)
        )
        await conn.execute(
            insert(LegacyOrder).values(
                legacy_order_id=legacy_id + 1,
                customer_legacy_id=legacy_id,
                created_at=now,
            )
        )
        await conn.execute(
            insert(LegacyPayment).values(
                legacy_payment_id=legacy_id + 2,
                legacy_order_id=legacy_id + 1,
            )
        )
        await conn.execute(
            insert(LegacyStatusHistory).values(id=legacy_id + 3, legacy_order_id=legacy_id + 1)
        )
        snapshot_id = await conn.scalar(SEAL_SOURCE)
        with pytest.raises(LegacySourceError, match="UUID снимка legacy изменился"):
            await read_source_snapshot(conn, expected_id=uuid4())
        snapshot = await read_source_snapshot(conn, expected_id=snapshot_id)
        source = LegacySource(conn, snapshot)

        for offset, entity in enumerate(LegacyEntity):
            assert [page.last_id async for page in extract_batches(source, entity)] == [
                legacy_id + offset
            ]


async def test_preview_checks_payment_references_between_batches_and_reports_emails(
    settings: SchemaSettings, legacy_id: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = datetime.now(UTC)
    async with transaction(settings.legacy_admin_database, settings.db_timeout_seconds) as conn:
        await conn.execute(
            insert(LegacyCustomer),
            [
                dict(
                    legacy_id=key,
                    full_name="Synthetic Customer",
                    email="same@example.test",
                    customer_status="active",
                    created_at=now,
                    updated_at=now,
                )
                for key in (legacy_id, legacy_id + 1)
            ],
        )
        await conn.execute(
            insert(LegacyOrder).values(
                legacy_order_id=legacy_id,
                customer_legacy_id=legacy_id,
                currency="USD",
                order_status="completed",
                order_date_text=now.isoformat(),
                total_amount="10.02",
                created_at=now,
                items_json=[{"product_name": "Product", "quantity": 1, "unit_price": "10.02"}],
            )
        )
        await conn.execute(
            insert(LegacyPayment),
            [
                dict(
                    legacy_payment_id=key,
                    legacy_order_id=legacy_id,
                    payment_reference=ref,
                    amount="10.02",
                    payment_status="completed",
                    payment_date_text=now.isoformat(),
                )
                for key, ref in ((legacy_id, " REF-TEST "), (legacy_id + 1, "REF-TEST"))
            ],
        )
        await conn.execute(SEAL_SOURCE)

        @asynccontextmanager
        async def open_source(_settings, *, expected_snapshot_id):
            snapshot = await read_source_snapshot(conn, expected_id=expected_snapshot_id)
            yield LegacySource(conn, snapshot)

        monkeypatch.setattr(preview, "open_legacy_source", open_source)
        quarantined = []
        duplicates = []
        summary = await preview.preview_transform(
            settings,
            batch_size=1,
            policy=ErrorPolicy.QUARANTINE,
            snapshot_id=None,
            quarantine=quarantined.append,
            duplicates=duplicates.append,
            progress=lambda *_: None,
        )

    assert [(item.accepted, item.rejected) for item in summary.entities] == [
        (2, 0),
        (1, 0),
        (1, 1),
        (0, 0),
    ]
    assert summary.error_counts == {"payment_reference_conflict": 1}
    assert summary.duplicate_email_groups == 1 and summary.duplicate_email_members == 2
    assert len(quarantined) == 1
    assert quarantined[0]["legacy_record_id"] == legacy_id + 1
    assert "REF-TEST" not in str(quarantined)
    assert [row["legacy_record_id"] for row in duplicates] == [legacy_id, legacy_id + 1]
    assert "same@example.test" not in str(duplicates)
    async with transaction(settings.target_db, settings.db_timeout_seconds) as target:
        assert (
            await target.scalar(select(Customer.id).where(Customer.legacy_id == legacy_id)) is None
        )


async def test_normalized_customer_keys_deduplicate_within_one_customer(
    settings: SchemaSettings, legacy_id: int
) -> None:
    now = datetime.now(UTC)
    async with transaction(settings.target_db, settings.db_timeout_seconds) as conn:
        customer = (
            insert(Customer)
            .values(
                legacy_id=legacy_id,
                first_name="Анна",
                email="shared@example.test",
                status="active",
                created_at=now,
                updated_at=now,
            )
            .returning(Customer.id)
        )
        customer_id = await conn.scalar(customer)
        assert await conn.scalar(customer.values(legacy_id=legacy_id + 1)) != customer_id
        await assert_rejected(conn, customer, "23505")
        address = insert(Address).values(customer_id=customer_id, city="Екатеринбург")
        await conn.execute(address)
        await assert_rejected(conn, address, "23505")
        primary_contact = insert(CustomerContact).values(
            customer_id=customer_id,
            contact_type="email",
            contact_value="shared@example.test",
            is_primary=True,
        )
        await conn.execute(primary_contact)
        await assert_rejected(
            conn, primary_contact.values(contact_value="another@example.test"), "23505"
        )


async def test_target_checks_order_links_money_and_payment_references(
    settings: SchemaSettings, legacy_id: int
) -> None:
    now = datetime.now(UTC)
    async with transaction(settings.target_db, settings.db_timeout_seconds) as conn:
        customer_id = await conn.scalar(
            insert(Customer)
            .values(
                legacy_id=legacy_id,
                first_name="Анна",
                status="active",
                created_at=now,
                updated_at=now,
            )
            .returning(Customer.id)
        )
        order = (
            insert(Order)
            .values(
                legacy_order_id=legacy_id,
                customer_id=customer_id,
                status="pending",
                currency="RUB",
                total_amount=Decimal("20.04"),
                ordered_at=now,
                created_at=now,
            )
            .returning(Order.id)
        )
        await assert_rejected(conn, order.values(customer_id=uuid4()), "23503")
        await assert_rejected(conn, order.values(total_amount=Decimal("NaN")), "23514")
        order_id = await conn.scalar(order)
        item = insert(OrderItem).values(
            order_id=order_id,
            line_number=1,
            product_name="Товар",
            quantity=2,
            unit_price=Decimal("10.02"),
            line_total=Decimal("20.04"),
        )
        await assert_rejected(conn, item.values(line_total=Decimal("20.05")), "23514")
        await conn.execute(item)
        await assert_rejected(conn, item, "23505")
        payment = insert(Payment).values(
            legacy_payment_id=legacy_id,
            order_id=order_id,
            payment_reference=f"payment-{legacy_id}",
            status="completed",
            amount=Decimal("20.04"),
            paid_at=now,
        )
        await assert_rejected(conn, payment.values(amount=Decimal("-1.00")), "23514")
        await conn.execute(payment)
        await assert_rejected(conn, payment.values(legacy_payment_id=legacy_id + 1), "23505")


async def test_batch_and_checkpoint_constraints_and_rollback(
    settings: SchemaSettings, legacy_id: int
) -> None:
    async with transaction(settings.target_db, settings.db_timeout_seconds) as conn:
        run_id = await conn.scalar(
            insert(MigrationRun)
            .values(configuration_hash="a" * 64, source_snapshot_id=uuid4())
            .returning(MigrationRun.id)
        )
        batch = insert(MigrationBatch).values(
            migration_run_id=run_id,
            entity_type="customers",
            batch_number=1,
            first_legacy_id=legacy_id,
            last_legacy_id=legacy_id,
            records_count=1,
        )
        await assert_rejected(conn, batch.values(status="COMMITTED"), "23514")
        await conn.execute(batch)
        await assert_rejected(conn, batch, "23505")
        checkpoint = insert(MigrationCheckpoint).values(
            migration_run_id=run_id,
            entity_type="customers",
            last_processed_legacy_id=legacy_id,
            processed_count=1,
        )
        now = datetime.now(UTC)
        async with conn.begin_nested() as load_batch:
            await conn.execute(
                insert(Customer).values(
                    legacy_id=legacy_id,
                    first_name="Анна",
                    status="active",
                    created_at=now,
                    updated_at=now,
                )
            )
            await conn.execute(checkpoint)
            await assert_rejected(conn, checkpoint, "23505")
            await load_batch.rollback()
        assert (
            await conn.scalar(
                select(MigrationCheckpoint.id).where(MigrationCheckpoint.migration_run_id == run_id)
            )
            is None
        )
        assert await conn.scalar(select(Customer.id).where(Customer.legacy_id == legacy_id)) is None
