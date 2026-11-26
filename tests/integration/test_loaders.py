from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import Text, cast, func, select, text
from sqlalchemy.ext.asyncio import AsyncConnection

from migration_pipeline.config import SchemaSettings
from migration_pipeline.database.connections import create_database_engine
from migration_pipeline.database.schema import SCHEMA_LOCK_KEY
from migration_pipeline.extractors.base import ExtractedBatch, LegacyEntity
from migration_pipeline.loaders.base import (
    ConflictPolicy,
    LoadConflict,
    LoadError,
    LoadStrategy,
    load_batch,
)
from migration_pipeline.models.migration import MigrationBatch
from migration_pipeline.models.target import (
    Address,
    Customer,
    CustomerAttribute,
    CustomerContact,
    Order,
    OrderItem,
    OrderStatusHistory,
    Payment,
)
from migration_pipeline.pipeline.checkpoint import CheckpointError
from migration_pipeline.pipeline.orchestrator import MigrationOptions
from migration_pipeline.pipeline.state import confirm_batch, create_run, read_run
from migration_pipeline.transformers.customers import AddressData, ContactData, CustomerData
from migration_pipeline.transformers.orders import OrderData, OrderItemData
from migration_pipeline.transformers.payments import PaymentData
from migration_pipeline.transformers.status_history import StatusEventData

pytestmark = pytest.mark.asyncio
STAMP = datetime(2024, 1, 2, tzinfo=UTC)
AMOUNT = Decimal("20.04")


@pytest_asyncio.fixture
async def target(settings: SchemaSettings):
    engine = create_database_engine(settings.target_db, settings.db_timeout_seconds)
    try:
        async with engine.connect() as connection, connection.begin() as transaction:
            yield connection
            await transaction.rollback()
    finally:
        await engine.dispose()


@pytest.fixture
def customer() -> CustomerData:
    key = uuid4().int % (2**62) + 1
    return CustomerData(
        key,
        "Анна",
        None,
        "same@example.test",
        None,
        "active",
        STAMP,
        STAMP,
        (ContactData("email", "same@example.test"), ContactData("phone", "+12025550100")),
        AddressData("Город", "RU", None),
        {"precision": {"numbers": [Decimal("123456789.123456789123456789"), True, None]}},
    )


def order_for(customer: CustomerData) -> OrderData:
    return OrderData(
        customer.legacy_id + 10,
        customer.legacy_id,
        "ORD-1",
        "completed",
        "USD",
        AMOUNT,
        STAMP,
        STAMP,
        (OrderItemData(1, "Товар", 2, Decimal("10.02"), AMOUNT),),
    )


@pytest.mark.parametrize("strategy", list(LoadStrategy))
async def test_load_strategies_preserve_relations_and_replay(
    target: AsyncConnection, customer: CustomerData, strategy: LoadStrategy
) -> None:
    second = replace(customer, legacy_id=customer.legacy_id + 1)
    orders = tuple(order_for(item) for item in (customer, second))
    payments = tuple(
        PaymentData(
            order.legacy_id + 10,
            order.legacy_id,
            f"REF-{order.legacy_id}",
            "card",
            "completed",
            AMOUNT,
            STAMP,
        )
        for order in orders
    )
    history = tuple(
        StatusEventData(order.legacy_id + 20, order.legacy_id, None, "completed", STAMP)
        for order in orders
    )
    batches = (
        ((customer, second), [2, 4, 2, 2]),
        (orders, [2, 2]),
        (payments, [2]),
        (history, [2]),
    )
    for batch, expected in batches:
        loaded = await load_batch(target, batch, strategy=strategy)
        assert (loaded.inserted, loaded.updated, loaded.unchanged) == (2, 0, 0)
        assert [item.inserted for item in loaded.tables] == expected

    keys = [customer.legacy_id, second.legacy_id]
    customer_ids = dict(
        (
            await target.execute(
                select(Customer.legacy_id, Customer.id).where(Customer.legacy_id.in_(keys))
            )
        ).all()
    )
    assert len(customer_ids) == 2

    for batch, expected in batches:
        replay = await load_batch(target, batch, strategy=strategy)
        assert (replay.inserted, replay.updated, replay.unchanged) == (0, 0, 2)
        assert [item.unchanged for item in replay.tables] == expected

    assert customer_ids == dict(
        (
            await target.execute(
                select(Customer.legacy_id, Customer.id).where(Customer.legacy_id.in_(keys))
            )
        ).all()
    )
    customer_id = customer_ids[customer.legacy_id]
    assert (
        await target.scalar(
            select(func.count())
            .select_from(CustomerContact)
            .where(CustomerContact.customer_id == customer_id)
        )
        == 2
    )
    assert (
        await target.scalar(
            select(func.count()).select_from(Address).where(Address.customer_id == customer_id)
        )
        == 1
    )
    value = await target.scalar(
        select(cast(CustomerAttribute.value, Text)).where(
            CustomerAttribute.customer_id == customer_id
        )
    )
    assert "123456789.123456789123456789" in value

    order_id = await target.scalar(
        select(Order.id).where(
            Order.legacy_order_id == orders[0].legacy_id, Order.customer_id == customer_id
        )
    )
    assert order_id is not None
    assert (
        await target.scalar(select(OrderItem.line_total).where(OrderItem.order_id == order_id))
        == AMOUNT
    )
    assert await target.scalar(select(Payment.amount).where(Payment.order_id == order_id)) == AMOUNT
    assert (
        await target.scalar(
            select(OrderStatusHistory.new_status).where(OrderStatusHistory.order_id == order_id)
        )
        == "completed"
    )
    assert (
        await target.scalar(
            text(
                "SELECT count(*) FROM pg_class "
                "WHERE relpersistence = 't' AND relname LIKE 'loader_%'"
            )
        )
        == 0
    )


@pytest.mark.parametrize("strategy", list(LoadStrategy))
async def test_conflicting_child_rolls_back_entire_batch(
    target: AsyncConnection, customer: CustomerData, strategy: LoadStrategy
) -> None:
    valid = customer
    invalid = replace(
        customer,
        legacy_id=customer.legacy_id + 1,
        contacts=(
            ContactData("email", "first@example.test"),
            ContactData("email", "second@example.test"),
        ),
    )
    with pytest.raises(LoadConflict, match="конфликт уникального ключа"):
        await load_batch(target, (valid, invalid), strategy=strategy)
    assert (
        await target.scalar(
            select(func.count())
            .select_from(Customer)
            .where(Customer.legacy_id.in_([valid.legacy_id, invalid.legacy_id]))
        )
        == 0
    )


@pytest.mark.parametrize("strategy", [LoadStrategy.UPSERT, LoadStrategy.COPY])
async def test_updates_require_opt_in_and_preserve_identity(
    target: AsyncConnection, customer: CustomerData, strategy: LoadStrategy
) -> None:
    await load_batch(target, (customer,), strategy=strategy)
    key = Customer.legacy_id == customer.legacy_id
    original_uuid = await target.scalar(select(Customer.id).where(key))
    modified = replace(customer, first_name="Ольга", attributes={"precision": {"numbers": [1]}})
    with pytest.raises(LoadConflict, match="целевая запись"):
        await load_batch(target, (modified,), strategy=strategy)
    assert await target.scalar(select(Customer.first_name).where(key)) == "Анна"

    result = await load_batch(
        target, (modified,), strategy=strategy, conflict_policy=ConflictPolicy.UPDATE
    )
    assert (result.inserted, result.updated, result.unchanged) == (0, 1, 0)
    assert result.tables[-1].updated == 1
    assert await target.scalar(select(Customer.id).where(key)) == original_uuid
    assert await target.scalar(select(Customer.first_name).where(key)) == "Ольга"
    replay = await load_batch(
        target, (modified,), strategy=strategy, conflict_policy=ConflictPolicy.UPDATE
    )
    assert (replay.updated, replay.unchanged) == (0, 1)
    with pytest.raises(LoadConflict, match="целевая запись"):
        await load_batch(
            target,
            (replace(modified, created_at=STAMP - timedelta(days=1)),),
            strategy=strategy,
            conflict_policy=ConflictPolicy.UPDATE,
        )


async def test_loader_rejects_missing_parent_and_payment_reference_conflict(
    target: AsyncConnection, customer: CustomerData
) -> None:
    order = order_for(customer)
    with pytest.raises(LoadError, match="отсутствуют целевые записи"):
        await load_batch(target, (order,), strategy=LoadStrategy.UPSERT)
    await load_batch(target, (customer,), strategy=LoadStrategy.UPSERT)
    await load_batch(target, (order,), strategy=LoadStrategy.UPSERT)
    payment = PaymentData(
        order.legacy_id + 1,
        order.legacy_id,
        f"REF-{order.legacy_id}",
        None,
        "completed",
        AMOUNT,
        STAMP,
    )
    await load_batch(target, (payment,), strategy=LoadStrategy.UPSERT)
    conflicting = replace(payment, legacy_id=payment.legacy_id + 1)
    with pytest.raises(LoadConflict):
        await load_batch(target, (conflicting,), strategy=LoadStrategy.COPY)
    assert (
        await target.scalar(
            select(Payment.legacy_payment_id).where(Payment.payment_reference == payment.reference)
        )
        == payment.legacy_id
    )


async def test_batch_timeout_includes_wait_for_target_lock(
    settings: SchemaSettings, target: AsyncConnection, customer: CustomerData
) -> None:
    await target.execute(
        text("SELECT pg_advisory_xact_lock(:lock_key)"), {"lock_key": SCHEMA_LOCK_KEY}
    )
    engine = create_database_engine(settings.target_db, settings.db_timeout_seconds)
    try:
        with pytest.raises(TimeoutError):
            async with engine.begin() as competing:
                await load_batch(
                    competing, (customer,), strategy=LoadStrategy.COPY, transaction_timeout=0.25
                )
    finally:
        await engine.dispose()
    assert (
        await target.scalar(select(Customer.id).where(Customer.legacy_id == customer.legacy_id))
        is None
    )


async def test_duplicate_legacy_id_with_different_data_rolls_back(
    target: AsyncConnection, customer: CustomerData
) -> None:
    conflict = replace(customer, first_name="Другое имя")
    with pytest.raises(LoadConflict, match="внутри партии различаются записи"):
        await load_batch(target, (customer, conflict), strategy=LoadStrategy.UPSERT)
    assert (
        await target.scalar(select(Customer.id).where(Customer.legacy_id == customer.legacy_id))
        is None
    )


async def test_failed_checkpoint_update_rolls_back_data_and_batch_metadata(
    target: AsyncConnection, customer: CustomerData
) -> None:
    options = MigrationOptions()
    run_id = await create_run(
        target, uuid4(), options.model_dump(mode="json"), options.fingerprint()
    )
    first = ExtractedBatch(
        LegacyEntity.CUSTOMERS,
        1,
        customer.legacy_id,
        customer.legacy_id,
        ({"legacy_id": customer.legacy_id},),
        0.0,
    )
    async with target.begin_nested():
        loaded = await load_batch(target, (customer,), strategy=options.strategy)
        await confirm_batch(target, run_id, first, loaded, ())

    earlier = replace(customer, legacy_id=customer.legacy_id - 1)
    overlaps = ExtractedBatch(
        LegacyEntity.CUSTOMERS,
        2,
        earlier.legacy_id,
        earlier.legacy_id,
        ({"legacy_id": earlier.legacy_id},),
        0.0,
    )
    with pytest.raises(CheckpointError):
        async with target.begin_nested():
            loaded = await load_batch(target, (earlier,), strategy=options.strategy)
            await confirm_batch(target, run_id, overlaps, loaded, ())
    assert (
        await target.scalar(select(Customer.id).where(Customer.legacy_id == earlier.legacy_id))
        is None
    )
    batch_numbers = await target.scalars(
        select(MigrationBatch.batch_number).where(MigrationBatch.migration_run_id == run_id)
    )
    assert list(batch_numbers) == [1]
    state = await read_run(target, run_id)
    assert state is not None and state.records_processed == 1
    assert (state.checkpoints[0].last_id, state.checkpoints[0].next_batch) == (
        customer.legacy_id,
        2,
    )
