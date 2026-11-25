from collections.abc import AsyncIterator, Iterable, Sequence
from dataclasses import dataclass
from itertools import batched
from uuid import UUID

from sqlalchemy import MetaData, Table, select
from sqlalchemy.ext.asyncio import AsyncConnection

from migration_pipeline.loaders.json_values import encode_json
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
from migration_pipeline.transformers.base import TransformedRecord
from migration_pipeline.transformers.customers import CustomerData
from migration_pipeline.transformers.orders import OrderData
from migration_pipeline.transformers.payments import PaymentData
from migration_pipeline.transformers.status_history import StatusEventData

type TargetRow = dict[str, object]
MAX_BIND_PARAMETERS = 30_000
metadata = MetaData(schema="public")


class LoadError(RuntimeError):
    """Ошибка записи партии нормализованных данных."""


class LoadConflict(LoadError):
    """Записи с одним ключом содержат конфликтующие данные."""


@dataclass(frozen=True, slots=True)
class TableSpec:
    table: Table
    key: tuple[str, ...]
    mutable: tuple[str, ...] = ()


CUSTOMERS = TableSpec(
    Customer.__table__.to_metadata(metadata, schema="public"),
    ("legacy_id",),
    ("first_name", "last_name", "email", "birth_date", "status", "updated_at"),
)
CONTACTS = TableSpec(
    CustomerContact.__table__.to_metadata(metadata, schema="public"),
    ("customer_id", "contact_type", "contact_value"),
)
ADDRESSES = TableSpec(
    Address.__table__.to_metadata(metadata, schema="public"),
    ("customer_id", "city", "country", "address_line"),
)
ATTRIBUTES = TableSpec(
    CustomerAttribute.__table__.to_metadata(metadata, schema="public"),
    ("customer_id", "name"),
    ("value",),
)
ORDERS = TableSpec(
    Order.__table__.to_metadata(metadata, schema="public"),
    ("legacy_order_id",),
    ("order_number", "status", "total_amount"),
)
ITEMS = TableSpec(
    OrderItem.__table__.to_metadata(metadata, schema="public"),
    ("order_id", "line_number"),
    ("product_name", "quantity", "unit_price", "line_total"),
)
PAYMENTS = TableSpec(
    Payment.__table__.to_metadata(metadata, schema="public"),
    ("legacy_payment_id",),
    ("payment_method", "status", "amount", "paid_at"),
)
HISTORY = TableSpec(
    OrderStatusHistory.__table__.to_metadata(metadata, schema="public"),
    ("legacy_history_id",),
)


async def groups_for_batch(
    connection: AsyncConnection, records: Sequence[TransformedRecord]
) -> AsyncIterator[tuple[TableSpec, list[TargetRow]]]:
    first = records[0]
    if isinstance(first, CustomerData):
        customers = [record for record in records if isinstance(record, CustomerData)]
        async for group in _customer_groups(connection, customers):
            yield group
    elif isinstance(first, OrderData):
        orders = [record for record in records if isinstance(record, OrderData)]
        async for group in _order_groups(connection, orders):
            yield group
    elif isinstance(first, PaymentData):
        payments = [record for record in records if isinstance(record, PaymentData)]
        order_ids = await _resolve_ids(
            connection, ORDERS, (row.order_legacy_id for row in payments)
        )
        yield (
            PAYMENTS,
            [
                {
                    "legacy_payment_id": row.legacy_id,
                    "order_id": order_ids[row.order_legacy_id],
                    "payment_reference": row.reference,
                    "payment_method": row.method,
                    "status": row.status,
                    "amount": row.amount,
                    "paid_at": row.paid_at,
                }
                for row in payments
            ],
        )
    else:
        events = [record for record in records if isinstance(record, StatusEventData)]
        order_ids = await _resolve_ids(connection, ORDERS, (row.order_legacy_id for row in events))
        yield (
            HISTORY,
            [
                {
                    "legacy_history_id": row.legacy_id,
                    "order_id": order_ids[row.order_legacy_id],
                    "old_status": row.old_status,
                    "new_status": row.new_status,
                    "changed_at": row.changed_at,
                }
                for row in events
            ],
        )


async def _customer_groups(
    connection: AsyncConnection, records: Sequence[CustomerData]
) -> AsyncIterator[tuple[TableSpec, list[TargetRow]]]:
    yield (
        CUSTOMERS,
        [
            {
                "legacy_id": row.legacy_id,
                "first_name": row.first_name,
                "last_name": row.last_name,
                "email": row.email,
                "birth_date": row.birth_date,
                "status": row.status,
                "created_at": row.created_at,
                "updated_at": row.updated_at,
            }
            for row in records
        ],
    )
    ids = await _resolve_ids(connection, CUSTOMERS, (row.legacy_id for row in records))
    yield (
        CONTACTS,
        [
            {
                "customer_id": ids[row.legacy_id],
                "contact_type": contact.contact_type,
                "contact_value": contact.contact_value,
                "is_primary": contact.is_primary,
            }
            for row in records
            for contact in row.contacts
        ],
    )
    yield (
        ADDRESSES,
        [
            {
                "customer_id": ids[row.legacy_id],
                "city": row.address.city,
                "country": row.address.country,
                "address_line": row.address.address_line,
            }
            for row in records
            if row.address is not None
        ],
    )
    yield (
        ATTRIBUTES,
        [
            {"customer_id": ids[row.legacy_id], "name": name, "value": encode_json(value)}
            for row in records
            for name, value in row.attributes.items()
        ],
    )


async def _order_groups(
    connection: AsyncConnection, records: Sequence[OrderData]
) -> AsyncIterator[tuple[TableSpec, list[TargetRow]]]:
    customer_ids = await _resolve_ids(
        connection, CUSTOMERS, (row.customer_legacy_id for row in records)
    )
    yield (
        ORDERS,
        [
            {
                "legacy_order_id": row.legacy_id,
                "customer_id": customer_ids[row.customer_legacy_id],
                "order_number": row.order_number,
                "status": row.status,
                "currency": row.currency,
                "total_amount": row.total_amount,
                "ordered_at": row.ordered_at,
                "created_at": row.created_at,
            }
            for row in records
        ],
    )
    ids = await _resolve_ids(connection, ORDERS, (row.legacy_id for row in records))
    yield (
        ITEMS,
        [
            {
                "order_id": ids[row.legacy_id],
                "line_number": item.line_number,
                "product_name": item.product_name,
                "quantity": item.quantity,
                "unit_price": item.unit_price,
                "line_total": item.line_total,
            }
            for row in records
            for item in row.items
        ],
    )


async def _resolve_ids(
    connection: AsyncConnection, spec: TableSpec, ids: Iterable[int]
) -> dict[int, UUID]:
    keys = set(ids)
    legacy_column = spec.table.c[spec.key[0]]
    resolved = {}
    for chunk in batched(sorted(keys), MAX_BIND_PARAMETERS):
        rows = await connection.execute(
            select(legacy_column, spec.table.c.id)
            .where(legacy_column.in_(chunk))
            .with_for_update(read=True, key_share=True)
        )
        resolved.update((legacy, target) for legacy, target in rows)
    if len(resolved) != len(keys):
        raise LoadError(
            f"{spec.table.name} — отсутствуют целевые записи для {len(keys) - len(resolved)} ID."
        )
    return resolved
