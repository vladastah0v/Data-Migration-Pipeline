import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from decimal import Decimal

from sqlalchemy import Column, Select, Text, cast, func, select
from sqlalchemy.ext.asyncio import AsyncConnection

from migration_pipeline.extractors.base import SOURCE_TABLES, LegacyEntity
from migration_pipeline.loaders.records import (
    ADDRESSES,
    ATTRIBUTES,
    CONTACTS,
    CUSTOMERS,
    HISTORY,
    ITEMS,
    ORDERS,
    PAYMENTS,
    TableSpec,
)
from migration_pipeline.transformers.base import TransformedRecord
from migration_pipeline.transformers.customers import CustomerData
from migration_pipeline.transformers.orders import OrderData
from migration_pipeline.transformers.payments import PaymentData
from migration_pipeline.verification.checksums import BusinessRow

c = CUSTOMERS.table.c
o = ORDERS.table.c
p = PAYMENTS.table.c
h = HISTORY.table.c
co = CONTACTS.table.c
ad = ADDRESSES.table.c
a = ATTRIBUTES.table.c
i = ITEMS.table.c


@dataclass(frozen=True, slots=True)
class Projection:
    table: TableSpec
    query: Select
    legacy_key: Column[int]

    async def read(self, connection: AsyncConnection, ids: Sequence[int]) -> list[BusinessRow]:
        if not ids:
            return []
        rows = await connection.execute(self.query.where(self.legacy_key.in_(ids)))
        if self.table is ATTRIBUTES:
            return [
                (legacy_id, name, json.loads(value, parse_float=Decimal))
                for legacy_id, name, value in rows
            ]
        return [tuple(row) for row in rows]


PROJECTIONS = {
    LegacyEntity.CUSTOMERS: (
        Projection(
            CUSTOMERS,
            select(
                c.legacy_id,
                c.first_name,
                c.last_name,
                c.email,
                c.birth_date,
                c.status,
                c.created_at,
                c.updated_at,
            ),
            c.legacy_id,
        ),
        Projection(
            CONTACTS,
            select(c.legacy_id, co.contact_type, co.contact_value, co.is_primary).join_from(
                CONTACTS.table, CUSTOMERS.table, co.customer_id == c.id
            ),
            c.legacy_id,
        ),
        Projection(
            ADDRESSES,
            select(c.legacy_id, ad.city, ad.country, ad.address_line).join_from(
                ADDRESSES.table, CUSTOMERS.table, ad.customer_id == c.id
            ),
            c.legacy_id,
        ),
        Projection(
            ATTRIBUTES,
            select(c.legacy_id, a.name, cast(a.value, Text)).join_from(
                ATTRIBUTES.table, CUSTOMERS.table, a.customer_id == c.id
            ),
            c.legacy_id,
        ),
    ),
    LegacyEntity.ORDERS: (
        Projection(
            ORDERS,
            select(
                o.legacy_order_id,
                c.legacy_id,
                o.order_number,
                o.status,
                o.currency,
                o.total_amount,
                o.ordered_at,
                o.created_at,
            ).join_from(ORDERS.table, CUSTOMERS.table, o.customer_id == c.id, isouter=True),
            o.legacy_order_id,
        ),
        Projection(
            ITEMS,
            select(
                o.legacy_order_id,
                i.line_number,
                i.product_name,
                i.quantity,
                i.unit_price,
                i.line_total,
            ).join_from(ITEMS.table, ORDERS.table, i.order_id == o.id),
            o.legacy_order_id,
        ),
    ),
    LegacyEntity.PAYMENTS: (
        Projection(
            PAYMENTS,
            select(
                p.legacy_payment_id,
                o.legacy_order_id,
                p.payment_reference,
                p.payment_method,
                p.status,
                p.amount,
                p.paid_at,
                o.currency,
            ).join_from(PAYMENTS.table, ORDERS.table, p.order_id == o.id, isouter=True),
            p.legacy_payment_id,
        ),
    ),
    LegacyEntity.STATUS_HISTORY: (
        Projection(
            HISTORY,
            select(
                h.legacy_history_id, o.legacy_order_id, h.old_status, h.new_status, h.changed_at
            ).join_from(HISTORY.table, ORDERS.table, h.order_id == o.id, isouter=True),
            h.legacy_history_id,
        ),
    ),
}

FOREIGN_KEYS = (
    (CONTACTS, co.customer_id, CUSTOMERS),
    (ADDRESSES, ad.customer_id, CUSTOMERS),
    (ATTRIBUTES, a.customer_id, CUSTOMERS),
    (ORDERS, o.customer_id, CUSTOMERS),
    (ITEMS, i.order_id, ORDERS),
    (PAYMENTS, p.order_id, ORDERS),
    (HISTORY, h.order_id, ORDERS),
)


async def foreign_key_violations(connection: AsyncConnection) -> dict[str, int]:
    counts = {}
    for child, reference, parent in FOREIGN_KEYS:
        query = (
            select(func.count())
            .select_from(child.table.outerjoin(parent.table, reference == parent.table.c.id))
            .where(parent.table.c.id.is_(None))
        )
        counts[child.table.name] = await connection.scalar(query)
    return counts


async def expected_rows(
    connection: AsyncConnection,
    entity: LegacyEntity,
    records: Sequence[TransformedRecord],
) -> dict[str, list[BusinessRow]]:
    rows = {projection.table.table.name: [] for projection in PROJECTIONS[entity]}
    order_ids = {row.order_legacy_id for row in records if isinstance(row, PaymentData)}
    currencies = await _payment_currencies(connection, order_ids)
    for row in records:
        match row:
            case CustomerData():
                rows["customers"].append(
                    (
                        row.legacy_id,
                        row.first_name,
                        row.last_name,
                        row.email,
                        row.birth_date,
                        row.status,
                        row.created_at,
                        row.updated_at,
                    )
                )
                rows["customer_contacts"].extend(
                    (row.legacy_id, contact.contact_type, contact.contact_value, contact.is_primary)
                    for contact in row.contacts
                )
                if row.address is not None:
                    rows["addresses"].append(
                        (
                            row.legacy_id,
                            row.address.city,
                            row.address.country,
                            row.address.address_line,
                        )
                    )
                rows["customer_attributes"].extend(
                    (row.legacy_id, name, value) for name, value in row.attributes.items()
                )
            case OrderData():
                rows["orders"].append(
                    (
                        row.legacy_id,
                        row.customer_legacy_id,
                        row.order_number,
                        row.status,
                        row.currency,
                        row.total_amount,
                        row.ordered_at,
                        row.created_at,
                    )
                )
                rows["order_items"].extend(
                    (
                        row.legacy_id,
                        item.line_number,
                        item.product_name,
                        item.quantity,
                        item.unit_price,
                        item.line_total,
                    )
                    for item in row.items
                )
            case PaymentData():
                rows["payments"].append(
                    (
                        row.legacy_id,
                        row.order_legacy_id,
                        row.reference,
                        row.method,
                        row.status,
                        row.amount,
                        row.paid_at,
                        currencies.get(row.order_legacy_id),
                    )
                )
            case _:
                rows["order_status_history"].append(
                    (
                        row.legacy_id,
                        row.order_legacy_id,
                        row.old_status,
                        row.new_status,
                        row.changed_at,
                    )
                )
    return rows


async def _payment_currencies(connection: AsyncConnection, ids: Iterable[int]) -> dict[int, str]:
    if not ids:
        return {}
    order = SOURCE_TABLES[LegacyEntity.ORDERS].c
    query = select(order.legacy_order_id, order.currency).where(order.legacy_order_id.in_(ids))
    return {
        key: currency.strip().upper()
        for key, currency in await connection.execute(query)
        if currency is not None
    }
