from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import UUID as UuidType
from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from migration_pipeline.models.base import schema_metadata

ORDER_STATUSES = (
    "'pending', 'confirmed', 'processing', 'paid', 'shipped', "
    "'delivered', 'completed', 'cancelled', 'refunded'"
)


class TargetBase(DeclarativeBase):
    metadata = schema_metadata()


class Customer(TargetBase):
    __tablename__ = "customers"
    __table_args__ = (
        CheckConstraint("btrim(first_name) <> ''", name="first_name"),
        CheckConstraint("status IN ('active', 'inactive', 'blocked')", name="status"),
    )

    id: Mapped[UUID] = mapped_column(
        UuidType, primary_key=True, server_default=text("gen_random_uuid()")
    )
    legacy_id: Mapped[int] = mapped_column(BigInteger, unique=True)
    first_name: Mapped[str] = mapped_column(Text)
    last_name: Mapped[str | None] = mapped_column(Text)
    email: Mapped[str | None] = mapped_column(Text, index=True)
    birth_date: Mapped[date | None] = mapped_column(Date)
    status: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class CustomerContact(TargetBase):
    __tablename__ = "customer_contacts"
    __table_args__ = (
        UniqueConstraint("customer_id", "contact_type", "contact_value"),
        Index(
            "uq_customer_contacts_primary_type",
            "customer_id",
            "contact_type",
            unique=True,
            postgresql_where=text("is_primary"),
        ),
        CheckConstraint("contact_type IN ('email', 'phone')", name="type"),
        CheckConstraint("btrim(contact_value) <> ''", name="value"),
    )

    id: Mapped[UUID] = mapped_column(
        UuidType, primary_key=True, server_default=text("gen_random_uuid()")
    )
    customer_id: Mapped[UUID] = mapped_column(ForeignKey("customers.id", ondelete="CASCADE"))
    contact_type: Mapped[str] = mapped_column(Text)
    contact_value: Mapped[str] = mapped_column(Text)
    is_primary: Mapped[bool] = mapped_column(server_default=text("false"))


class Address(TargetBase):
    __tablename__ = "addresses"
    __table_args__ = (
        UniqueConstraint(
            "customer_id", "city", "country", "address_line", postgresql_nulls_not_distinct=True
        ),
        CheckConstraint("num_nonnulls(city, country, address_line) > 0", name="has_parts"),
    )

    id: Mapped[UUID] = mapped_column(
        UuidType, primary_key=True, server_default=text("gen_random_uuid()")
    )
    customer_id: Mapped[UUID] = mapped_column(ForeignKey("customers.id", ondelete="CASCADE"))
    city: Mapped[str | None] = mapped_column(Text)
    country: Mapped[str | None] = mapped_column(Text)
    address_line: Mapped[str | None] = mapped_column(Text)


class CustomerAttribute(TargetBase):
    __tablename__ = "customer_attributes"
    __table_args__ = (CheckConstraint("btrim(name) <> ''", name="name"),)

    customer_id: Mapped[UUID] = mapped_column(
        ForeignKey("customers.id", ondelete="CASCADE"), primary_key=True
    )
    name: Mapped[str] = mapped_column(Text, primary_key=True)
    value: Mapped[object] = mapped_column(JSONB)


class Order(TargetBase):
    __tablename__ = "orders"
    __table_args__ = (
        CheckConstraint(f"status IN ({ORDER_STATUSES})", name="status"),
        CheckConstraint("currency ~ '^[A-Z]{3}$'", name="currency"),
        CheckConstraint("total_amount >= 0 AND total_amount <> 'NaN'::numeric", name="total"),
    )

    id: Mapped[UUID] = mapped_column(
        UuidType, primary_key=True, server_default=text("gen_random_uuid()")
    )
    legacy_order_id: Mapped[int] = mapped_column(BigInteger, unique=True)
    customer_id: Mapped[UUID] = mapped_column(ForeignKey("customers.id"), index=True)
    order_number: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text)
    currency: Mapped[str] = mapped_column(String(3))
    total_amount: Mapped[Decimal] = mapped_column(Numeric(14, 2))
    ordered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class OrderItem(TargetBase):
    __tablename__ = "order_items"
    __table_args__ = (
        UniqueConstraint("order_id", "line_number"),
        CheckConstraint("line_number > 0", name="line_number"),
        CheckConstraint("quantity > 0", name="quantity"),
        CheckConstraint("unit_price >= 0 AND unit_price <> 'NaN'::numeric", name="unit_price"),
        CheckConstraint("line_total = quantity * unit_price", name="line_total"),
    )

    id: Mapped[UUID] = mapped_column(
        UuidType, primary_key=True, server_default=text("gen_random_uuid()")
    )
    order_id: Mapped[UUID] = mapped_column(ForeignKey("orders.id", ondelete="CASCADE"))
    line_number: Mapped[int] = mapped_column(Integer)
    product_name: Mapped[str] = mapped_column(Text)
    quantity: Mapped[int] = mapped_column(Integer)
    unit_price: Mapped[Decimal] = mapped_column(Numeric(14, 2))
    line_total: Mapped[Decimal] = mapped_column(Numeric(14, 2))


class Payment(TargetBase):
    __tablename__ = "payments"
    __table_args__ = (
        CheckConstraint("btrim(payment_reference) <> ''", name="reference"),
        CheckConstraint(
            "status IN ('pending', 'completed', 'failed', 'cancelled', 'refunded')", name="status"
        ),
        CheckConstraint("amount >= 0 AND amount <> 'NaN'::numeric", name="amount"),
    )

    id: Mapped[UUID] = mapped_column(
        UuidType, primary_key=True, server_default=text("gen_random_uuid()")
    )
    legacy_payment_id: Mapped[int] = mapped_column(BigInteger, unique=True)
    order_id: Mapped[UUID] = mapped_column(ForeignKey("orders.id"), index=True)
    payment_reference: Mapped[str] = mapped_column(Text, unique=True)
    payment_method: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text)
    amount: Mapped[Decimal] = mapped_column(Numeric(14, 2))
    paid_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class OrderStatusHistory(TargetBase):
    __tablename__ = "order_status_history"
    __table_args__ = (
        Index("ix_order_status_history_order_time", "order_id", "changed_at", "legacy_history_id"),
        CheckConstraint(f"old_status IN ({ORDER_STATUSES})", name="old_status"),
        CheckConstraint(f"new_status IN ({ORDER_STATUSES})", name="new_status"),
    )

    id: Mapped[UUID] = mapped_column(
        UuidType, primary_key=True, server_default=text("gen_random_uuid()")
    )
    legacy_history_id: Mapped[int] = mapped_column(BigInteger, unique=True)
    order_id: Mapped[UUID] = mapped_column(ForeignKey("orders.id", ondelete="CASCADE"))
    old_status: Mapped[str | None] = mapped_column(Text)
    new_status: Mapped[str] = mapped_column(Text)
    changed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
