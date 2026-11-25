from datetime import datetime
from uuid import UUID

from sqlalchemy import UUID as UuidType
from sqlalchemy import BigInteger, CheckConstraint, DateTime, ForeignKey, SmallInteger, Text, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from migration_pipeline.models.base import schema_metadata


class LegacyBase(DeclarativeBase):
    metadata = schema_metadata()


class LegacyCustomer(LegacyBase):
    __tablename__ = "legacy_customers"

    legacy_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    full_name: Mapped[str | None] = mapped_column(Text)
    email: Mapped[str | None] = mapped_column(Text)
    phone: Mapped[str | None] = mapped_column(Text)
    address_text: Mapped[str | None] = mapped_column(Text)
    city: Mapped[str | None] = mapped_column(Text)
    country: Mapped[str | None] = mapped_column(Text)
    birth_date_text: Mapped[str | None] = mapped_column(Text)
    customer_status: Mapped[str | None] = mapped_column(Text)
    extra_data: Mapped[object | None] = mapped_column(JSONB(none_as_null=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class LegacyOrder(LegacyBase):
    __tablename__ = "legacy_orders"

    legacy_order_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    customer_legacy_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("legacy_customers.legacy_id"), index=True
    )
    order_number: Mapped[str | None] = mapped_column(Text)
    order_date_text: Mapped[str | None] = mapped_column(Text)
    order_status: Mapped[str | None] = mapped_column(Text)
    currency: Mapped[str | None] = mapped_column(Text)
    total_amount: Mapped[str | None] = mapped_column(Text)
    items_json: Mapped[object | None] = mapped_column(JSONB(none_as_null=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class LegacyPayment(LegacyBase):
    __tablename__ = "legacy_payments"

    legacy_payment_id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=False
    )
    legacy_order_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("legacy_orders.legacy_order_id"), index=True
    )
    payment_reference: Mapped[str | None] = mapped_column(Text)
    payment_method: Mapped[str | None] = mapped_column(Text)
    payment_status: Mapped[str | None] = mapped_column(Text)
    amount: Mapped[str | None] = mapped_column(Text)
    payment_date_text: Mapped[str | None] = mapped_column(Text)


class LegacyStatusHistory(LegacyBase):
    __tablename__ = "legacy_status_history"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    legacy_order_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("legacy_orders.legacy_order_id"), index=True
    )
    old_status: Mapped[str | None] = mapped_column(Text)
    new_status: Mapped[str | None] = mapped_column(Text)
    changed_at_text: Mapped[str | None] = mapped_column(Text)


class LegacySourceSnapshot(LegacyBase):
    __tablename__ = "legacy_source_snapshot"
    __table_args__ = (
        CheckConstraint("singleton = 1", name="singleton"),
        CheckConstraint("(snapshot_id IS NULL) = (sealed_at IS NULL)", name="seal"),
    )

    singleton: Mapped[int] = mapped_column(
        SmallInteger, primary_key=True, autoincrement=False, server_default=text("1")
    )
    snapshot_id: Mapped[UUID | None] = mapped_column(UuidType)
    sealed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
