"""Исходная схема CRM и фиксация неизменяемого снимка."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "legacy_0001"
down_revision = None
branch_labels = None
depends_on = None

SOURCE_TABLES = (
    "legacy_customers",
    "legacy_orders",
    "legacy_payments",
    "legacy_status_history",
)


def upgrade() -> None:
    op.create_table(
        "legacy_customers",
        sa.Column("legacy_id", sa.BigInteger(), autoincrement=False, nullable=False),
        sa.Column("full_name", sa.Text(), nullable=True),
        sa.Column("email", sa.Text(), nullable=True),
        sa.Column("phone", sa.Text(), nullable=True),
        sa.Column("address_text", sa.Text(), nullable=True),
        sa.Column("city", sa.Text(), nullable=True),
        sa.Column("country", sa.Text(), nullable=True),
        sa.Column("birth_date_text", sa.Text(), nullable=True),
        sa.Column("customer_status", sa.Text(), nullable=True),
        sa.Column(
            "extra_data", postgresql.JSONB(none_as_null=True, astext_type=sa.Text()), nullable=True
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("legacy_id", name=op.f("pk_legacy_customers")),
    )
    op.create_table(
        "legacy_source_snapshot",
        sa.Column(
            "singleton",
            sa.SmallInteger(),
            server_default=sa.text("1"),
            autoincrement=False,
            nullable=False,
        ),
        sa.Column("snapshot_id", sa.UUID(), nullable=True),
        sa.Column("sealed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "(snapshot_id IS NULL) = (sealed_at IS NULL)",
            name=op.f("ck_legacy_source_snapshot_seal"),
        ),
        sa.CheckConstraint("singleton = 1", name=op.f("ck_legacy_source_snapshot_singleton")),
        sa.PrimaryKeyConstraint("singleton", name=op.f("pk_legacy_source_snapshot")),
    )
    op.create_table(
        "legacy_orders",
        sa.Column("legacy_order_id", sa.BigInteger(), autoincrement=False, nullable=False),
        sa.Column("customer_legacy_id", sa.BigInteger(), nullable=False),
        sa.Column("order_number", sa.Text(), nullable=True),
        sa.Column("order_date_text", sa.Text(), nullable=True),
        sa.Column("order_status", sa.Text(), nullable=True),
        sa.Column("currency", sa.Text(), nullable=True),
        sa.Column("total_amount", sa.Text(), nullable=True),
        sa.Column(
            "items_json", postgresql.JSONB(none_as_null=True, astext_type=sa.Text()), nullable=True
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["customer_legacy_id"],
            ["legacy_customers.legacy_id"],
            name=op.f("fk_legacy_orders_customer_legacy_id_legacy_customers"),
        ),
        sa.PrimaryKeyConstraint("legacy_order_id", name=op.f("pk_legacy_orders")),
    )
    op.create_index(
        op.f("ix_legacy_orders_customer_legacy_id"),
        "legacy_orders",
        ["customer_legacy_id"],
        unique=False,
    )
    op.create_table(
        "legacy_payments",
        sa.Column("legacy_payment_id", sa.BigInteger(), autoincrement=False, nullable=False),
        sa.Column("legacy_order_id", sa.BigInteger(), nullable=False),
        sa.Column("payment_reference", sa.Text(), nullable=True),
        sa.Column("payment_method", sa.Text(), nullable=True),
        sa.Column("payment_status", sa.Text(), nullable=True),
        sa.Column("amount", sa.Text(), nullable=True),
        sa.Column("payment_date_text", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["legacy_order_id"],
            ["legacy_orders.legacy_order_id"],
            name=op.f("fk_legacy_payments_legacy_order_id_legacy_orders"),
        ),
        sa.PrimaryKeyConstraint("legacy_payment_id", name=op.f("pk_legacy_payments")),
    )
    op.create_index(
        op.f("ix_legacy_payments_legacy_order_id"),
        "legacy_payments",
        ["legacy_order_id"],
        unique=False,
    )
    op.create_table(
        "legacy_status_history",
        sa.Column("id", sa.BigInteger(), autoincrement=False, nullable=False),
        sa.Column("legacy_order_id", sa.BigInteger(), nullable=False),
        sa.Column("old_status", sa.Text(), nullable=True),
        sa.Column("new_status", sa.Text(), nullable=True),
        sa.Column("changed_at_text", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["legacy_order_id"],
            ["legacy_orders.legacy_order_id"],
            name=op.f("fk_legacy_status_history_legacy_order_id_legacy_orders"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_legacy_status_history")),
    )
    op.create_index(
        op.f("ix_legacy_status_history_legacy_order_id"),
        "legacy_status_history",
        ["legacy_order_id"],
        unique=False,
    )
    op.execute("INSERT INTO public.legacy_source_snapshot DEFAULT VALUES")
    _protect_source_snapshot()


def _protect_source_snapshot() -> None:
    op.execute("""
        CREATE FUNCTION public.require_open_legacy_snapshot() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog AS $$
        DECLARE
            snapshot_open boolean;
        BEGIN
            SELECT snapshot_id IS NULL INTO snapshot_open
            FROM public.legacy_source_snapshot
            WHERE singleton = 1
            FOR SHARE;

            IF snapshot_open IS DISTINCT FROM true THEN
                RAISE EXCEPTION 'Legacy source snapshot is sealed or missing'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NULL;
        END;
        $$
        """)
    for table in SOURCE_TABLES:
        op.execute(f"""
            CREATE TRIGGER require_open_snapshot
            BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.{table}
            FOR EACH STATEMENT EXECUTE FUNCTION public.require_open_legacy_snapshot()
            """)

    op.execute("""
        CREATE FUNCTION public.guard_legacy_snapshot_seal() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog AS $$
        BEGIN
            IF TG_OP = 'UPDATE' THEN
                IF OLD.snapshot_id IS NULL
                    AND NEW.snapshot_id IS NOT NULL
                    AND NEW.sealed_at IS NOT NULL THEN
                    RETURN NEW;
                END IF;
            END IF;
            RAISE EXCEPTION 'Legacy snapshot can only be sealed once'
                USING ERRCODE = '55000';
        END;
        $$
        """)
    op.execute("""
        CREATE TRIGGER guard_snapshot_seal
        BEFORE UPDATE OR DELETE ON public.legacy_source_snapshot
        FOR EACH ROW EXECUTE FUNCTION public.guard_legacy_snapshot_seal()
        """)
    op.execute("""
        CREATE TRIGGER guard_snapshot_truncate
        BEFORE TRUNCATE ON public.legacy_source_snapshot
        FOR EACH STATEMENT EXECUTE FUNCTION public.guard_legacy_snapshot_seal()
        """)


def downgrade() -> None:
    op.drop_index(
        op.f("ix_legacy_status_history_legacy_order_id"), table_name="legacy_status_history"
    )
    op.drop_table("legacy_status_history")
    op.drop_index(op.f("ix_legacy_payments_legacy_order_id"), table_name="legacy_payments")
    op.drop_table("legacy_payments")
    op.drop_index(op.f("ix_legacy_orders_customer_legacy_id"), table_name="legacy_orders")
    op.drop_table("legacy_orders")
    op.drop_table("legacy_source_snapshot")
    op.drop_table("legacy_customers")
    op.execute("DROP FUNCTION public.guard_legacy_snapshot_seal()")
    op.execute("DROP FUNCTION public.require_open_legacy_snapshot()")
