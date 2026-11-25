"""Целевая схема CRM и состояние миграции."""

import sqlalchemy as sa
from alembic import context, op
from sqlalchemy.dialects import postgresql

revision = "target_0001"
down_revision = None
branch_labels = None
depends_on = None

ORDER_STATUSES = (
    "'pending', 'confirmed', 'processing', 'paid', 'shipped', "
    "'delivered', 'completed', 'cancelled', 'refunded'"
)


def upgrade() -> None:
    op.create_table(
        "customers",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("legacy_id", sa.BigInteger(), nullable=False),
        sa.Column("first_name", sa.Text(), nullable=False),
        sa.Column("last_name", sa.Text(), nullable=True),
        sa.Column("email", sa.Text(), nullable=True),
        sa.Column("birth_date", sa.Date(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("btrim(first_name) <> ''", name=op.f("ck_customers_first_name")),
        sa.CheckConstraint(
            "status IN ('active', 'inactive', 'blocked')", name=op.f("ck_customers_status")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_customers")),
        sa.UniqueConstraint("legacy_id", name=op.f("uq_customers_legacy_id")),
    )
    op.create_index(op.f("ix_customers_email"), "customers", ["email"], unique=False)
    op.create_table(
        "addresses",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("customer_id", sa.UUID(), nullable=False),
        sa.Column("city", sa.Text(), nullable=True),
        sa.Column("country", sa.Text(), nullable=True),
        sa.Column("address_line", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "num_nonnulls(city, country, address_line) > 0", name=op.f("ck_addresses_has_parts")
        ),
        sa.ForeignKeyConstraint(
            ["customer_id"],
            ["customers.id"],
            name=op.f("fk_addresses_customer_id_customers"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_addresses")),
        sa.UniqueConstraint(
            "customer_id",
            "city",
            "country",
            "address_line",
            name=op.f("uq_addresses_customer_id"),
            postgresql_nulls_not_distinct=True,
        ),
    )
    op.create_table(
        "customer_attributes",
        sa.Column("customer_id", sa.UUID(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("value", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.CheckConstraint("btrim(name) <> ''", name=op.f("ck_customer_attributes_name")),
        sa.ForeignKeyConstraint(
            ["customer_id"],
            ["customers.id"],
            name=op.f("fk_customer_attributes_customer_id_customers"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("customer_id", "name", name=op.f("pk_customer_attributes")),
    )
    op.create_table(
        "customer_contacts",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("customer_id", sa.UUID(), nullable=False),
        sa.Column("contact_type", sa.Text(), nullable=False),
        sa.Column("contact_value", sa.Text(), nullable=False),
        sa.Column("is_primary", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.CheckConstraint("btrim(contact_value) <> ''", name=op.f("ck_customer_contacts_value")),
        sa.CheckConstraint(
            "contact_type IN ('email', 'phone')", name=op.f("ck_customer_contacts_type")
        ),
        sa.ForeignKeyConstraint(
            ["customer_id"],
            ["customers.id"],
            name=op.f("fk_customer_contacts_customer_id_customers"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_customer_contacts")),
        sa.UniqueConstraint(
            "customer_id",
            "contact_type",
            "contact_value",
            name=op.f("uq_customer_contacts_customer_id"),
        ),
    )
    op.create_index(
        "uq_customer_contacts_primary_type",
        "customer_contacts",
        ["customer_id", "contact_type"],
        unique=True,
        postgresql_where=sa.text("is_primary"),
    )
    op.create_table(
        "orders",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("legacy_order_id", sa.BigInteger(), nullable=False),
        sa.Column("customer_id", sa.UUID(), nullable=False),
        sa.Column("order_number", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("currency", sa.String(length=3), nullable=False),
        sa.Column("total_amount", sa.Numeric(precision=14, scale=2), nullable=False),
        sa.Column("ordered_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("currency ~ '^[A-Z]{3}$'", name=op.f("ck_orders_currency")),
        sa.CheckConstraint(
            f"status IN ({ORDER_STATUSES})",
            name=op.f("ck_orders_status"),
        ),
        sa.CheckConstraint(
            "total_amount >= 0 AND total_amount <> 'NaN'::numeric", name=op.f("ck_orders_total")
        ),
        sa.ForeignKeyConstraint(
            ["customer_id"], ["customers.id"], name=op.f("fk_orders_customer_id_customers")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_orders")),
        sa.UniqueConstraint("legacy_order_id", name=op.f("uq_orders_legacy_order_id")),
    )
    op.create_index(op.f("ix_orders_customer_id"), "orders", ["customer_id"], unique=False)
    op.create_table(
        "order_items",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("order_id", sa.UUID(), nullable=False),
        sa.Column("line_number", sa.Integer(), nullable=False),
        sa.Column("product_name", sa.Text(), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("unit_price", sa.Numeric(precision=14, scale=2), nullable=False),
        sa.Column("line_total", sa.Numeric(precision=14, scale=2), nullable=False),
        sa.CheckConstraint(
            "unit_price >= 0 AND unit_price <> 'NaN'::numeric",
            name=op.f("ck_order_items_unit_price"),
        ),
        sa.CheckConstraint("line_number > 0", name=op.f("ck_order_items_line_number")),
        sa.CheckConstraint(
            "line_total = quantity * unit_price", name=op.f("ck_order_items_line_total")
        ),
        sa.CheckConstraint("quantity > 0", name=op.f("ck_order_items_quantity")),
        sa.ForeignKeyConstraint(
            ["order_id"],
            ["orders.id"],
            name=op.f("fk_order_items_order_id_orders"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_order_items")),
        sa.UniqueConstraint("order_id", "line_number", name=op.f("uq_order_items_order_id")),
    )
    op.create_table(
        "order_status_history",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("legacy_history_id", sa.BigInteger(), nullable=False),
        sa.Column("order_id", sa.UUID(), nullable=False),
        sa.Column("old_status", sa.Text(), nullable=True),
        sa.Column("new_status", sa.Text(), nullable=False),
        sa.Column("changed_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            f"new_status IN ({ORDER_STATUSES})",
            name=op.f("ck_order_status_history_new_status"),
        ),
        sa.CheckConstraint(
            f"old_status IN ({ORDER_STATUSES})",
            name=op.f("ck_order_status_history_old_status"),
        ),
        sa.ForeignKeyConstraint(
            ["order_id"],
            ["orders.id"],
            name=op.f("fk_order_status_history_order_id_orders"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_order_status_history")),
        sa.UniqueConstraint(
            "legacy_history_id", name=op.f("uq_order_status_history_legacy_history_id")
        ),
    )
    op.create_index(
        "ix_order_status_history_order_time",
        "order_status_history",
        ["order_id", "changed_at", "legacy_history_id"],
        unique=False,
    )
    op.create_table(
        "payments",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("legacy_payment_id", sa.BigInteger(), nullable=False),
        sa.Column("order_id", sa.UUID(), nullable=False),
        sa.Column("payment_reference", sa.Text(), nullable=False),
        sa.Column("payment_method", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("amount", sa.Numeric(precision=14, scale=2), nullable=False),
        sa.Column("paid_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "amount >= 0 AND amount <> 'NaN'::numeric", name=op.f("ck_payments_amount")
        ),
        sa.CheckConstraint("btrim(payment_reference) <> ''", name=op.f("ck_payments_reference")),
        sa.CheckConstraint(
            "status IN ('pending', 'completed', 'failed', 'cancelled', 'refunded')",
            name=op.f("ck_payments_status"),
        ),
        sa.ForeignKeyConstraint(
            ["order_id"], ["orders.id"], name=op.f("fk_payments_order_id_orders")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_payments")),
        sa.UniqueConstraint("legacy_payment_id", name=op.f("uq_payments_legacy_payment_id")),
        sa.UniqueConstraint("payment_reference", name=op.f("uq_payments_payment_reference")),
    )
    op.create_index(op.f("ix_payments_order_id"), "payments", ["order_id"], unique=False)
    op.create_table(
        "migration_runs",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.Text(), server_default=sa.text("'RUNNING'"), nullable=False),
        sa.Column("configuration_hash", sa.Text(), nullable=False),
        sa.Column("source_snapshot_id", sa.UUID(), nullable=False),
        sa.Column(
            "records_processed", sa.BigInteger(), server_default=sa.text("0"), nullable=False
        ),
        sa.Column("records_inserted", sa.BigInteger(), server_default=sa.text("0"), nullable=False),
        sa.Column("records_updated", sa.BigInteger(), server_default=sa.text("0"), nullable=False),
        sa.Column("records_rejected", sa.BigInteger(), server_default=sa.text("0"), nullable=False),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "(status = 'RUNNING') = (finished_at IS NULL)", name=op.f("ck_migration_runs_finished")
        ),
        sa.CheckConstraint(
            "configuration_hash ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_migration_runs_configuration_hash"),
        ),
        sa.CheckConstraint(
            "status IN ('RUNNING', 'SUCCESS', 'SUCCESS_WITH_REJECTIONS', "
            "'FAILED_VERIFICATION', 'FAILED_MIGRATION')",
            name=op.f("ck_migration_runs_status"),
        ),
        sa.CheckConstraint("finished_at >= started_at", name=op.f("ck_migration_runs_duration")),
        sa.CheckConstraint(
            "records_processed >= 0 AND records_inserted >= 0 "
            "AND records_updated >= 0 AND records_rejected >= 0",
            name=op.f("ck_migration_runs_record_counts"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_migration_runs")),
    )
    op.create_index(
        "ix_migration_runs_snapshot_started",
        "migration_runs",
        ["source_snapshot_id", "started_at"],
        unique=False,
    )
    op.create_table(
        "migration_batches",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("migration_run_id", sa.UUID(), nullable=False),
        sa.Column("entity_type", sa.Text(), nullable=False),
        sa.Column("batch_number", sa.BigInteger(), nullable=False),
        sa.Column("first_legacy_id", sa.BigInteger(), nullable=False),
        sa.Column("last_legacy_id", sa.BigInteger(), nullable=False),
        sa.Column("records_count", sa.BigInteger(), nullable=False),
        sa.Column("status", sa.Text(), server_default=sa.text("'STARTED'"), nullable=False),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("committed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "(status = 'COMMITTED') = (committed_at IS NOT NULL)",
            name=op.f("ck_migration_batches_committed"),
        ),
        sa.CheckConstraint(
            "status IN ('STARTED', 'COMMITTED', 'FAILED')", name=op.f("ck_migration_batches_status")
        ),
        sa.CheckConstraint("batch_number > 0", name=op.f("ck_migration_batches_number")),
        sa.CheckConstraint(
            "committed_at >= started_at", name=op.f("ck_migration_batches_duration")
        ),
        sa.CheckConstraint(
            "first_legacy_id <= last_legacy_id", name=op.f("ck_migration_batches_legacy_id_range")
        ),
        sa.CheckConstraint("records_count > 0", name=op.f("ck_migration_batches_records_count")),
        sa.ForeignKeyConstraint(
            ["migration_run_id"],
            ["migration_runs.id"],
            name=op.f("fk_migration_batches_migration_run_id_migration_runs"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_migration_batches")),
        sa.UniqueConstraint(
            "migration_run_id",
            "entity_type",
            "batch_number",
            name=op.f("uq_migration_batches_migration_run_id"),
        ),
    )
    op.create_table(
        "migration_checkpoints",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("migration_run_id", sa.UUID(), nullable=False),
        sa.Column("entity_type", sa.Text(), nullable=False),
        sa.Column("last_processed_legacy_id", sa.BigInteger(), nullable=False),
        sa.Column("processed_count", sa.BigInteger(), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "processed_count >= 0", name=op.f("ck_migration_checkpoints_processed_count")
        ),
        sa.ForeignKeyConstraint(
            ["migration_run_id"],
            ["migration_runs.id"],
            name=op.f("fk_migration_checkpoints_migration_run_id_migration_runs"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_migration_checkpoints")),
        sa.UniqueConstraint(
            "migration_run_id",
            "entity_type",
            name=op.f("uq_migration_checkpoints_migration_run_id"),
        ),
    )
    op.create_table(
        "migration_errors",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("migration_run_id", sa.UUID(), nullable=False),
        sa.Column("entity_type", sa.Text(), nullable=False),
        sa.Column("legacy_record_id", sa.BigInteger(), nullable=True),
        sa.Column("error_code", sa.Text(), nullable=False),
        sa.Column("error_message", sa.Text(), nullable=False),
        sa.Column("is_quarantined", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["migration_run_id"],
            ["migration_runs.id"],
            name=op.f("fk_migration_errors_migration_run_id_migration_runs"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_migration_errors")),
        sa.UniqueConstraint(
            "migration_run_id",
            "entity_type",
            "legacy_record_id",
            "error_code",
            name=op.f("uq_migration_errors_migration_run_id"),
        ),
    )
    role = context.config.attributes["app_role"]
    quoted_role = op.get_context().dialect.identifier_preparer.quote_identifier(role)
    op.execute(
        sa.DDL(
            "REVOKE INSERT, UPDATE, DELETE ON TABLE public.alembic_version FROM %(app_role)s",
            context={"app_role": quoted_role},
        ),
    )


def downgrade() -> None:
    op.drop_table("migration_errors")
    op.drop_table("migration_checkpoints")
    op.drop_table("migration_batches")
    op.drop_index("ix_migration_runs_snapshot_started", table_name="migration_runs")
    op.drop_table("migration_runs")
    op.drop_index(op.f("ix_payments_order_id"), table_name="payments")
    op.drop_table("payments")
    op.drop_index("ix_order_status_history_order_time", table_name="order_status_history")
    op.drop_table("order_status_history")
    op.drop_table("order_items")
    op.drop_index(op.f("ix_orders_customer_id"), table_name="orders")
    op.drop_table("orders")
    op.drop_index(
        "uq_customer_contacts_primary_type",
        table_name="customer_contacts",
        postgresql_where=sa.text("is_primary"),
    )
    op.drop_table("customer_contacts")
    op.drop_table("customer_attributes")
    op.drop_table("addresses")
    op.drop_index(op.f("ix_customers_email"), table_name="customers")
    op.drop_table("customers")
