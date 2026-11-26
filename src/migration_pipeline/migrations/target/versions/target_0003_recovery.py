"""Аудит повторов и отметка об отклонении подтверждённой строки."""

import sqlalchemy as sa
from alembic import op

revision = "target_0003"
down_revision = "target_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "migration_errors",
        sa.Column(
            "is_batch_rejection", sa.Boolean(), nullable=False, server_default=sa.text("true")
        ),
    )
    op.execute("""
        UPDATE migration_errors AS error
        SET is_batch_rejection = false
        FROM migration_runs AS run
        WHERE error.migration_run_id = run.id
          AND (error.error_code = 'migration_failed' OR run.configuration->>'policy' = 'strict')
        """)
    op.create_table(
        "migration_retries",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("migration_run_id", sa.UUID(), nullable=False),
        sa.Column("operation", sa.Text(), nullable=False),
        sa.Column("entity_type", sa.Text(), nullable=True),
        sa.Column("batch_number", sa.BigInteger(), nullable=True),
        sa.Column("attempt", sa.BigInteger(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_migration_retries")),
        sa.ForeignKeyConstraint(
            ["migration_run_id"],
            ["migration_runs.id"],
            name=op.f("fk_migration_retries_migration_run_id_migration_runs"),
        ),
    )
    op.create_index(
        op.f("ix_migration_retries_migration_run_id"), "migration_retries", ["migration_run_id"]
    )


def downgrade() -> None:
    op.drop_table("migration_retries")
    op.drop_column("migration_errors", "is_batch_rejection")
