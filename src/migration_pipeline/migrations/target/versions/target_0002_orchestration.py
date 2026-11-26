"""Состояние загрузки и конфигурация запуска."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "target_0002"
down_revision = "target_0001"
branch_labels = None
depends_on = None

PREVIOUS_STATES = (
    "'RUNNING', 'SUCCESS', 'SUCCESS_WITH_REJECTIONS', 'FAILED_VERIFICATION', 'FAILED_MIGRATION'"
)


def upgrade() -> None:
    op.add_column("migration_runs", sa.Column("configuration", JSONB(), nullable=True))
    op.add_column(
        "migration_runs",
        sa.Column(
            "records_unchanged", sa.BigInteger(), nullable=False, server_default=sa.text("0")
        ),
    )
    op.create_check_constraint(
        op.f("ck_migration_runs_records_unchanged"), "migration_runs", "records_unchanged >= 0"
    )
    op.drop_constraint(op.f("ck_migration_runs_status"), "migration_runs", type_="check")
    op.create_check_constraint(
        op.f("ck_migration_runs_status"),
        "migration_runs",
        f"status IN ({PREVIOUS_STATES}, 'LOADED')",
    )


def downgrade() -> None:
    op.execute("""
        UPDATE migration_runs
        SET status = 'FAILED_MIGRATION',
            error_message = 'Состояние LOADED недоступно в предыдущей ревизии.'
        WHERE status = 'LOADED'
        """)
    op.drop_constraint(op.f("ck_migration_runs_status"), "migration_runs", type_="check")
    op.create_check_constraint(
        op.f("ck_migration_runs_status"), "migration_runs", f"status IN ({PREVIOUS_STATES})"
    )
    op.drop_constraint(op.f("ck_migration_runs_records_unchanged"), "migration_runs", type_="check")
    op.drop_column("migration_runs", "records_unchanged")
    op.drop_column("migration_runs", "configuration")
