"""Итоговый отчёт верификации запуска."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "target_0004"
down_revision = "target_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("migration_runs", sa.Column("verified_at", sa.DateTime(timezone=True)))
    op.add_column("migration_runs", sa.Column("verification_report", JSONB()))
    op.create_check_constraint(
        op.f("ck_migration_runs_verification_report"),
        "migration_runs",
        "(verified_at IS NULL) = (verification_report IS NULL)",
    )


def downgrade() -> None:
    op.drop_constraint(
        op.f("ck_migration_runs_verification_report"), "migration_runs", type_="check"
    )
    op.drop_column("migration_runs", "verification_report")
    op.drop_column("migration_runs", "verified_at")
