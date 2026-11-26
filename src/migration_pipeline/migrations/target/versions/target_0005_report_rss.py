"""Пиковый RSS процесса загрузки."""

import sqlalchemy as sa
from alembic import op

revision = "target_0005"
down_revision = "target_0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("migration_runs", sa.Column("peak_rss_bytes", sa.BigInteger()))
    op.create_check_constraint(
        op.f("ck_migration_runs_peak_rss_bytes"), "migration_runs", "peak_rss_bytes > 0"
    )


def downgrade() -> None:
    op.drop_constraint(op.f("ck_migration_runs_peak_rss_bytes"), "migration_runs", type_="check")
    op.drop_column("migration_runs", "peak_rss_bytes")
