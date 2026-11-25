from io import StringIO

import pytest
from alembic import command

from migration_pipeline.config import DatabaseSide
from migration_pipeline.database.schema import revision_config


@pytest.mark.usefixtures("example_env")
@pytest.mark.parametrize(
    ("side", "expected", "other_revision"),
    [
        ("legacy", "CREATE TRIGGER require_open_snapshot", "target_0001"),
        ("target", "REVOKE INSERT, UPDATE, DELETE", "legacy_0001"),
    ],
)
def test_initial_revision_generates_sql_for_its_own_schema(
    side: DatabaseSide, expected: str, other_revision: str
) -> None:
    output = StringIO()
    config = revision_config(side)
    config.output_buffer = output

    command.upgrade(config, "head", sql=True)

    sql = output.getvalue()
    assert expected in sql
    assert f"{side}_0001" in sql
    assert other_revision not in sql
