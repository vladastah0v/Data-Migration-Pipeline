from itertools import batched

from sqlalchemy.dialects.postgresql import Insert, insert
from sqlalchemy.ext.asyncio import AsyncConnection

from migration_pipeline.loaders.batch import execute_insert, sql_values
from migration_pipeline.loaders.records import MAX_BIND_PARAMETERS, TableSpec, TargetRow


def conflict_statement(statement: Insert, spec: TableSpec, *, update: bool) -> Insert:
    keys = [spec.table.c[name] for name in spec.key]
    if update:
        return statement.on_conflict_do_update(
            index_elements=keys,
            set_={name: statement.excluded[name] for name in spec.mutable},
        )
    return statement.on_conflict_do_nothing(index_elements=keys)


async def upsert_rows(
    connection: AsyncConnection, spec: TableSpec, rows: list[TargetRow], *, update: bool
) -> int:
    written = 0
    for chunk in batched(rows, MAX_BIND_PARAMETERS // len(rows[0])):
        statement = insert(spec.table).values([sql_values(row) for row in chunk])
        written += await execute_insert(
            connection, conflict_statement(statement, spec, update=update)
        )
    return written
