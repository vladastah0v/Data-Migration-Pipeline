from itertools import batched

from sqlalchemy import Text, cast, insert, literal
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.sql import Executable

from migration_pipeline.loaders.records import MAX_BIND_PARAMETERS, TableSpec, TargetRow


def sql_values(row: TargetRow) -> TargetRow:
    if "value" in row:
        return {**row, "value": cast(literal(row["value"], type_=Text), JSONB)}
    return row


async def execute_insert(connection: AsyncConnection, statement: Executable) -> int:
    result = await connection.execute(statement.execution_options(preserve_rowcount=True))
    return result.rowcount


async def insert_rows(
    connection: AsyncConnection, spec: TableSpec, rows: list[TargetRow], *, single: bool
) -> int:
    chunk_size = 1 if single else MAX_BIND_PARAMETERS // len(rows[0])
    written = 0
    for chunk in batched(rows, chunk_size):
        statement = insert(spec.table).values([sql_values(row) for row in chunk])
        written += await execute_insert(connection, statement)
    return written
