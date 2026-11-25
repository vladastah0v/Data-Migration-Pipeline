from uuid import uuid4

from sqlalchemy import column, select, table, true
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncConnection

from migration_pipeline.loaders.batch import execute_insert
from migration_pipeline.loaders.records import TableSpec, TargetRow
from migration_pipeline.loaders.upsert import conflict_statement


async def copy_rows(
    connection: AsyncConnection, spec: TableSpec, rows: list[TargetRow], *, update: bool
) -> int:
    name = f"loader_{uuid4().hex}"
    columns = tuple(rows[0])
    preparer = connection.dialect.identifier_preparer
    quote = preparer.quote_identifier
    selected = ", ".join(quote(name) for name in columns)
    await connection.exec_driver_sql(
        f"CREATE TEMP TABLE {quote(name)} ON COMMIT DROP AS "
        f"SELECT {selected} FROM {preparer.format_table(spec.table)} WITH NO DATA"
    )
    driver = (await connection.get_raw_connection()).driver_connection
    await driver.copy_records_to_table(
        name,
        schema_name="pg_temp",
        columns=columns,
        records=(tuple(row[key] for key in columns) for row in rows),
    )
    stage = table(name, *(column(key) for key in columns), schema="pg_temp")
    statement = insert(spec.table).from_select(columns, select(stage).where(true()))
    written = await execute_insert(connection, conflict_statement(statement, spec, update=update))
    await connection.exec_driver_sql(f"DROP TABLE pg_temp.{quote(name)}")
    return written
