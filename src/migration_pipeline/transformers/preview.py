from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import BigInteger, Column, MetaData, Table, Text, insert, select, text
from sqlalchemy.ext.asyncio import AsyncConnection

from migration_pipeline.config import Settings
from migration_pipeline.database.connections import POSTGRES_MAJOR, create_database_engine
from migration_pipeline.database.legacy import open_legacy_source
from migration_pipeline.extractors.base import ExtractedBatch, LegacyEntity, extract_batches
from migration_pipeline.transformers.base import PARENT_KEYS, BatchReferences, transform_batch
from migration_pipeline.transformers.errors import ErrorPolicy

scratch = MetaData(schema="pg_temp")
customers = Table(
    "transform_customers", scratch, Column("legacy_id", BigInteger), Column("email", Text)
)
orders = Table("transform_orders", scratch, Column("legacy_id", BigInteger))
payments = Table(
    "transform_payments", scratch, Column("reference", Text), Column("legacy_id", BigInteger)
)
PARENT_TABLES = {
    LegacyEntity.ORDERS: customers,
    LegacyEntity.PAYMENTS: orders,
    LegacyEntity.STATUS_HISTORY: orders,
}

type ReportEntry = dict[str, str | int | bool | None]
type ReportSink = Callable[[ReportEntry], None]


class PreviewError(RuntimeError):
    """Диагностическое преобразование не может подготовить рабочие таблицы."""


@dataclass(frozen=True, slots=True)
class EntitySummary:
    entity: LegacyEntity
    accepted: int
    rejected: int


@dataclass(frozen=True, slots=True)
class PreviewSummary:
    snapshot_id: UUID
    entities: tuple[EntitySummary, ...]
    error_counts: dict[str, int]
    duplicate_email_groups: int
    duplicate_email_members: int


async def preview_transform(
    settings: Settings,
    *,
    batch_size: int,
    policy: ErrorPolicy,
    snapshot_id: UUID | None,
    quarantine: ReportSink,
    duplicates: ReportSink,
    progress: Callable[[LegacyEntity, int, int], None],
) -> PreviewSummary:
    engine = create_database_engine(settings.target_db, settings.db_timeout_seconds)
    try:
        async with (
            open_legacy_source(settings, expected_snapshot_id=snapshot_id) as source,
            engine.begin() as target,
        ):
            await _prepare_scratch(target)
            summaries = []
            error_counts: Counter[str] = Counter()
            for entity in LegacyEntity:
                accepted = rejected = 0
                async for batch in extract_batches(source, entity, batch_size=batch_size):
                    result = transform_batch(
                        entity,
                        batch.rows,
                        batch_number=batch.number,
                        references=await _references(target, batch),
                        policy=policy,
                    )
                    accepted += len(result.records)
                    rejected += len(result.issues)
                    for issue in result.issues:
                        error_counts[issue.code.value] += 1
                        if issue.is_quarantined:
                            quarantine(
                                {"snapshot_id": str(source.snapshot.id), **issue.log_fields()}
                            )
                    if result.records:
                        match entity:
                            case LegacyEntity.CUSTOMERS:
                                await target.execute(
                                    insert(customers),
                                    [
                                        {"legacy_id": row.legacy_id, "email": row.email}
                                        for row in result.records
                                    ],
                                )
                            case LegacyEntity.ORDERS:
                                await target.execute(
                                    insert(orders),
                                    [{"legacy_id": row.legacy_id} for row in result.records],
                                )
                            case LegacyEntity.PAYMENTS:
                                await target.execute(
                                    insert(payments),
                                    [
                                        {"reference": row.reference, "legacy_id": row.legacy_id}
                                        for row in result.records
                                    ],
                                )
                    progress(entity, accepted, rejected)
                summaries.append(EntitySummary(entity, accepted, rejected))
            groups, members = await _report_duplicate_emails(target, source.snapshot.id, duplicates)
            return PreviewSummary(
                source.snapshot.id, tuple(summaries), dict(error_counts), groups, members
            )
    finally:
        await engine.dispose()


async def _prepare_scratch(connection: AsyncConnection) -> None:
    if connection.dialect.server_version_info[0] != POSTGRES_MAJOR:
        raise PreviewError(f"Для проверки требуется PostgreSQL {POSTGRES_MAJOR} на стороне target.")
    await connection.exec_driver_sql(
        "CREATE TEMP TABLE transform_customers "
        "(legacy_id bigint PRIMARY KEY, email text) ON COMMIT DROP"
    )
    await connection.exec_driver_sql(
        "CREATE TEMP TABLE transform_orders (legacy_id bigint PRIMARY KEY) ON COMMIT DROP"
    )
    await connection.exec_driver_sql(
        "CREATE TEMP TABLE transform_payments "
        "(reference text PRIMARY KEY, legacy_id bigint NOT NULL) ON COMMIT DROP"
    )


async def _references(connection: AsyncConnection, batch: ExtractedBatch) -> BatchReferences:
    parent = PARENT_TABLES.get(batch.entity)
    if parent is None:
        return BatchReferences()
    field = PARENT_KEYS[batch.entity]
    ids = {row[field] for row in batch.rows}
    accepted = await connection.scalars(
        select(parent.c.legacy_id).where(parent.c.legacy_id.in_(ids))
    )
    parent_ids = frozenset(accepted)
    if batch.entity is not LegacyEntity.PAYMENTS:
        return BatchReferences(parent_ids)

    references = {
        value.strip()
        for row in batch.rows
        if isinstance(value := row["payment_reference"], str) and value.strip()
    }
    existing = await connection.execute(
        select(payments.c.reference, payments.c.legacy_id).where(
            payments.c.reference.in_(references)
        )
    )
    return BatchReferences(parent_ids, {row.reference: row.legacy_id for row in existing})


async def _report_duplicate_emails(
    connection: AsyncConnection, snapshot_id: UUID, write: ReportSink
) -> tuple[int, int]:
    groups = members = 0
    previous_group = None
    query = text("""
        WITH duplicates AS (
            SELECT email, min(legacy_id) AS first_legacy_id
            FROM pg_temp.transform_customers
            WHERE email IS NOT NULL
            GROUP BY email
            HAVING count(*) > 1
        )
        SELECT d.first_legacy_id, c.legacy_id
        FROM duplicates d
        JOIN pg_temp.transform_customers c USING (email)
        ORDER BY d.first_legacy_id, c.legacy_id
        """)
    async with connection.stream(query) as rows:
        async for row in rows:
            if previous_group != row.first_legacy_id:
                groups += 1
                previous_group = row.first_legacy_id
            members += 1
            write(
                {
                    "snapshot_id": str(snapshot_id),
                    "first_legacy_id": row.first_legacy_id,
                    "legacy_record_id": row.legacy_id,
                }
            )
    return groups, members
