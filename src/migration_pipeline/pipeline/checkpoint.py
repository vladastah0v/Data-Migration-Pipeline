from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncConnection

from migration_pipeline.extractors.base import ExtractedBatch, LegacyEntity
from migration_pipeline.models.migration import MigrationBatch, MigrationCheckpoint


class CheckpointError(RuntimeError):
    """Состояние контрольной точки конфликтует с партией."""


@dataclass(frozen=True, slots=True)
class Checkpoint:
    entity: LegacyEntity
    last_id: int
    processed: int
    next_batch: int


async def read_checkpoints(connection: AsyncConnection, run_id: UUID) -> tuple[Checkpoint, ...]:
    batch_numbers = (
        select(
            MigrationBatch.entity_type.label("entity"),
            func.max(MigrationBatch.batch_number).label("last_batch"),
        )
        .where(MigrationBatch.migration_run_id == run_id, MigrationBatch.status == "COMMITTED")
        .group_by(MigrationBatch.entity_type)
        .subquery()
    )
    result = await connection.execute(
        select(
            MigrationCheckpoint.entity_type,
            MigrationCheckpoint.last_processed_legacy_id,
            MigrationCheckpoint.processed_count,
            (func.coalesce(batch_numbers.c.last_batch, 0) + 1).label("next_batch"),
        )
        .outerjoin(batch_numbers, batch_numbers.c.entity == MigrationCheckpoint.entity_type)
        .where(MigrationCheckpoint.migration_run_id == run_id)
        .order_by(MigrationCheckpoint.entity_type)
    )
    return tuple(
        Checkpoint(LegacyEntity(entity), last_id, processed, next_batch)
        for entity, last_id, processed, next_batch in result
    )


async def advance_checkpoint(
    connection: AsyncConnection, run_id: UUID, batch: ExtractedBatch
) -> None:
    stmt = insert(MigrationCheckpoint).values(
        migration_run_id=run_id,
        entity_type=batch.entity.value,
        last_processed_legacy_id=batch.last_id,
        processed_count=len(batch.rows),
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[MigrationCheckpoint.migration_run_id, MigrationCheckpoint.entity_type],
        set_={
            "last_processed_legacy_id": stmt.excluded.last_processed_legacy_id,
            "processed_count": MigrationCheckpoint.processed_count + stmt.excluded.processed_count,
            "updated_at": func.clock_timestamp(),
        },
        where=MigrationCheckpoint.last_processed_legacy_id < batch.first_id,
    ).returning(MigrationCheckpoint.id)
    if await connection.scalar(stmt) is None:
        raise CheckpointError("Партия пересекается с уже подтверждённым диапазоном.")
