import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from enum import StrEnum
from time import perf_counter

from sqlalchemy import RowMapping, select

from migration_pipeline.database.legacy import LegacySource
from migration_pipeline.models.legacy import (
    LegacyCustomer,
    LegacyOrder,
    LegacyPayment,
    LegacyStatusHistory,
)

BIGINT_MIN = -(2**63)
BIGINT_MAX = 2**63 - 1
DEFAULT_BATCH_SIZE = 1_000
MAX_BATCH_SIZE = 10_000

logger = logging.getLogger(__name__)


class LegacyEntity(StrEnum):
    CUSTOMERS = "customers"
    ORDERS = "orders"
    PAYMENTS = "payments"
    STATUS_HISTORY = "status_history"


SOURCE_TABLES = {
    LegacyEntity.CUSTOMERS: LegacyCustomer.__table__,
    LegacyEntity.ORDERS: LegacyOrder.__table__,
    LegacyEntity.PAYMENTS: LegacyPayment.__table__,
    LegacyEntity.STATUS_HISTORY: LegacyStatusHistory.__table__,
}


@dataclass(frozen=True, slots=True)
class ExtractedBatch:
    entity: LegacyEntity
    number: int
    first_id: int
    last_id: int
    rows: tuple[RowMapping, ...]
    duration_seconds: float

    def log_fields(self) -> dict[str, str | int | float]:
        return {
            "entity_type": self.entity.value,
            "batch_number": self.number,
            "records_extracted": len(self.rows),
            "last_processed_id": self.last_id,
            "extraction_duration_seconds": round(self.duration_seconds, 6),
        }


async def extract_batches(
    source: LegacySource,
    entity: LegacyEntity,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    after_id: int | None = None,
    start_batch_number: int = 1,
) -> AsyncIterator[ExtractedBatch]:
    """Читать партии в порядке первичного ключа с исключением переданного ID."""
    if not 1 <= batch_size <= MAX_BATCH_SIZE:
        raise ValueError(f"Размер партии должен быть от 1 до {MAX_BATCH_SIZE}.")
    if after_id is not None and not BIGINT_MIN <= after_id <= BIGINT_MAX:
        raise ValueError("ID курсора должен помещаться в BIGINT.")
    if start_batch_number < 1:
        raise ValueError("Номер первой партии должен быть положительным.")

    table = SOURCE_TABLES[entity]
    (key,) = table.primary_key.columns
    base_query = select(table).order_by(key).limit(batch_size)
    while True:
        query = base_query.where(key > after_id) if after_id is not None else base_query
        started = perf_counter()
        result = await source.connection.execute(query)
        rows = tuple(result.mappings())
        duration = perf_counter() - started
        if not rows:
            return

        after_id = rows[-1][key]
        batch = ExtractedBatch(entity, start_batch_number, rows[0][key], after_id, rows, duration)
        logger.info("extract_batch", extra=batch.log_fields())
        yield batch

        if len(rows) < batch_size:
            return
        start_batch_number += 1
