from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime

from migration_pipeline.transformers.normalization import (
    ORDER_STATUSES,
    legacy_id,
    normalize_status,
    normalize_timestamp,
)


@dataclass(frozen=True, slots=True)
class StatusEventData:
    legacy_id: int
    order_legacy_id: int
    old_status: str | None
    new_status: str
    changed_at: datetime


def transform_status_event(row: Mapping[str, object]) -> StatusEventData:
    return StatusEventData(
        legacy_id(row.get("id"), "id"),
        legacy_id(row.get("legacy_order_id"), "legacy_order_id"),
        normalize_status(row.get("old_status"), ORDER_STATUSES, "old_status", optional=True),
        normalize_status(row.get("new_status"), ORDER_STATUSES, "new_status"),
        normalize_timestamp(row.get("changed_at_text"), "changed_at_text"),
    )
