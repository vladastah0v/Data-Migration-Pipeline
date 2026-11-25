from collections.abc import Callable, Collection, Iterable, Mapping
from dataclasses import dataclass, field

from migration_pipeline.extractors.base import BIGINT_MAX, BIGINT_MIN, SOURCE_TABLES, LegacyEntity
from migration_pipeline.transformers.customers import CustomerData, transform_customer
from migration_pipeline.transformers.errors import (
    ErrorCode,
    ErrorPolicy,
    FieldError,
    RecordIssue,
    StrictTransformError,
)
from migration_pipeline.transformers.orders import OrderData, transform_order
from migration_pipeline.transformers.payments import PaymentData, transform_payment
from migration_pipeline.transformers.status_history import StatusEventData, transform_status_event

type TransformedRecord = CustomerData | OrderData | PaymentData | StatusEventData
type RawRecord = Mapping[str, object]

TRANSFORMERS: dict[LegacyEntity, Callable[[RawRecord], TransformedRecord]] = {
    LegacyEntity.CUSTOMERS: transform_customer,
    LegacyEntity.ORDERS: transform_order,
    LegacyEntity.PAYMENTS: transform_payment,
    LegacyEntity.STATUS_HISTORY: transform_status_event,
}

PARENT_KEYS = {
    LegacyEntity.ORDERS: "customer_legacy_id",
    LegacyEntity.PAYMENTS: "legacy_order_id",
    LegacyEntity.STATUS_HISTORY: "legacy_order_id",
}


@dataclass(frozen=True, slots=True)
class BatchReferences:
    parent_ids: Collection[int] = frozenset()
    payment_owners: Mapping[str, int] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class TransformedBatch:
    records: tuple[TransformedRecord, ...]
    issues: tuple[RecordIssue, ...]


def transform_batch(
    entity: LegacyEntity,
    rows: Iterable[RawRecord],
    *,
    batch_number: int,
    references: BatchReferences,
    policy: ErrorPolicy,
) -> TransformedBatch:
    records = []
    issues = []
    (primary_key,) = SOURCE_TABLES[entity].primary_key.columns
    parent_key = PARENT_KEYS.get(entity)
    payment_owners = dict(references.payment_owners)
    for row in rows:
        try:
            record = TRANSFORMERS[entity](row)
            if parent_key is not None and row[parent_key] not in references.parent_ids:
                raise FieldError(ErrorCode.MISSING_PARENT, parent_key)
            if isinstance(record, PaymentData):
                owner = payment_owners.get(record.reference)
                if owner is not None and owner != record.legacy_id:
                    raise FieldError(ErrorCode.PAYMENT_REFERENCE_CONFLICT, "payment_reference")
                payment_owners[record.reference] = record.legacy_id
        except FieldError as exc:
            raw_id = row.get(primary_key.name)
            issue = RecordIssue(
                entity,
                batch_number,
                raw_id if type(raw_id) is int and BIGINT_MIN <= raw_id <= BIGINT_MAX else None,
                exc.code,
                exc.field,
                policy is ErrorPolicy.QUARANTINE,
            )
            if policy is ErrorPolicy.STRICT:
                raise StrictTransformError(issue) from None
            issues.append(issue)
        else:
            records.append(record)
    return TransformedBatch(tuple(records), tuple(issues))
