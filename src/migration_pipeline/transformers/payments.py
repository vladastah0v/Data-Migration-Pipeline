from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from migration_pipeline.transformers.normalization import (
    PAYMENT_STATUSES,
    legacy_id,
    normalize_money,
    normalize_status,
    normalize_timestamp,
    optional_text,
    required_text,
)


@dataclass(frozen=True, slots=True)
class PaymentData:
    legacy_id: int
    order_legacy_id: int
    reference: str
    method: str | None
    status: str
    amount: Decimal
    paid_at: datetime | None


def transform_payment(row: Mapping[str, object]) -> PaymentData:
    payment_id = legacy_id(row.get("legacy_payment_id"), "legacy_payment_id")
    order_id = legacy_id(row.get("legacy_order_id"), "legacy_order_id")
    reference = required_text(row.get("payment_reference"), "payment_reference")
    method = optional_text(row.get("payment_method"), "payment_method")
    status = normalize_status(row.get("payment_status"), PAYMENT_STATUSES, "payment_status")
    date_text = optional_text(row.get("payment_date_text"), "payment_date_text")
    paid_at = (
        normalize_timestamp(date_text, "payment_date_text")
        if date_text is not None or status in {"completed", "refunded"}
        else None
    )
    return PaymentData(
        payment_id,
        order_id,
        reference,
        method,
        status,
        normalize_money(row.get("amount"), "amount"),
        paid_at,
    )
