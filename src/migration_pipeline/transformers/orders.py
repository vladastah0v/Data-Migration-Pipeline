from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from migration_pipeline.transformers.errors import ErrorCode, FieldError
from migration_pipeline.transformers.normalization import (
    MAX_MONEY,
    ORDER_STATUSES,
    legacy_id,
    normalize_money,
    normalize_status,
    normalize_timestamp,
    optional_text,
    read_json,
    required_text,
)

INTEGER_MAX = 2**31 - 1


@dataclass(frozen=True, slots=True)
class OrderItemData:
    line_number: int
    product_name: str
    quantity: int
    unit_price: Decimal
    line_total: Decimal


@dataclass(frozen=True, slots=True)
class OrderData:
    legacy_id: int
    customer_legacy_id: int
    order_number: str | None
    status: str
    currency: str
    total_amount: Decimal
    ordered_at: datetime
    created_at: datetime
    items: tuple[OrderItemData, ...]


def transform_order(row: Mapping[str, object]) -> OrderData:
    order_id = legacy_id(row.get("legacy_order_id"), "legacy_order_id")
    customer_id = legacy_id(row.get("customer_legacy_id"), "customer_legacy_id")
    currency = required_text(row.get("currency"), "currency")
    if len(currency) != 3 or not currency.isascii() or not currency.isalpha():
        raise FieldError(ErrorCode.INVALID_CURRENCY, "currency")

    total = normalize_money(row.get("total_amount"), "total_amount")
    raw_items = read_json(row.get("items_json"), "items_json")
    if not isinstance(raw_items, list) or not raw_items:
        raise FieldError(ErrorCode.INVALID_JSON, "items_json")
    items = tuple(_item(index, value) for index, value in enumerate(raw_items, 1))
    if sum(item.line_total for item in items) != total:
        raise FieldError(ErrorCode.TOTAL_MISMATCH, "total_amount")

    return OrderData(
        order_id,
        customer_id,
        optional_text(row.get("order_number"), "order_number"),
        normalize_status(row.get("order_status"), ORDER_STATUSES, "order_status"),
        currency.upper(),
        total,
        normalize_timestamp(row.get("order_date_text"), "order_date_text"),
        normalize_timestamp(row.get("created_at"), "created_at"),
        items,
    )


def _item(number: int, value: object) -> OrderItemData:
    field = f"items_json[{number - 1}]"
    if not isinstance(value, dict):
        raise FieldError(ErrorCode.INVALID_JSON, field)
    product = required_text(value.get("product_name"), f"{field}.product_name")
    quantity = value.get("quantity")
    if isinstance(quantity, str):
        digits = quantity.strip()
        if len(digits) <= 10 and digits.isascii() and digits.isdecimal():
            quantity = int(digits)
    if type(quantity) is not int or not 1 <= quantity <= INTEGER_MAX:
        raise FieldError(ErrorCode.INVALID_QUANTITY, f"{field}.quantity")

    price = normalize_money(value.get("unit_price"), f"{field}.unit_price")
    total = quantity * price
    if total > MAX_MONEY:
        raise FieldError(ErrorCode.INVALID_MONEY, f"{field}.line_total")
    declared = value.get("line_total")
    if declared is not None and normalize_money(declared, f"{field}.line_total") != total:
        raise FieldError(ErrorCode.TOTAL_MISMATCH, f"{field}.line_total")
    return OrderItemData(number, product, quantity, price, total)
