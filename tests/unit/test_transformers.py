from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from migration_pipeline.extractors.base import LegacyEntity
from migration_pipeline.transformers.base import BatchReferences, transform_batch
from migration_pipeline.transformers.customers import transform_customer
from migration_pipeline.transformers.errors import (
    ErrorCode,
    ErrorPolicy,
    FieldError,
    StrictTransformError,
)
from migration_pipeline.transformers.normalization import (
    normalize_email,
    normalize_money,
    normalize_phone,
    normalize_timestamp,
)
from migration_pipeline.transformers.orders import transform_order
from migration_pipeline.transformers.payments import transform_payment
from migration_pipeline.transformers.status_history import transform_status_event

STAMP = datetime(2021, 3, 2, 15, tzinfo=UTC)
CUSTOMER = {
    "legacy_id": 13,
    "full_name": " Jane Doe ",
    "email": " A.B+X@EXAMPLE.TEST ",
    "phone": "202-555-0100",
    "birth_date_text": "02/03/1980",
    "country": "United States",
    "city": " Demo City ",
    "address_text": "   ",
    "customer_status": " SUSPENDED ",
    "extra_data": '{"segment": "business", "priority": 1.25}',
    "created_at": STAMP,
    "updated_at": STAMP,
}
ORDER = {
    "legacy_order_id": 21,
    "customer_legacy_id": 13,
    "order_number": " O-21 ",
    "order_status": "New",
    "order_date_text": "02.03.2021 15:00:00",
    "currency": "usd",
    "total_amount": "20,04",
    "created_at": STAMP,
    "items_json": (
        '[{"product_name": "Товар", "quantity": "2", "unit_price": 10.02, "line_total": 20.04}]'
    ),
}
PAYMENT = {
    "legacy_payment_id": 31,
    "legacy_order_id": 21,
    "payment_reference": " P-31 ",
    "payment_method": " card ",
    "payment_status": "Success",
    "amount": "20,04",
    "payment_date_text": "2021-03-02T18:00:00+03:00",
}


@pytest.mark.parametrize(
    ("raw", "country", "expected"),
    [
        ("202-555-0100", "US", "+12025550100"),
        ("001 (202) 555-0100", None, "+12025550100"),
        ("8 (912) 555 00 00", "RU", "+79125550000"),
        (" ", None, None),
    ],
)
def test_normalizes_unambiguous_phone(raw: str, country: str | None, expected: str | None) -> None:
    assert normalize_phone(raw, country) == expected


@pytest.mark.parametrize("raw", ["202-555-0100", "abc", "+1 202 555 0100 x123"])
def test_rejects_ambiguous_phone(raw: str) -> None:
    with pytest.raises(FieldError) as result:
        normalize_phone(raw, None)
    assert result.value.code is ErrorCode.INVALID_PHONE


@pytest.mark.parametrize(
    "raw", ["a..b@example.test", "a@-example.test", "x@a@b.test", "@example.test"]
)
def test_rejects_malformed_email(raw: str) -> None:
    with pytest.raises(FieldError) as result:
        normalize_email(raw)
    assert result.value.code is ErrorCode.INVALID_EMAIL


@pytest.mark.parametrize("value", ["20.04", "20,04", Decimal("20.040"), " +20.04 "])
def test_money_is_decimal_without_rounding(value: str | Decimal) -> None:
    assert normalize_money(value, "amount") == Decimal("20.04")


@pytest.mark.parametrize(
    "value", [True, -1, 3.15, "1,000.00", "1.001", "1000000000000", Decimal("NaN")]
)
def test_rejects_inexact_or_invalid_money(value: object) -> None:
    with pytest.raises(FieldError) as result:
        normalize_money(value, "amount")
    assert result.value.code is ErrorCode.INVALID_MONEY


def test_normalized_customer_preserves_existing_values() -> None:
    customer = transform_customer(CUSTOMER)
    assert (customer.first_name, customer.last_name) == ("Jane", "Doe")
    assert (customer.email, customer.status) == ("a.b+x@example.test", "blocked")
    assert customer.birth_date == date(1980, 3, 2)
    assert customer.address is not None
    assert (customer.address.city, customer.address.country, customer.address.address_line) == (
        "Demo City",
        "US",
        None,
    )
    assert [(item.contact_type, item.contact_value) for item in customer.contacts] == [
        ("email", "a.b+x@example.test"),
        ("phone", "+12025550100"),
    ]
    assert customer.attributes["priority"] == Decimal("1.25")
    for name in ("Jane", "Mary Anne Doe"):
        ambiguous = transform_customer({**CUSTOMER, "full_name": name})
        assert (ambiguous.first_name, ambiguous.last_name) == (name, None)


def test_rejects_future_birth_date_and_broken_attribute_json() -> None:
    with pytest.raises(FieldError) as result:
        transform_customer({**CUSTOMER, "birth_date_text": "2022-01-01"})
    assert result.value.code is ErrorCode.INVALID_DATE
    for value in ("[]", "{", '{"bad": NaN}', {" a": 1, "a": 2}):
        with pytest.raises(FieldError) as result:
            transform_customer({**CUSTOMER, "extra_data": value})
        assert result.value.code is ErrorCode.INVALID_JSON


def test_transforms_order_payment_and_status_timestamps() -> None:
    order = transform_order(ORDER)
    assert (order.status, order.currency, order.ordered_at) == ("pending", "USD", STAMP)
    assert order.total_amount == order.items[0].line_total == Decimal("20.04")
    assert order.items[0].unit_price == Decimal("10.02")
    payment = transform_payment(PAYMENT)
    assert (payment.reference, payment.paid_at, payment.status) == ("P-31", STAMP, "completed")
    assert payment.amount == order.total_amount
    assert (
        transform_payment(
            {**PAYMENT, "payment_status": "pending", "payment_date_text": None}
        ).paid_at
        is None
    )
    history = transform_status_event(
        {
            "id": 19,
            "legacy_order_id": 21,
            "old_status": "New",
            "new_status": "canceled",
            "changed_at_text": "2021-03-02T23:00:00+08:00",
        }
    )
    assert (history.old_status, history.new_status, history.changed_at) == (
        "pending",
        "cancelled",
        STAMP,
    )
    assert normalize_timestamp("02/03/2021", "date") == datetime(2021, 3, 2, tzinfo=UTC)


@pytest.mark.parametrize(
    ("overrides", "code"),
    [
        ({"total_amount": "20.03"}, ErrorCode.TOTAL_MISMATCH),
        ({"currency": "₽"}, ErrorCode.INVALID_CURRENCY),
        ({"items_json": {"product_name": "test"}}, ErrorCode.INVALID_JSON),
        ({"items_json": '[{"product_name": "a", "quantity": true}]'}, ErrorCode.INVALID_QUANTITY),
        (
            {"items_json": '[{"product_name": "a", "quantity": 2, "unit_price": 999999999999.99}]'},
            ErrorCode.INVALID_MONEY,
        ),
        (
            {
                "items_json": (
                    '[{"product_name": "a", "quantity": 2, "unit_price": 10.02, "line_total": 21}]'
                )
            },
            ErrorCode.TOTAL_MISMATCH,
        ),
    ],
)
def test_rejects_invalid_order(overrides: dict[str, object], code: ErrorCode) -> None:
    with pytest.raises(FieldError) as result:
        transform_order({**ORDER, **overrides})
    assert result.value.code is code


@pytest.mark.parametrize("policy", [ErrorPolicy.SKIP, ErrorPolicy.QUARANTINE])
def test_batch_checks_parent_and_reference_ownership(policy: ErrorPolicy) -> None:
    payment_rows = [
        PAYMENT,
        {**PAYMENT, "legacy_payment_id": 32, "payment_reference": "P-31"},
        {**PAYMENT, "legacy_payment_id": 33, "payment_reference": "P-33"},
        {**PAYMENT, "legacy_payment_id": 34, "legacy_order_id": 99},
    ]
    result = transform_batch(
        LegacyEntity.PAYMENTS,
        payment_rows,
        batch_number=2,
        references=BatchReferences({21}, {"P-33": 15}),
        policy=policy,
    )
    assert [payment.legacy_id for payment in result.records] == [31]
    assert [issue.code for issue in result.issues] == [
        ErrorCode.PAYMENT_REFERENCE_CONFLICT,
        ErrorCode.PAYMENT_REFERENCE_CONFLICT,
        ErrorCode.MISSING_PARENT,
    ]
    assert all(
        issue.is_quarantined == (policy is ErrorPolicy.QUARANTINE) for issue in result.issues
    )
    assert "P-31" not in str(result.issues[0].log_fields())


def test_strict_policy_stops_without_exposing_raw_data() -> None:
    with pytest.raises(StrictTransformError) as exception:
        transform_batch(
            LegacyEntity.CUSTOMERS,
            [{**CUSTOMER, "email": "private-value"}],
            batch_number=1,
            references=BatchReferences(),
            policy=ErrorPolicy.STRICT,
        )
    assert exception.value.issue.code is ErrorCode.INVALID_EMAIL
    assert "private-value" not in str(exception.value)
