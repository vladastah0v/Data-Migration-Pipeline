import json
from collections.abc import Iterator
from decimal import Decimal
from itertools import islice

import pytest
from pydantic import ValidationError

from migration_pipeline.seeding.data import (
    CUSTOMER_ERRORS,
    HISTORY_ERRORS,
    ORDER_ERRORS,
    PAYMENT_ERRORS,
    CustomerRow,
    HistoryRow,
    LegacyDataGenerator,
    OrderRow,
    PaymentRow,
    SeedOptions,
    SeedProfile,
)


@pytest.mark.parametrize(
    ("profile", "counts"),
    [
        (SeedProfile.SMALL, (1_000, 5_000, 5_000, 10_000)),
        (SeedProfile.MEDIUM, (10_000, 50_000, 50_000, 100_000)),
        (SeedProfile.LARGE, (100_000, 250_000, 150_000, 500_000)),
    ],
)
def test_profiles_declare_the_expected_counts(
    profile: SeedProfile, counts: tuple[int, ...]
) -> None:
    tables = LegacyDataGenerator(SeedOptions(profile=profile)).tables()

    assert tuple(table.count for table in tables) == counts


@pytest.mark.parametrize("bad_rate", ["NaN", "-0.01", "1.01", "unknown", "Infinity"])
def test_rejects_invalid_rates(bad_rate: str) -> None:
    with pytest.raises(ValidationError):
        SeedOptions(invalid_rate=bad_rate)


def test_rows_are_deterministic_and_independent_of_batch_size() -> None:
    def sample(seed: int, size: int) -> list[list[tuple[object, ...]]]:
        options = SeedOptions(seed=seed, batch_size=size, invalid_rate=Decimal("0.02"))
        return [list(islice(table.rows, 30)) for table in LegacyDataGenerator(options).tables()]

    assert sample(91, 1) == sample(91, 347)
    assert sample(91, 1) != sample(23, 1)


def test_exact_error_quotas_and_duplicate_contacts() -> None:
    options = SeedOptions(invalid_rate=Decimal("0.02"), duplicate_contact_rate=Decimal("0.1"))
    generator = LegacyDataGenerator(options)
    errors = (CUSTOMER_ERRORS, ORDER_ERRORS, PAYMENT_ERRORS, HISTORY_ERRORS)
    actual_counts = []
    eligible_emails = []
    for table, markers in zip(generator.tables(), errors, strict=True):
        invalid_count = 0
        for row in table.rows:
            if any(getattr(row, field) == value for field, value in markers):
                invalid_count += 1
            elif isinstance(row, CustomerRow):
                eligible_emails.append(row.email.strip().lower())
        assert invalid_count == table.invalid_count
        actual_counts.append(invalid_count)

    assert actual_counts == [20, 100, 100, 200]
    assert len(eligible_emails) - len(set(eligible_emails)) == generator.duplicate_contacts == 97


def test_full_invalid_rate_leaves_no_contacts_eligible_for_duplication() -> None:
    generator = LegacyDataGenerator(
        SeedOptions(invalid_rate=Decimal(1), duplicate_contact_rate=Decimal(1))
    )

    assert generator.duplicate_contacts == 0
    assert all(table.invalid_count == table.count for table in generator.tables())


def test_clean_rows_have_consistent_relations_and_amounts() -> None:
    customers, orders, payments, history = LegacyDataGenerator(SeedOptions()).tables()

    def money(value: str) -> Decimal:
        return Decimal(value.replace(",", "."))

    assert isinstance(customers.rows, Iterator)
    assert sum(1 for _ in customers.rows) == customers.count
    totals = {}
    order_statuses = {}
    for row in orders.rows:
        assert isinstance(row, OrderRow)
        assert 1 <= row.customer_legacy_id <= customers.count
        amounts = [
            money(line["unit_price"]) * line["quantity"] for line in json.loads(row.items_json)
        ]
        totals[row.legacy_order_id] = money(row.total_amount)
        order_statuses[row.legacy_order_id] = row.order_status.strip().lower()
        assert sum(amounts) == totals[row.legacy_order_id]
    for row in payments.rows:
        assert isinstance(row, PaymentRow)
        assert money(row.amount) == totals[row.legacy_order_id]
    for row in history.rows:
        assert isinstance(row, HistoryRow)
        assert row.legacy_order_id in totals
        if row.id % 2 == 0:
            assert row.new_status.strip().lower() == order_statuses[row.legacy_order_id]
