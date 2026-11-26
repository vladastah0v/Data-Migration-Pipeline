from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

from migration_pipeline.verification.checksums import BusinessChecksum, canonical_json


def test_canonical_values_preserve_exact_json_numbers_and_normalize_dates() -> None:
    left = (
        1,
        {"я": [Decimal("123456789012345678901234567890.123400"), True], "a": Decimal("-0.00")},
        datetime(2025, 1, 2, 10, tzinfo=timezone(timedelta(hours=5))),
    )
    right = (
        1,
        {"a": 0, "я": [Decimal("123456789012345678901234567890.1234"), True]},
        datetime(2025, 1, 2, 5, tzinfo=UTC),
    )
    assert canonical_json(left) == canonical_json(right)
    assert canonical_json(Decimal("5.000")) == canonical_json(5)
    assert canonical_json(True) != canonical_json(1)
    assert canonical_json("5") != canonical_json(5)
    assert canonical_json([1, 2]) != canonical_json([2, 1])


def test_checksum_is_stable_across_batch_sizes_and_child_order() -> None:
    together = BusinessChecksum()
    together.add([(-2, "Я"), (-2, "Б"), (0, "First"), (2**63 - 1, "Last")])
    separate = BusinessChecksum()
    separate.add([(-2, "Б"), (-2, "Я")])
    separate.add([(0, "First"), (2**63 - 1, "Last")])
    assert together.hexdigest() == separate.hexdigest()
    assert together.count == separate.count == 4
    separate.add([(2**63 - 1, "Last")])
    assert together.hexdigest() != separate.hexdigest()
