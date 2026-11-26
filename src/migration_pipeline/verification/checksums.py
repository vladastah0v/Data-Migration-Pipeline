import json
from collections.abc import Iterable
from datetime import UTC, date, datetime
from decimal import Decimal
from hashlib import sha256

CANONICAL_VERSION = 1

type BusinessRow = tuple[object, ...]


def canonical_json(value: object) -> str:
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("Контрольная сумма требует конечного числа.")
        if not value:
            return "0"
        formatted = format(value, "f")
        return formatted.rstrip("0").rstrip(".") if "." in formatted else formatted
    if isinstance(value, datetime):
        value = value.astimezone(UTC).isoformat(timespec="microseconds")
    elif isinstance(value, date):
        value = value.isoformat()
    if isinstance(value, dict):
        return (
            "{"
            + ",".join(
                f"{canonical_json(key)}:{canonical_json(item)}"
                for key, item in sorted(value.items())
            )
            + "}"
        )
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(map(canonical_json, value)) + "]"
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


class BusinessChecksum:
    def __init__(self) -> None:
        self.count = 0
        self._checksum = sha256()

    def add(self, rows: Iterable[BusinessRow]) -> None:
        # The first column is the legacy ID. Sorting within an ID also orders child rows.
        ordered = sorted((row[0], canonical_json(row)) for row in rows)
        for _, encoded in ordered:
            self._checksum.update(encoded.encode("utf-8") + b"\n")
        self.count += len(ordered)

    def hexdigest(self) -> str:
        return self._checksum.hexdigest()
