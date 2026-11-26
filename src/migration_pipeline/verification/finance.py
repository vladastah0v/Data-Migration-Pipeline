from collections import Counter
from decimal import Decimal

from migration_pipeline.verification.checksums import BusinessRow


class FinancialChecks:
    def __init__(self) -> None:
        self.source: dict[tuple[str, str], Decimal] = {}
        self.target: dict[tuple[str, str], Decimal] = {}
        self.mismatches = dict.fromkeys(("orders", "payments", "order_items"), 0)
        self.invalid_line_totals = 0
        self.invalid_order_totals = 0

    def add(
        self, expected: dict[str, list[BusinessRow]], actual: dict[str, list[BusinessRow]]
    ) -> None:
        for table in self.mismatches.keys() & expected.keys():
            source_rows = _money_rows(table, expected[table])
            target_rows = _money_rows(table, actual[table])
            differing = (source_rows - target_rows) | (target_rows - source_rows)
            key_size = 2 if table == "order_items" else 1
            self.mismatches[table] += len({row[:key_size] for row in differing})
        _add_totals(self.source, expected)
        self.invalid_order_totals += _add_totals(self.target, actual)
        self.invalid_line_totals += sum(
            quantity * price != total
            for _, _, _, quantity, price, total in actual.get("order_items", ())
        )

    def report(self) -> dict[str, object]:
        sums = {}
        for table, currency in sorted(self.source.keys() | self.target.keys()):
            key = (table, currency)
            source = self.source.get(key, Decimal(0))
            target = self.target.get(key, Decimal(0))
            sums.setdefault(table, {})[currency] = {
                "source": format(source, ".2f"),
                "target": format(target, ".2f"),
                "difference": format(target - source, ".2f"),
            }
        return {
            "passed": self.source == self.target
            and not any(self.mismatches.values())
            and not self.invalid_order_totals
            and not self.invalid_line_totals,
            "totals_by_currency": sums,
            "value_mismatches": self.mismatches,
            "invalid_order_totals": self.invalid_order_totals,
            "invalid_line_totals": self.invalid_line_totals,
        }


def _money_rows(table: str, rows: list[BusinessRow]) -> Counter[BusinessRow]:
    if table == "orders":
        return Counter((row[0], row[4], row[5]) for row in rows)
    if table == "payments":
        return Counter((row[0], row[5], row[7]) for row in rows)
    return Counter((row[0], row[1], *row[3:]) for row in rows)


def _add_totals(
    totals: dict[tuple[str, str], Decimal], tables: dict[str, list[BusinessRow]]
) -> int:
    orders = {row[0]: (row[4], row[5]) for row in tables.get("orders", ())}
    line_sums = {}
    for order_id, _, _, _, _, line_total in tables.get("order_items", ()):
        currency = orders[order_id][0]
        _accumulate(totals, ("order_items", currency), line_total)
        _accumulate(line_sums, order_id, line_total)
    for currency, amount in orders.values():
        _accumulate(totals, ("orders", currency), amount)
    for row in tables.get("payments", ()):
        _accumulate(totals, ("payments", row[7] or "UNKNOWN"), row[5])
    return sum(line_sums.get(order_id) != total for order_id, (_, total) in orders.items())


def _accumulate[K](totals: dict[K, Decimal], key: K, amount: Decimal) -> None:
    totals[key] = totals.get(key, Decimal(0)) + amount
