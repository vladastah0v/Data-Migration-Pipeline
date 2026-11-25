import json
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from random import Random
from typing import Annotated, NamedTuple

from faker import Faker
from pydantic import BaseModel, Field
from sqlalchemy import Table

from migration_pipeline.models.legacy import (
    LegacyCustomer,
    LegacyOrder,
    LegacyPayment,
    LegacyStatusHistory,
)


class SeedProfile(StrEnum):
    SMALL = "small"
    MEDIUM = "medium"
    LARGE = "large"


@dataclass(frozen=True, slots=True)
class ProfileSize:
    customers: int
    orders: int
    payments: int

    @property
    def history(self) -> int:
        return 2 * self.orders


PROFILE_SIZES = {
    SeedProfile.SMALL: ProfileSize(1_000, 5_000, 5_000),
    SeedProfile.MEDIUM: ProfileSize(10_000, 50_000, 50_000),
    SeedProfile.LARGE: ProfileSize(100_000, 250_000, 150_000),
}

DEFAULT_SEED = 42
DEFAULT_BATCH_SIZE = 1_000
MAX_BATCH_SIZE = 10_000
Rate = Annotated[Decimal, Field(ge=0, le=1, allow_inf_nan=False)]

CUSTOMER_EPOCH = datetime(2019, 1, 1, tzinfo=UTC)
BIRTHDAY_EPOCH = date(1950, 1, 1)
ORDER_EPOCH = datetime(2022, 1, 1, tzinfo=UTC)
PRODUCT_NAMES = ("Кабель", "Кронштейн", "Клавиатура", "Переходник", "Чехол", "Фильтр")


class SeedOptions(BaseModel):
    profile: SeedProfile = SeedProfile.SMALL
    seed: int = DEFAULT_SEED
    batch_size: Annotated[int, Field(ge=1, le=MAX_BATCH_SIZE)] = DEFAULT_BATCH_SIZE
    invalid_rate: Rate = Decimal(0)
    duplicate_contact_rate: Rate = Decimal(0)


class CustomerRow(NamedTuple):
    legacy_id: int
    full_name: str | None
    email: str
    phone: str | None
    address_text: str | None
    city: str | None
    country: str
    birth_date_text: str
    customer_status: str
    extra_data: str
    created_at: datetime
    updated_at: datetime


class OrderRow(NamedTuple):
    legacy_order_id: int
    customer_legacy_id: int
    order_number: str
    order_date_text: str
    order_status: str
    currency: str
    total_amount: str
    items_json: str
    created_at: datetime


class PaymentRow(NamedTuple):
    legacy_payment_id: int
    legacy_order_id: int
    payment_reference: str | None
    payment_method: str
    payment_status: str
    amount: str
    payment_date_text: str


class HistoryRow(NamedTuple):
    id: int
    legacy_order_id: int
    old_status: str | None
    new_status: str
    changed_at_text: str


@dataclass(frozen=True, slots=True)
class SourceRows:
    table: Table
    columns: tuple[str, ...]
    count: int
    invalid_count: int
    rows: Iterator[tuple[object, ...]]


@dataclass(frozen=True, slots=True)
class Selection:
    population: int
    quota: int
    phase: int

    @classmethod
    def from_rate(cls, population: int, rate: Decimal, random: Random) -> "Selection":
        numerator, denominator = rate.as_integer_ratio()
        phase = random.randrange(population) if population else 0
        return cls(population, population * numerator // denominator, phase)

    def index_at(self, position: int) -> int | None:
        if not self.quota:
            return None
        previous, remainder = divmod((position - 1) * self.quota + self.phase, self.population)
        if remainder + self.quota >= self.population:
            return previous
        return None


class LegacyDataGenerator:
    def __init__(self, options: SeedOptions) -> None:
        self.seed = options.seed
        self.size = PROFILE_SIZES[options.profile]
        self._customer_errors = self._selection(
            self.size.customers, options.invalid_rate, "customers"
        )
        self._order_errors = self._selection(self.size.orders, options.invalid_rate, "orders")
        self._payment_errors = self._selection(self.size.payments, options.invalid_rate, "payments")
        self._history_errors = self._selection(self.size.history, options.invalid_rate, "history")
        eligible = max(0, self.size.customers - self._customer_errors.quota - 1)
        self._duplicates = self._selection(eligible, options.duplicate_contact_rate, "duplicates")

    @property
    def duplicate_contacts(self) -> int:
        return self._duplicates.quota

    def tables(self) -> tuple[SourceRows, ...]:
        return (
            SourceRows(
                LegacyCustomer.__table__,
                CustomerRow._fields,
                self.size.customers,
                self._customer_errors.quota,
                self._customers(),
            ),
            SourceRows(
                LegacyOrder.__table__,
                OrderRow._fields,
                self.size.orders,
                self._order_errors.quota,
                self._orders(),
            ),
            SourceRows(
                LegacyPayment.__table__,
                PaymentRow._fields,
                self.size.payments,
                self._payment_errors.quota,
                self._payments(),
            ),
            SourceRows(
                LegacyStatusHistory.__table__,
                HistoryRow._fields,
                self.size.history,
                self._history_errors.quota,
                self._history(),
            ),
        )

    def _selection(self, population: int, rate: Decimal, key: str) -> Selection:
        return Selection.from_rate(population, rate, Random(f"{self.seed}:{key}:selection"))

    def _customers(self) -> Iterator[CustomerRow]:
        faker = Faker("en_US")
        faker.seed_instance(self.seed)
        random = Random(f"{self.seed}:customers:data")
        valid_position = 0
        shared_email = ""
        for legacy_id in range(1, self.size.customers + 1):
            created_at = CUSTOMER_EPOCH + timedelta(minutes=legacy_id)
            birthday = BIRTHDAY_EPOCH + timedelta(days=random.randrange(20_000))
            name = f"{faker.first_name()} {faker.last_name()}"
            email = f"customer{legacy_id}@example.test"
            error = self._customer_errors.index_at(legacy_id)
            if error is None:
                valid_position += 1
                if valid_position == 1:
                    shared_email = email
                elif self._duplicates.index_at(valid_position - 1) is not None:
                    email = shared_email
            row = CustomerRow(
                legacy_id=legacy_id,
                full_name=(name.upper() if legacy_id % 7 == 0 else name),
                email=(f" {email.upper()} " if legacy_id % 3 == 0 else email),
                phone=_test_phone(legacy_id),
                address_text=(f"{legacy_id} Example Avenue" if legacy_id % 5 else None),
                city=random.choice(("Example City", "Demo Junction", "Sample Bay")),
                country=("USA", "United States", "US")[legacy_id % 3],
                birth_date_text=_format_date(birthday, legacy_id),
                customer_status=_format_status(
                    random.choice(("active", "inactive", "blocked")), legacy_id
                ),
                extra_data=json.dumps(
                    {
                        "segment": random.choice(("standard", "business")),
                        "source": "synthetic",
                        "marketing_opt_in": bool(legacy_id % 2),
                    }
                ),
                created_at=created_at,
                updated_at=created_at + timedelta(days=legacy_id % 30),
            )
            if error is not None:
                field, value = CUSTOMER_ERRORS[error % len(CUSTOMER_ERRORS)]
                row = row._replace(**{field: value})
            yield row

    def _orders(self) -> Iterator[OrderRow]:
        random = Random(f"{self.seed}:orders:data")
        for legacy_id in range(1, self.size.orders + 1):
            ordered_at = _order_date(legacy_id)
            items = []
            total_cents = 0
            for product, quantity, cents in self._order_lines(legacy_id):
                items.append(
                    {
                        "product_name": product,
                        "quantity": quantity,
                        "unit_price": _format_amount(cents, legacy_id),
                        "line_total": _format_amount(quantity * cents, legacy_id),
                    }
                )
                total_cents += quantity * cents
            row = OrderRow(
                legacy_order_id=legacy_id,
                customer_legacy_id=random.randint(1, self.size.customers),
                order_number=f"CRM-{legacy_id:08d}",
                order_date_text=_format_time(ordered_at, legacy_id),
                order_status=_format_status(self._final_order_status(legacy_id), legacy_id),
                currency=("USD", "EUR", "GBP")[legacy_id % 3],
                total_amount=_format_amount(total_cents, legacy_id),
                items_json=json.dumps(items, ensure_ascii=False),
                created_at=ordered_at,
            )
            error = self._order_errors.index_at(legacy_id)
            if error is not None:
                field, value = ORDER_ERRORS[error % len(ORDER_ERRORS)]
                row = row._replace(**{field: value})
            yield row

    def _payments(self) -> Iterator[PaymentRow]:
        for legacy_id in range(1, self.size.payments + 1):
            total_cents = sum(
                quantity * cents for _, quantity, cents in self._order_lines(legacy_id)
            )
            row = PaymentRow(
                legacy_payment_id=legacy_id,
                legacy_order_id=legacy_id,
                payment_reference=f"PAY-{legacy_id:08d}",
                payment_method=("card", "bank_transfer", "cash")[legacy_id % 3],
                payment_status=_format_status("completed", legacy_id),
                amount=_format_amount(total_cents, legacy_id + 1),
                payment_date_text=_format_time(
                    _order_date(legacy_id) + timedelta(hours=1), legacy_id
                ),
            )
            error = self._payment_errors.index_at(legacy_id)
            if error is not None:
                field, value = PAYMENT_ERRORS[error % len(PAYMENT_ERRORS)]
                row = row._replace(**{field: value})
            yield row

    def _history(self) -> Iterator[HistoryRow]:
        for legacy_id in range(1, self.size.history + 1):
            order_id = (legacy_id + 1) // 2
            ordered_at = _order_date(order_id)
            if legacy_id % 2:
                row = HistoryRow(
                    legacy_id,
                    order_id,
                    None,
                    _format_status("pending", legacy_id),
                    _format_time(ordered_at, legacy_id),
                )
            else:
                row = HistoryRow(
                    legacy_id,
                    order_id,
                    _format_status("pending", legacy_id - 1),
                    _format_status(self._final_order_status(order_id), legacy_id),
                    _format_time(ordered_at + timedelta(days=1), legacy_id),
                )
            error = self._history_errors.index_at(legacy_id)
            if error is not None:
                field, value = HISTORY_ERRORS[error % len(HISTORY_ERRORS)]
                row = row._replace(**{field: value})
            yield row

    def _order_lines(self, order_id: int) -> Iterator[tuple[str, int, int]]:
        random = Random(f"{self.seed}:orders:{order_id}:lines")
        for _ in range(random.randint(1, 4)):
            yield random.choice(PRODUCT_NAMES), random.randint(1, 5), random.randint(100, 25_000)

    def _final_order_status(self, order_id: int) -> str:
        return "completed" if order_id <= self.size.payments else "processing"


CUSTOMER_ERRORS = (
    ("email", "invalid-email"),
    ("birth_date_text", "31.02.1990"),
    ("customer_status", "unknown-customer-status"),
    ("full_name", None),
    ("phone", "invalid-phone"),
)
ORDER_ERRORS = (
    ("total_amount", "not-money"),
    ("order_date_text", "not-a-date"),
    ("items_json", '{"malformed": true}'),
    ("currency", "?"),
    ("order_status", "unknown-order-status"),
)
PAYMENT_ERRORS = (
    ("amount", "-10.00"),
    ("payment_date_text", "not-a-date"),
    ("payment_status", "unknown-payment-status"),
    ("payment_reference", None),
)
HISTORY_ERRORS = (
    ("changed_at_text", "not-a-date"),
    ("new_status", "unknown-order-status"),
)


def _order_date(order_id: int) -> datetime:
    return ORDER_EPOCH + timedelta(minutes=order_id)


def _format_date(value: date, key: int) -> str:
    return value.strftime(("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y")[key % 3])


def _format_time(value: datetime, key: int) -> str:
    if key % 3 == 0:
        return value.isoformat(timespec="seconds")
    return value.strftime(("%d.%m.%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S")[key % 2])


def _format_status(status: str, key: int) -> str:
    return (status, status.upper(), f" {status.title()} ")[key % 3]


def _format_amount(cents: int, key: int) -> str:
    amount, fraction = divmod(cents, 100)
    separator = "," if key % 3 == 0 else "."
    return f"{amount}{separator}{fraction:02d}"


def _test_phone(legacy_id: int) -> str | None:
    if legacy_id > 100:
        return None
    line = 100 + legacy_id - 1
    if legacy_id % 3 == 0:
        return f"+1202555{line:04d}"
    if legacy_id % 3 == 1:
        return f"+1 (202) 555-{line:04d}"
    return f"202-555-{line:04d}"
