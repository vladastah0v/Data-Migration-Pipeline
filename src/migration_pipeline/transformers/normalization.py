import json
import re
from collections.abc import Mapping
from datetime import UTC, date, datetime
from decimal import Decimal

from migration_pipeline.extractors.base import BIGINT_MAX, BIGINT_MIN
from migration_pipeline.transformers.errors import ErrorCode, FieldError

MAX_MONEY = Decimal("999999999999.99")
CENT = Decimal("0.01")
MONEY_PATTERN = re.compile(r"[-+]?[0-9]+(?:[.,][0-9]+)?")
EMAIL_LOCAL = re.compile(r"[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*")
DOMAIN_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
PHONE_FORMAT = re.compile(r"\+?[0-9 .()\t-]+")
INTERNATIONAL_PHONE = re.compile(r"\+[1-9][0-9]{7,14}")
NORTH_AMERICAN_PHONE = re.compile(r"[2-9][0-9]{2}[2-9][0-9]{6}")
RUSSIAN_PHONE = re.compile(r"[3489][0-9]{9}")

COUNTRY_CODES = {
    "us": "US",
    "usa": "US",
    "united states": "US",
    "united states of america": "US",
    "ca": "CA",
    "canada": "CA",
    "ru": "RU",
    "rus": "RU",
    "russia": "RU",
    "россия": "RU",
    "gb": "GB",
    "uk": "GB",
    "united kingdom": "GB",
}

CUSTOMER_STATUSES = {
    "active": "active",
    "inactive": "inactive",
    "blocked": "blocked",
    "disabled": "inactive",
    "suspended": "blocked",
}
ORDER_STATUSES = {
    "pending": "pending",
    "new": "pending",
    "confirmed": "confirmed",
    "processing": "processing",
    "paid": "paid",
    "shipped": "shipped",
    "delivered": "delivered",
    "completed": "completed",
    "cancelled": "cancelled",
    "canceled": "cancelled",
    "refunded": "refunded",
}
PAYMENT_STATUSES = {
    "pending": "pending",
    "completed": "completed",
    "paid": "completed",
    "success": "completed",
    "failed": "failed",
    "declined": "failed",
    "cancelled": "cancelled",
    "canceled": "cancelled",
    "refunded": "refunded",
}


def optional_text(value: object, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or "\x00" in value:
        raise FieldError(ErrorCode.INVALID_TEXT, field)
    return value.strip() or None


def required_text(value: object, field: str) -> str:
    result = optional_text(value, field)
    if result is None:
        raise FieldError(ErrorCode.REQUIRED, field)
    return result


def legacy_id(value: object, field: str) -> int:
    if type(value) is not int or not BIGINT_MIN <= value <= BIGINT_MAX:
        raise FieldError(ErrorCode.INVALID_ID, field)
    return value


def country_code(value: object) -> str | None:
    country = optional_text(value, "country")
    if country is None:
        return None
    country = " ".join(country.split())
    return COUNTRY_CODES.get(country.casefold(), country)


def normalize_email(value: object) -> str | None:
    email = optional_text(value, "email")
    if email is None:
        return None
    email = email.lower()
    local, separator, domain = email.partition("@")
    labels = domain.split(".")
    if (
        not separator
        or len(email) > 254
        or len(local) > 64
        or not EMAIL_LOCAL.fullmatch(local)
        or len(labels) < 2
        or not all(DOMAIN_LABEL.fullmatch(label) for label in labels)
    ):
        raise FieldError(ErrorCode.INVALID_EMAIL, "email")
    return email


def normalize_phone(value: object, country: str | None) -> str | None:
    phone = optional_text(value, "phone")
    if phone is None:
        return None
    if not PHONE_FORMAT.fullmatch(phone):
        raise FieldError(ErrorCode.INVALID_PHONE, "phone")
    digits = phone.translate(str.maketrans("", "", " .()\t-"))
    if digits.startswith("00"):
        digits = "+" + digits[2:]
    if INTERNATIONAL_PHONE.fullmatch(digits):
        return digits
    if country in {"US", "CA"}:
        national = digits.removeprefix("1") if len(digits) == 11 else digits
        if NORTH_AMERICAN_PHONE.fullmatch(national):
            return "+1" + national
    if country == "RU":
        national = digits[1:] if len(digits) == 11 and digits[0] in "78" else digits
        if RUSSIAN_PHONE.fullmatch(national):
            return "+7" + national
    raise FieldError(ErrorCode.INVALID_PHONE, "phone")


def normalize_status(
    value: object, choices: Mapping[str, str], field: str, *, optional: bool = False
) -> str | None:
    status = optional_text(value, field)
    if status is None and optional:
        return None
    if status is not None and (normalized := choices.get(status.casefold())) is not None:
        return normalized
    raise FieldError(ErrorCode.INVALID_STATUS, field)


def normalize_date(value: object, field: str) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    value = optional_text(value, field)
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        for format_ in ("%d.%m.%Y", "%d/%m/%Y"):
            try:
                return datetime.strptime(value, format_).date()
            except ValueError:
                continue
    raise FieldError(ErrorCode.INVALID_DATE, field)


def normalize_timestamp(value: object, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        timestamp = required_text(value, field)
        try:
            parsed = datetime.fromisoformat(timestamp)
        except ValueError:
            for format_ in ("%d.%m.%Y %H:%M:%S", "%d/%m/%Y %H:%M:%S"):
                try:
                    parsed = datetime.strptime(timestamp, format_)
                    break
                except ValueError:
                    continue
            else:
                normalized = normalize_date(timestamp, field)
                parsed = datetime.combine(normalized, datetime.min.time())
    try:
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    except OverflowError:
        raise FieldError(ErrorCode.INVALID_DATE, field) from None


def normalize_money(value: object, field: str) -> Decimal:
    if isinstance(value, str):
        text = value.strip()
        if not MONEY_PATTERN.fullmatch(text):
            raise FieldError(ErrorCode.INVALID_MONEY, field)
        amount = Decimal(text.replace(",", "."))
    elif isinstance(value, Decimal) or type(value) is int:
        amount = Decimal(value)
    else:
        raise FieldError(ErrorCode.INVALID_MONEY, field)
    if not amount.is_finite() or not 0 <= amount <= MAX_MONEY:
        raise FieldError(ErrorCode.INVALID_MONEY, field)
    cents = amount.quantize(CENT)
    if amount != cents:
        raise FieldError(ErrorCode.INVALID_MONEY, field)
    return abs(cents)


def read_json(value: object, field: str) -> object:
    if isinstance(value, str):
        try:
            return json.loads(value, parse_float=Decimal, parse_constant=_invalid_json_constant)
        except (ValueError, RecursionError):
            raise FieldError(ErrorCode.INVALID_JSON, field) from None
    return value


def _invalid_json_constant(_value: str) -> None:
    raise ValueError("JSON constants must be finite")
