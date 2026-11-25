from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime

from migration_pipeline.transformers.errors import ErrorCode, FieldError
from migration_pipeline.transformers.normalization import (
    CUSTOMER_STATUSES,
    country_code,
    legacy_id,
    normalize_date,
    normalize_email,
    normalize_phone,
    normalize_status,
    normalize_timestamp,
    optional_text,
    read_json,
    required_text,
)


@dataclass(frozen=True, slots=True)
class ContactData:
    contact_type: str
    contact_value: str
    is_primary: bool = True


@dataclass(frozen=True, slots=True)
class AddressData:
    city: str | None
    country: str | None
    address_line: str | None


@dataclass(frozen=True, slots=True)
class CustomerData:
    legacy_id: int
    first_name: str
    last_name: str | None
    email: str | None
    birth_date: date | None
    status: str
    created_at: datetime
    updated_at: datetime
    contacts: tuple[ContactData, ...]
    address: AddressData | None
    attributes: dict[str, object]


def transform_customer(row: Mapping[str, object]) -> CustomerData:
    customer_id = legacy_id(row.get("legacy_id"), "legacy_id")
    parts = required_text(row.get("full_name"), "full_name").split()
    first_name, last_name = parts if len(parts) == 2 else (" ".join(parts), None)

    email = normalize_email(row.get("email"))
    country = country_code(row.get("country"))
    phone = normalize_phone(row.get("phone"), country)
    contacts = tuple(
        ContactData(contact_type, value)
        for contact_type, value in (("email", email), ("phone", phone))
        if value is not None
    )
    city = optional_text(row.get("city"), "city")
    address_line = optional_text(row.get("address_text"), "address_text")
    address = (
        AddressData(city, country, address_line) if any((city, country, address_line)) else None
    )

    birth_date = normalize_date(row.get("birth_date_text"), "birth_date_text")
    created_at = normalize_timestamp(row.get("created_at"), "created_at")
    updated_at = normalize_timestamp(row.get("updated_at"), "updated_at")
    if birth_date is not None and birth_date > created_at.date():
        raise FieldError(ErrorCode.INVALID_DATE, "birth_date_text")
    if updated_at < created_at:
        raise FieldError(ErrorCode.INVALID_DATE, "updated_at")

    return CustomerData(
        customer_id,
        first_name,
        last_name,
        email,
        birth_date,
        normalize_status(row.get("customer_status"), CUSTOMER_STATUSES, "customer_status"),
        created_at,
        updated_at,
        contacts,
        address,
        _attributes(row.get("extra_data")),
    )


def _attributes(value: object) -> dict[str, object]:
    value = read_json(value, "extra_data")
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise FieldError(ErrorCode.INVALID_JSON, "extra_data")
    result = {}
    for raw_key, item in value.items():
        key = required_text(raw_key, "extra_data")
        if key in result:
            raise FieldError(ErrorCode.INVALID_JSON, "extra_data")
        result[key] = item
    return result
