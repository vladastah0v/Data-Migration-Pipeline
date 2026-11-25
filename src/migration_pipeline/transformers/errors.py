from dataclasses import dataclass
from enum import StrEnum

from migration_pipeline.extractors.base import LegacyEntity


class ErrorPolicy(StrEnum):
    STRICT = "strict"
    SKIP = "skip"
    QUARANTINE = "quarantine"


class ErrorCode(StrEnum):
    REQUIRED = "required"
    INVALID_TEXT = "invalid_text"
    INVALID_ID = "invalid_id"
    INVALID_EMAIL = "invalid_email"
    INVALID_PHONE = "invalid_phone"
    INVALID_DATE = "invalid_date"
    INVALID_STATUS = "invalid_status"
    INVALID_CURRENCY = "invalid_currency"
    INVALID_JSON = "invalid_json"
    INVALID_QUANTITY = "invalid_quantity"
    INVALID_MONEY = "invalid_money"
    TOTAL_MISMATCH = "total_mismatch"
    MISSING_PARENT = "missing_parent"
    PAYMENT_REFERENCE_CONFLICT = "payment_reference_conflict"


MESSAGES = {
    ErrorCode.REQUIRED: "Обязательное поле отсутствует.",
    ErrorCode.INVALID_TEXT: "Требуется текстовое значение.",
    ErrorCode.INVALID_ID: "Идентификатор должен быть целым числом в диапазоне BIGINT.",
    ErrorCode.INVALID_EMAIL: "Некорректный email.",
    ErrorCode.INVALID_PHONE: "Телефон невозможно однозначно привести к международному формату.",
    ErrorCode.INVALID_DATE: "Дата недопустима, не распознана или нарушает порядок событий.",
    ErrorCode.INVALID_STATUS: "Неизвестный статус.",
    ErrorCode.INVALID_CURRENCY: "Ожидается трёхбуквенный код валюты.",
    ErrorCode.INVALID_JSON: "Недопустимая структура JSON.",
    ErrorCode.INVALID_QUANTITY: "Количество должно быть положительным целым числом.",
    ErrorCode.INVALID_MONEY: "Сумма должна помещаться в NUMERIC(14,2) без округления.",
    ErrorCode.TOTAL_MISMATCH: "Сумма расходится с расчётом по позициям.",
    ErrorCode.MISSING_PARENT: "Связанная запись отсутствует среди принятых.",
    ErrorCode.PAYMENT_REFERENCE_CONFLICT: "Платёжная ссылка уже принадлежит другой записи.",
}


class FieldError(ValueError):
    def __init__(self, code: ErrorCode, field: str) -> None:
        self.code = code
        self.field = field
        super().__init__(MESSAGES[code])


@dataclass(frozen=True, slots=True)
class RecordIssue:
    entity: LegacyEntity
    batch_number: int
    legacy_record_id: int | None
    code: ErrorCode
    field: str
    is_quarantined: bool

    def log_fields(self) -> dict[str, str | int | bool | None]:
        return {
            "entity_type": self.entity.value,
            "batch_number": self.batch_number,
            "legacy_record_id": self.legacy_record_id,
            "error_code": self.code.value,
            "field": self.field,
            "error_message": MESSAGES[self.code],
            "is_quarantined": self.is_quarantined,
        }


class StrictTransformError(RuntimeError):
    def __init__(self, issue: RecordIssue) -> None:
        self.issue = issue
        super().__init__(
            f"{issue.entity.value}, ID {issue.legacy_record_id}, {issue.field} — "
            f"{MESSAGES[issue.code]}"
        )
