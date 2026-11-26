import json
from collections.abc import Iterable
from html import escape

from migration_pipeline.reporting.service import NOT_MEASURED, ReportFormat

_CHECKS = {
    "record_counts": "Количество записей",
    "completeness": "Полнота переноса",
    "foreign_keys": "Внешние связи",
    "financial": "Денежные суммы",
    "checksums": "Контрольные суммы",
}


def render_report(report: dict[str, object], format: ReportFormat) -> str:
    if format is ReportFormat.JSON:
        return json.dumps(report, ensure_ascii=False, indent=2) + "\n"

    counts = [
        (
            "Исходных записей",
            report["records_source"] if report["records_source"] is not None else "Недоступно",
        ),
        ("Обработано", report["records_processed"]),
        ("Успешно перенесено", report["records_transferred"]),
        ("Вставлено", report["records_inserted"]),
        ("Обновлено", report["records_updated"]),
        ("Без изменений", report["records_unchanged"]),
        ("Отклонено", report["records_rejected"]),
        ("Записей аудита ошибок", report["error_count"]),
        ("Повторов", report["retry_count"]),
        ("Длительность, с", _number(report["duration_seconds"])),
        ("Скорость, записей/с", _number(report["records_per_second"])),
        ("Пиковый RSS, МиБ", _rss(report["peak_rss_bytes"])),
    ]
    overview = (
        ("UUID запуска", report["run_id"]),
        ("UUID снимка legacy", report["source_snapshot_id"]),
        ("Статус", report["status"]),
        ("Начало", report["started_at"]),
        ("Окончание загрузки", report["finished_at"]),
        ("Состояние на", report["observed_at"]),
        ("Стратегия загрузки", report["strategy"]),
        ("Размер партии", report["batch_size"]),
        ("Политика отклонения", report["policy"]),
    )
    sections = [
        "# Отчёт о миграции",
        _table(("Параметр", "Значение"), overview),
        "## Записи и ресурсы",
        _table(("Показатель", "Значение"), counts),
    ]
    if report["error_message"]:
        sections.extend(("## Причина сбоя", _cell(report["error_message"])))

    sections.extend(
        (
            "## Контрольные точки",
            _table(
                ("Сущность", "Последний legacy ID", "Строки", "Следующая партия"),
                [
                    (
                        item["entity_type"],
                        item["last_legacy_id"],
                        item["records_processed"],
                        item["next_batch"],
                    )
                    for item in report["checkpoints"]
                ],
            ),
            "## Партии",
            _table(("Статус", "Количество"), list(report["batches"].items())),
            "## Повторы",
            _table(
                ("Операция", "Причина", "Количество"),
                [(item["operation"], item["reason"], item["count"]) for item in report["retries"]],
            ),
            "## Журнал ошибок",
            _table(
                ("Сущность", "Код", "Карантин", "Подтверждённое отклонение", "Количество"),
                [
                    (
                        item["entity_type"],
                        item["error_code"],
                        _yes(item["is_quarantined"]),
                        _yes(item["is_batch_rejection"]),
                        item["count"],
                    )
                    for item in report["errors"]
                ],
            ),
            "## Верификация",
        )
    )
    verification = report["verification_report"]
    if verification is None:
        sections.append("Верификация ещё не записана.")
    else:
        sections.extend(
            (
                f"Дата проверки — {_cell(report['verified_at'])}",
                _table(
                    ("Проверка", "Результат"),
                    [
                        (_CHECKS.get(check, check), "Пройдена" if passed else "Расхождение")
                        for check, passed in verification["checks"].items()
                    ],
                ),
            )
        )
    return "\n\n".join(sections) + "\n"


def _table(header: tuple[str, ...], rows: Iterable[tuple[object, ...]]) -> str:
    data = ["| " + " | ".join(map(_cell, row)) + " |" for row in rows]
    if not data:
        return "Записей нет."
    return "\n".join(
        (
            "| " + " | ".join(header) + " |",
            "| " + " | ".join(("---",) * len(header)) + " |",
            *data,
        )
    )


def _cell(value: object) -> str:
    if value is None:
        return "—"
    return (
        escape(str(value))
        .replace("\\", "\\\\")
        .replace("|", "\\|")
        .replace("\n", " ")
        .replace("\r", " ")
    )


def _number(value: float | None) -> str:
    return f"{value:.3f}".replace(".", ",") if value is not None else NOT_MEASURED


def _rss(value: int | None) -> str:
    return _number(value / 2**20) if value is not None else NOT_MEASURED


def _yes(value: bool) -> str:
    return "да" if value else "нет"
