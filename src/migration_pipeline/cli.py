import asyncio
import json
import os
import sys
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from pathlib import Path
from time import perf_counter
from typing import IO, Annotated
from uuid import UUID

import typer
from alembic.util import CommandError
from asyncpg import InterfaceError, PostgresError
from pydantic import BaseModel, ValidationError
from pydantic_settings import SettingsError
from rich.console import Console
from rich.progress import BarColumn, MofNCompleteColumn, Progress, SpinnerColumn, TaskID, TextColumn
from rich.table import Table
from rich.text import Text
from sqlalchemy.exc import DBAPIError, SQLAlchemyError

from migration_pipeline.config import (
    DatabaseSettings,
    DatabaseSide,
    LegacyAdminSettings,
    LegacySettings,
    SchemaSettings,
    Settings,
)
from migration_pipeline.database.connections import POSTGRES_MAJOR, read_server_version
from migration_pipeline.database.legacy import LegacySourceError, open_legacy_source
from migration_pipeline.database.schema import DatabaseSchemaError, upgrade_schema
from migration_pipeline.extractors.base import (
    BIGINT_MAX,
    BIGINT_MIN,
    LegacyEntity,
    extract_batches,
)
from migration_pipeline.extractors.base import DEFAULT_BATCH_SIZE as DEFAULT_EXTRACT_BATCH_SIZE
from migration_pipeline.extractors.base import MAX_BATCH_SIZE as MAX_EXTRACT_BATCH_SIZE
from migration_pipeline.loaders.base import ConflictPolicy, LoadStrategy
from migration_pipeline.pipeline.orchestrator import (
    MigrationFailed,
    MigrationOptions,
    MigrationOrchestrator,
    migration_status,
    run_fields,
)
from migration_pipeline.pipeline.state import RunState
from migration_pipeline.seeding.data import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_SEED,
    MAX_BATCH_SIZE,
    SeedOptions,
    SeedProfile,
)
from migration_pipeline.seeding.service import SeedError, seed_legacy
from migration_pipeline.transformers.errors import ErrorPolicy, StrictTransformError
from migration_pipeline.transformers.preview import PreviewError, ReportEntry, preview_transform

app = typer.Typer(
    name="migration-pipeline",
    help="Пакетная миграция данных между базами PostgreSQL.",
    no_args_is_help=True,
    add_completion=False,
)

console = Console()
error_console = Console(stderr=True)


@app.callback()
def main() -> None:
    """Управление подключениями и запусками миграции."""


@app.command()
def check() -> None:
    """Проверить конфигурацию и доступность обеих баз PostgreSQL 16."""
    settings = _load_settings(Settings)
    databases = (("Legacy", settings.legacy_db), ("Target", settings.target_db))
    results = asyncio.run(_check_databases(settings))
    table = Table("База", "Подключение", "Результат", title="Проверка PostgreSQL")
    for (label, database), (success, message) in zip(databases, results, strict=True):
        address = Text(f"{database.host}:{database.port}/{database.name}")
        table.add_row(label, address, Text(message, style="green" if success else "red"))
    console.print(table)

    if not all(success for success, _ in results):
        raise typer.Exit(code=1)


@app.command()
def init() -> None:
    """Применить миграции Alembic к исходной и целевой базам."""
    settings = _load_settings(SchemaSettings)
    databases: tuple[DatabaseSide, ...] = ("legacy", "target")
    for side in databases:
        console.print(f"{side} — применение миграций")
        try:
            asyncio.run(upgrade_schema(settings, side))
        except (OSError, SQLAlchemyError, PostgresError, InterfaceError, CommandError) as exc:
            error_console.print(f"{side} — {_database_error(exc)}", markup=False, style="red")
            raise typer.Exit(code=1) from exc
        console.print(f"{side} — схема обновлена", style="green")


@app.command()
def seed(
    profile: Annotated[SeedProfile, typer.Option(help="Объём синтетического набора.")] = (
        SeedProfile.SMALL
    ),
    random_seed: Annotated[int, typer.Option("--seed", help="Зерно генератора.")] = DEFAULT_SEED,
    batch_size: Annotated[
        int,
        typer.Option(min=1, max=MAX_BATCH_SIZE, help="Число строк в одной операции COPY."),
    ] = DEFAULT_BATCH_SIZE,
    invalid_rate: Annotated[
        str, typer.Option(help="Доля строк с дефектами в каждой таблице от 0 до 1.")
    ] = "0",
    duplicate_contact_rate: Annotated[
        str, typer.Option(help="Доля корректных клиентов с повторяющимся email от 0 до 1.")
    ] = "0",
) -> None:
    """Пакетно заполнить пустую legacy-БД и зафиксировать снимок."""
    options = _load_settings(
        SeedOptions,
        profile=profile,
        seed=random_seed,
        batch_size=batch_size,
        invalid_rate=invalid_rate,
        duplicate_contact_rate=duplicate_contact_rate,
    )
    settings = _load_settings(LegacyAdminSettings)
    start = perf_counter()
    _cli_event(
        "seed_started",
        profile=profile.value,
        seed=options.seed,
        batch_size=options.batch_size,
        invalid_rate=str(options.invalid_rate),
        duplicate_contact_rate=str(options.duplicate_contact_rate),
    )

    with Progress(
        TextColumn("{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        console=console,
        disable=not console.is_terminal,
    ) as progress:
        tasks: dict[str, TaskID] = {}

        def update_progress(table: str, done: int, total: int) -> None:
            if table not in tasks:
                tasks[table] = progress.add_task(table, total=total)
            progress.update(tasks[table], completed=done)

        try:
            result = asyncio.run(seed_legacy(settings, options, update_progress))
        except SeedError as exc:
            _cli_event("seed_failed", reason=type(exc).__name__)
            error_console.print(str(exc), style="red", markup=False)
            raise typer.Exit(code=1) from exc
        except (OSError, SQLAlchemyError, PostgresError, InterfaceError, CommandError) as exc:
            _cli_event("seed_failed", reason=type(exc).__name__)
            error_console.print(_database_error(exc), style="red", markup=False)
            raise typer.Exit(code=1) from exc

    table = Table("Таблица", "Строки", "С дефектами", title="Синтетические данные")
    for entry in result.tables:
        table.add_row(entry.name, str(entry.rows), str(entry.invalid_rows))
    console.print(table)
    console.print(f"Зафиксирован снимок {result.snapshot_id}")
    _cli_event(
        "seed_completed",
        snapshot_id=str(result.snapshot_id),
        counts={entry.name: entry.rows for entry in result.tables},
        invalid_counts={entry.name: entry.invalid_rows for entry in result.tables},
        duplicate_contacts=result.duplicate_contacts,
        duration_seconds=round(perf_counter() - start, 3),
    )


@app.command()
def extract(
    entity: Annotated[LegacyEntity, typer.Option(help="Сущность источника для чтения.")],
    batch_size: Annotated[
        int,
        typer.Option(min=1, max=MAX_EXTRACT_BATCH_SIZE, help="Максимум строк в партии."),
    ] = DEFAULT_EXTRACT_BATCH_SIZE,
    after_id: Annotated[
        int | None,
        typer.Option(min=BIGINT_MIN, max=BIGINT_MAX, help="Последний уже обработанный ID."),
    ] = None,
    snapshot_id: Annotated[
        UUID | None,
        typer.Option(help="Ожидаемый UUID зафиксированного снимка legacy."),
    ] = None,
) -> None:
    """Прочитать сущность из снимка legacy и показать сводку чтения."""
    settings = _load_settings(LegacySettings)
    _cli_event(
        "extract_started",
        entity_type=entity.value,
        batch_size=batch_size,
        after_id=after_id,
        expected_snapshot_id=str(snapshot_id) if snapshot_id is not None else None,
    )

    with Progress(
        SpinnerColumn(),
        TextColumn("{task.description} — строк: {task.completed:.0f}"),
        console=console,
        disable=not console.is_terminal,
        redirect_stderr=False,
    ) as progress:
        try:
            actual_id, batches, records, last_id = asyncio.run(
                _inspect_extraction(settings, entity, batch_size, after_id, snapshot_id, progress)
            )
        except (LegacySourceError, OSError, SQLAlchemyError, PostgresError, InterfaceError) as exc:
            _cli_event("extract_failed", entity_type=entity.value, reason=type(exc).__name__)
            message = str(exc) if isinstance(exc, LegacySourceError) else _database_error(exc)
            error_console.print(message, style="red", markup=False)
            raise typer.Exit(code=1) from exc

    table = Table("Сущность", "Партий", "Строк", "Последний ID", title="Чтение legacy")
    table.add_row(
        entity.value, str(batches), str(records), str(last_id) if last_id is not None else "—"
    )
    console.print(table)
    console.print(f"Снимок {actual_id}")
    _cli_event(
        "extract_completed",
        entity_type=entity.value,
        snapshot_id=str(actual_id),
        batches=batches,
        records_extracted=records,
        last_processed_id=last_id,
    )


async def _inspect_extraction(
    settings: LegacySettings,
    entity: LegacyEntity,
    batch_size: int,
    after_id: int | None,
    snapshot_id: UUID | None,
    progress: Progress,
) -> tuple[UUID, int, int, int | None]:
    batches = records = 0
    last_id = None
    async with open_legacy_source(settings, expected_snapshot_id=snapshot_id) as source:
        task = progress.add_task(entity.value, total=None)
        async for batch in extract_batches(
            source, entity, batch_size=batch_size, after_id=after_id
        ):
            batches += 1
            records += len(batch.rows)
            last_id = batch.last_id
            progress.update(task, advance=len(batch.rows))
            _cli_event("extract_batch", snapshot_id=str(source.snapshot.id), **batch.log_fields())
        return source.snapshot.id, batches, records, last_id


@app.command()
def transform(
    policy: Annotated[ErrorPolicy, typer.Option(help="Политика обработки ошибок.")] = (
        ErrorPolicy.STRICT
    ),
    batch_size: Annotated[
        int, typer.Option(min=1, max=MAX_EXTRACT_BATCH_SIZE, help="Максимум строк в партии.")
    ] = DEFAULT_EXTRACT_BATCH_SIZE,
    snapshot_id: Annotated[
        UUID | None, typer.Option(help="Ожидаемый UUID зафиксированного снимка.")
    ] = None,
    quarantine_file: Annotated[
        Path | None, typer.Option(dir_okay=False, help="Новый JSONL-файл карантина.")
    ] = None,
    duplicates_file: Annotated[
        Path | None, typer.Option(dir_okay=False, help="Новый JSONL-файл совпадений email.")
    ] = None,
) -> None:
    """Проверить трансформации снимка на временных таблицах PostgreSQL."""
    if (policy is ErrorPolicy.QUARANTINE) != (quarantine_file is not None):
        raise typer.BadParameter(
            "Укажите --quarantine-file при выборе политики quarantine.",
            param_hint="--quarantine-file",
        )

    settings = _load_settings(Settings)
    _cli_event("transform_started", policy=policy.value, batch_size=batch_size)
    try:
        with (
            ExitStack() as reports,
            Progress(
                SpinnerColumn(),
                TextColumn("{task.description} — проверено {task.completed:.0f}"),
                console=console,
                disable=not console.is_terminal,
                redirect_stderr=False,
            ) as progress,
        ):
            quarantine = reports.enter_context(_report_file(quarantine_file))
            duplicates = reports.enter_context(_report_file(duplicates_file))
            tasks: dict[LegacyEntity, TaskID] = {}

            def update_progress(entity: LegacyEntity, accepted: int, rejected: int) -> None:
                if entity not in tasks:
                    tasks[entity] = progress.add_task(entity.value, total=None)
                progress.update(tasks[entity], completed=accepted + rejected)

            result = asyncio.run(
                preview_transform(
                    settings,
                    batch_size=batch_size,
                    policy=policy,
                    snapshot_id=snapshot_id,
                    quarantine=lambda row: _write_report(quarantine, row),
                    duplicates=lambda row: _write_report(duplicates, row),
                    progress=update_progress,
                )
            )
    except StrictTransformError as exc:
        _cli_event("transform_failed", **exc.issue.log_fields())
        error_console.print(str(exc), style="red", markup=False)
        raise typer.Exit(code=1) from exc
    except (LegacySourceError, PreviewError, ReportFileError) as exc:
        _cli_event("transform_failed", reason=type(exc).__name__)
        error_console.print(str(exc), style="red", markup=False)
        raise typer.Exit(code=1) from exc
    except (OSError, SQLAlchemyError, PostgresError, InterfaceError) as exc:
        _cli_event("transform_failed", reason=type(exc).__name__)
        error_console.print(_database_error(exc), style="red", markup=False)
        raise typer.Exit(code=1) from exc

    table = Table("Сущность", "Принято", "Отклонено", title="Проверка трансформаций")
    for entity in result.entities:
        table.add_row(entity.entity.value, str(entity.accepted), str(entity.rejected))
    console.print(table)
    console.print(
        f"Совпадения email — {result.duplicate_email_groups} групп, "
        f"{result.duplicate_email_members} клиентов"
    )
    _cli_event(
        "transform_completed",
        snapshot_id=str(result.snapshot_id),
        accepted={entry.entity.value: entry.accepted for entry in result.entities},
        rejected={entry.entity.value: entry.rejected for entry in result.entities},
        error_counts=result.error_counts,
        duplicate_email_groups=result.duplicate_email_groups,
        duplicate_email_members=result.duplicate_email_members,
    )


@app.command()
def migrate(
    batch_size: Annotated[
        int, typer.Option(min=1, max=MAX_EXTRACT_BATCH_SIZE, help="Максимум строк в партии.")
    ] = DEFAULT_EXTRACT_BATCH_SIZE,
    strategy: Annotated[LoadStrategy, typer.Option(help="Способ загрузки.")] = LoadStrategy.UPSERT,
    policy: Annotated[
        ErrorPolicy, typer.Option(help="Политика отклонения записей.")
    ] = ErrorPolicy.STRICT,
    conflict_policy: Annotated[
        ConflictPolicy, typer.Option(help="Политика изменения уже загруженных записей.")
    ] = ConflictPolicy.VERIFY,
    transaction_timeout: Annotated[
        float, typer.Option(help="Таймаут транзакции партии в секундах.")
    ] = 60,
    snapshot_id: Annotated[
        UUID | None, typer.Option(help="Ожидаемый UUID зафиксированного снимка legacy.")
    ] = None,
) -> None:
    """Загрузить снимок legacy в target и сохранить состояние миграции."""
    options = _load_settings(
        MigrationOptions,
        batch_size=batch_size,
        strategy=strategy,
        policy=policy,
        conflict_policy=conflict_policy,
        transaction_timeout=transaction_timeout,
    )
    settings = _load_settings(Settings)
    with Progress(
        SpinnerColumn(),
        TextColumn("{task.description} — обработано {task.completed:.0f}"),
        console=console,
        disable=not console.is_terminal,
        redirect_stderr=False,
    ) as progress:
        tasks: dict[object, TaskID] = {}

        def observe(event: str, fields: dict[str, object]) -> None:
            _cli_event(event, **fields)
            if event == "migration_started":
                console.print(f"Запуск {fields['run_id']}")
            if event == "migration_batch_committed":
                entity = fields["entity_type"]
                if entity not in tasks:
                    tasks[entity] = progress.add_task(str(entity), total=None)
                progress.update(tasks[entity], advance=int(fields["records_extracted"]))

        try:
            result = asyncio.run(
                MigrationOrchestrator(settings, options, observe).run(
                    expected_snapshot_id=snapshot_id
                )
            )
        except MigrationFailed as exc:
            _cli_event("migration_failed", run_id=str(exc.run_id), audited=exc.audited)
            error_console.print(f"{exc.run_id} — {exc}", style="red", markup=False)
            if not exc.audited:
                error_console.print("Запись ошибки в target недоступна.", style="red")
            raise typer.Exit(code=1) from exc
        except (
            LegacySourceError,
            DatabaseSchemaError,
            OSError,
            SQLAlchemyError,
            PostgresError,
            InterfaceError,
        ) as exc:
            _pipeline_startup_error(exc)
    _print_migration_state(result)


@app.command()
def status(
    run_id: Annotated[UUID | None, typer.Option(help="UUID запуска миграции.")] = None,
) -> None:
    """Показать запуск, счётчики и подтверждённые контрольные точки."""
    settings = _load_settings(Settings)
    try:
        result = asyncio.run(migration_status(settings, run_id))
    except (DatabaseSchemaError, OSError, SQLAlchemyError, PostgresError, InterfaceError) as exc:
        _pipeline_startup_error(exc)
    if result is None:
        if run_id is None:
            console.print("Запусков миграции пока нет.")
            return
        error_console.print(f"Запуск {run_id} не найден.", style="red")
        raise typer.Exit(code=1)
    _print_migration_state(result)
    _cli_event(
        "migration_status",
        **run_fields(result),
        checkpoints=[
            {
                "entity_type": item.entity.value,
                "last_processed_id": item.last_id,
                "records_processed": item.processed,
                "next_batch": item.next_batch,
            }
            for item in result.checkpoints
        ],
    )


def _pipeline_startup_error(error: Exception) -> None:
    message = (
        str(error)
        if isinstance(error, (LegacySourceError, DatabaseSchemaError))
        else _database_error(error)
    )
    _cli_event("migration_unavailable", reason=type(error).__name__)
    error_console.print(message, style="red", markup=False)
    raise typer.Exit(code=1) from error


def _print_migration_state(state: RunState) -> None:
    console.print(f"Запуск {state.id}")
    console.print(f"Снимок {state.source_snapshot_id}")
    table = Table("Статус", "Обработано", "Вставлено", "Обновлено", "Без изменений", "Отклонено")
    table.add_row(
        state.status,
        str(state.records_processed),
        str(state.records_inserted),
        str(state.records_updated),
        str(state.records_unchanged),
        str(state.records_rejected),
    )
    console.print(table)
    if state.checkpoints:
        checkpoints = Table("Сущность", "Последний ID", "Обработано", "Следующая партия")
        for item in state.checkpoints:
            checkpoints.add_row(
                item.entity.value, str(item.last_id), str(item.processed), str(item.next_batch)
            )
        console.print(checkpoints)
    if state.error_message:
        console.print(Text(state.error_message, style="red"))


class ReportFileError(RuntimeError):
    """Ошибка создания диагностического отчёта."""


@contextmanager
def _report_file(path: Path | None) -> Iterator[IO[str] | None]:
    if path is None:
        yield None
        return
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError as exc:
        raise ReportFileError("Файл отчёта уже существует. Выберите новое имя.") from exc
    except OSError as exc:
        raise ReportFileError("Не удалось создать файл отчёта.") from exc
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", buffering=1) as report:
            yield report
    except BaseException:
        path.unlink(missing_ok=True)
        raise


def _write_report(report: IO[str] | None, entry: ReportEntry) -> None:
    if report is not None:
        try:
            print(json.dumps(entry, ensure_ascii=False), file=report)
        except OSError as exc:
            raise ReportFileError("Ошибка записи в файл отчёта.") from exc


def _cli_event(event: str, **fields: object) -> None:
    print(json.dumps({"event": event, **fields}, ensure_ascii=False), file=sys.stderr)


def _load_settings[SettingsType: BaseModel](
    settings_type: type[SettingsType], **values: object
) -> SettingsType:
    try:
        return settings_type(**values)
    except ValidationError as exc:
        error_console.print("Ошибка конфигурации", style="red")
        for error in exc.errors(include_input=False, include_context=False, include_url=False):
            field = ".".join(map(str, error["loc"])) or "Настройки подключений"
            error_console.print(f"{field} — {error['msg']}", markup=False)
        raise typer.Exit(code=2) from exc
    except SettingsError as exc:
        error_console.print("Не удалось прочитать настройки из окружения или .env.", style="red")
        raise typer.Exit(code=2) from exc


async def _check_databases(settings: Settings) -> list[tuple[bool, str]]:
    return await asyncio.gather(
        _check_database(settings.legacy_db, settings.db_timeout_seconds),
        _check_database(settings.target_db, settings.db_timeout_seconds),
    )


async def _check_database(database: DatabaseSettings, timeout: float) -> tuple[bool, str]:
    try:
        version = await read_server_version(database, timeout)
    except (OSError, SQLAlchemyError, PostgresError, InterfaceError) as exc:
        return False, _database_error(exc)

    major, minor = divmod(version, 10_000)
    if major != POSTGRES_MAJOR:
        return False, f"PostgreSQL {major}.{minor}. Требуется версия {POSTGRES_MAJOR}."
    return True, f"PostgreSQL {major}.{minor} — доступна"


def _database_error(error: Exception) -> str:
    cause = error.orig if isinstance(error, DBAPIError) else error
    sqlstate = getattr(cause, "sqlstate", None)
    if sqlstate in {"28000", "28P01"}:
        return "Ошибка аутентификации PostgreSQL"
    if sqlstate == "42501":
        return "У роли PostgreSQL недостаточно прав"
    if sqlstate:
        return f"Ошибка PostgreSQL (SQLSTATE {sqlstate})"
    if isinstance(error, CommandError):
        return "Ошибка Alembic. Проверьте версию PostgreSQL и текущую ревизию базы."
    return f"Ошибка подключения ({type(cause).__name__})"
