import asyncio

import typer
from alembic.util import CommandError
from asyncpg import InterfaceError, PostgresError
from pydantic import ValidationError
from pydantic_settings import SettingsError
from rich.console import Console
from rich.table import Table
from rich.text import Text
from sqlalchemy.exc import DBAPIError, SQLAlchemyError

from migration_pipeline.config import DatabaseSettings, DatabaseSide, SchemaSettings, Settings
from migration_pipeline.database.connections import POSTGRES_MAJOR, read_server_version
from migration_pipeline.database.schema import upgrade_schema

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


def _load_settings[SettingsType: Settings](settings_type: type[SettingsType]) -> SettingsType:
    try:
        return settings_type()
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
