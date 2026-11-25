import asyncio

import typer
from asyncpg import InterfaceError, PostgresError
from pydantic import ValidationError
from pydantic_settings import SettingsError
from rich.console import Console
from rich.table import Table
from rich.text import Text
from sqlalchemy.exc import DBAPIError, SQLAlchemyError

from migration_pipeline.config import DatabaseSettings, Settings
from migration_pipeline.database.connections import read_server_version

app = typer.Typer(
    name="migration-pipeline",
    help="Пакетная миграция данных между базами PostgreSQL.",
    no_args_is_help=True,
    add_completion=False,
)

POSTGRES_MAJOR = 16
console = Console()
error_console = Console(stderr=True)


@app.callback()
def main() -> None:
    """Управление подключениями и запусками миграции."""


@app.command()
def check() -> None:
    """Проверить конфигурацию и доступность обеих баз PostgreSQL 16."""
    try:
        settings = Settings()
    except ValidationError as exc:
        error_console.print("Ошибка конфигурации", style="red")
        for error in exc.errors(include_input=False, include_context=False, include_url=False):
            field = ".".join(map(str, error["loc"])) or "Настройки подключений"
            error_console.print(f"{field} — {error['msg']}", markup=False)
        raise typer.Exit(code=2) from exc
    except SettingsError as exc:
        error_console.print("Не удалось прочитать настройки из окружения или .env.", style="red")
        raise typer.Exit(code=2) from exc

    databases = (("Legacy", settings.legacy_db), ("Target", settings.target_db))
    results = asyncio.run(_check_databases(settings))
    table = Table("База", "Подключение", "Результат", title="Проверка PostgreSQL")
    for (label, database), (success, message) in zip(databases, results, strict=True):
        address = Text(f"{database.host}:{database.port}/{database.name}")
        table.add_row(label, address, Text(message, style="green" if success else "red"))
    console.print(table)

    if not all(success for success, _ in results):
        raise typer.Exit(code=1)


async def _check_databases(settings: Settings) -> list[tuple[bool, str]]:
    return await asyncio.gather(
        _check_database(settings.legacy_db, settings.db_timeout_seconds),
        _check_database(settings.target_db, settings.db_timeout_seconds),
    )


async def _check_database(database: DatabaseSettings, timeout: float) -> tuple[bool, str]:
    try:
        version = await read_server_version(database, timeout)
    except (OSError, SQLAlchemyError, PostgresError, InterfaceError) as exc:
        cause = exc.orig if isinstance(exc, DBAPIError) else exc
        if getattr(cause, "sqlstate", None) in {"28000", "28P01"}:
            return False, "Ошибка аутентификации PostgreSQL"
        return False, f"Ошибка подключения ({type(cause).__name__})"

    major, minor = divmod(version, 10_000)
    if major != POSTGRES_MAJOR:
        return False, f"PostgreSQL {major}.{minor}. Требуется версия {POSTGRES_MAJOR}."
    return True, f"PostgreSQL {major}.{minor} — доступна"
