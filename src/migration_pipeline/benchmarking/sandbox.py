import asyncio
import json
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from secrets import token_hex
from tempfile import TemporaryDirectory
from uuid import uuid4

from pydantic_settings import PydanticBaseSettingsSource
from sqlalchemy import text

from migration_pipeline.config import SchemaSettings
from migration_pipeline.database.connections import create_database_engine
from migration_pipeline.database.schema import lock_schema, upgrade_schema
from migration_pipeline.models.migration import MigrationBase
from migration_pipeline.models.target import TargetBase


class BenchmarkError(RuntimeError):
    """Ошибка подготовки или выполнения эксперимента."""


class _SandboxSettings(SchemaSettings):
    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[SchemaSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (init_settings,)


def _docker_environment() -> dict[str, str]:
    prefixes = ("COMPOSE_", "LEGACY_DB", "TARGET_DB", "MIGRATION_LAB_MODE", "DB_TIMEOUT_SECONDS")
    return {key: value for key, value in os.environ.items() if not key.upper().startswith(prefixes)}


async def _docker(*args: str, timeout: int = 180) -> str:
    try:
        process = await asyncio.create_subprocess_exec(
            "docker",
            *args,
            env=_docker_environment(),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except FileNotFoundError as exc:
        raise BenchmarkError("Для бенчмарка необходим локальный Docker с Compose.") from exc
    try:
        async with asyncio.timeout(timeout):
            output, _ = await process.communicate()
    except (TimeoutError, asyncio.CancelledError) as exc:
        if process.returncode is None:
            with suppress(ProcessLookupError):
                process.kill()
        await process.communicate()
        if isinstance(exc, TimeoutError):
            raise BenchmarkError("Docker не ответил в пределах таймаута.") from exc
        raise
    if process.returncode:
        raise BenchmarkError(f"Команда Docker {args[0]} завершилась с кодом {process.returncode}.")
    return output.decode("utf-8").strip()


async def docker_resources() -> dict[str, object]:
    fields = ("NCPU", "MemTotal", "ServerVersion", "OperatingSystem", "Architecture")
    raw = await _docker("info", "--format", " ".join(f"{{{{json .{key}}}}}\n" for key in fields))
    try:
        return dict(zip(fields, map(json.loads, raw.splitlines()), strict=True))
    except ValueError as exc:
        raise BenchmarkError("Не удалось прочитать ресурсы Docker Engine.") from exc


class BenchmarkSandbox:
    def __init__(self, project: str, arguments: tuple[str, ...], settings: SchemaSettings) -> None:
        self.project = project
        self._arguments = arguments
        self.settings = settings

    async def docker_free_bytes(self) -> int:
        output = await _docker(
            *self._arguments, "exec", "-T", "target-db", "df", "-Pk", "/var/lib/postgresql/data"
        )
        try:
            return int(output.splitlines()[-1].split()[3]) * 1024
        except (IndexError, ValueError) as exc:
            raise BenchmarkError("Не удалось прочитать свободное место тома PostgreSQL.") from exc

    async def reset_target(self) -> None:
        # Only locations and credentials provisioned by open_sandbox reach this operation.
        engine = create_database_engine(
            self.settings.admin_database("target"), self.settings.db_timeout_seconds
        )
        try:
            tables = [
                f'public."{table.name}"'
                for metadata in (TargetBase.metadata, MigrationBase.metadata)
                for table in metadata.tables.values()
            ]
            async with engine.begin() as connection:
                await lock_schema(connection)
                await connection.execute(text(f"TRUNCATE {', '.join(tables)}"))
        finally:
            await engine.dispose()


@asynccontextmanager
async def open_sandbox() -> AsyncIterator[BenchmarkSandbox]:
    root = Path(__file__).resolve().parents[3]
    compose = root / "docker-compose.yml"
    if not compose.is_file():
        raise BenchmarkError(
            "Для запуска benchmark нужен исходный каталог проекта с Docker Compose."
        )
    token = uuid4().hex[:16]
    project = f"dmp-benchmark-{token}"
    with TemporaryDirectory(prefix=project) as temp:
        env_file = Path(temp) / ".env"
        values = {side: _credentials(side, token) for side in ("legacy", "target")}
        with env_file.open("x", encoding="utf-8") as file:
            for side, spec in values.items():
                name = side.upper()
                for env_key, value in (
                    ("DB__PORT", 0),
                    ("DB__NAME", spec["name"]),
                    ("DB__USER", spec["user"]),
                    ("DB__PASSWORD", spec["password"]),
                    ("DB_ADMIN_USER", spec["admin_user"]),
                    ("DB_ADMIN_PASSWORD", spec["admin_password"]),
                ):
                    file.write(f"{name}_{env_key}={value}\n")
        args = (
            "compose",
            "--project-directory",
            str(root),
            "-f",
            str(compose),
            "--env-file",
            str(env_file),
            "--project-name",
            project,
        )
        try:
            await _docker(
                *args, "up", "-d", "--wait", "--wait-timeout", "60", "legacy-db", "target-db"
            )
            ports = {}
            for side in values:
                address = await _docker(*args, "port", f"{side}-db", "5432")
                host, _, port = address.rpartition(":")
                if host != "127.0.0.1" or not port.isdecimal() or not int(port):
                    raise BenchmarkError("Тестовый PostgreSQL должен быть опубликован на loopback.")
                ports[side] = int(port)
            settings = _settings(values, ports)
            for side in ("legacy", "target"):
                await upgrade_schema(settings, side)
            yield BenchmarkSandbox(project, args, settings)
        finally:
            try:
                await _docker(*args, "down", "--volumes", "--rmi", "local", "--timeout", "5")
            except BenchmarkError as exc:
                raise BenchmarkError(
                    f"Не удалось удалить временный Compose-проект {project}."
                ) from exc


def _credentials(side: str, token: str) -> dict[str, str]:
    return {
        "name": f"benchmark_{side}_{token}",
        "user": f"benchmark_{side}_app",
        "password": token_hex(24),
        "admin_user": f"benchmark_{side}_admin",
        "admin_password": token_hex(24),
    }


def _settings(values: dict[str, dict[str, str]], ports: dict[str, int]) -> SchemaSettings:
    fields = {}
    for side, spec in values.items():
        fields[f"{side}_db"] = {
            "host": "127.0.0.1",
            "port": ports[side],
            **{key: spec[key] for key in ("name", "user", "password")},
        }
        fields[f"{side}_db_admin_user"] = spec["admin_user"]
        fields[f"{side}_db_admin_password"] = spec["admin_password"]
    return _SandboxSettings(db_timeout_seconds=20, **fields)
