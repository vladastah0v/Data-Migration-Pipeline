import pytest
from asyncpg.exceptions import InvalidPasswordError
from typer.testing import CliRunner

from migration_pipeline import cli
from migration_pipeline.config import DatabaseSettings, DatabaseSide, SchemaSettings
from migration_pipeline.seeding.service import SeedError

runner = CliRunner()


@pytest.mark.usefixtures("isolated_environment")
def test_help_is_available_without_settings() -> None:
    assert runner.invoke(cli.app, ["--help"]).exit_code == 0
    assert runner.invoke(cli.app, ["check", "--help"]).exit_code == 0


@pytest.mark.usefixtures("isolated_environment")
def test_check_returns_config_error_with_missing_settings() -> None:
    result = runner.invoke(cli.app, ["check"])

    assert result.exit_code == 2
    assert "Ошибка конфигурации" in result.stderr
    assert "legacy_db" in result.stderr
    assert "target_db" in result.stderr


@pytest.mark.usefixtures("isolated_environment")
def test_check_does_not_show_malformed_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LEGACY_DB", "malformed-with-private-value")

    result = runner.invoke(cli.app, ["check"])

    assert result.exit_code == 2
    assert "Не удалось прочитать настройки" in result.stderr
    assert "malformed-with-private-value" not in result.output


@pytest.mark.usefixtures("example_env")
def test_check_accepts_two_postgres_16_servers(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = []

    async def read_version(database: DatabaseSettings, timeout: float) -> int:
        calls.append((database.name, timeout))
        return 160011

    monkeypatch.setattr(cli, "read_server_version", read_version)

    result = runner.invoke(cli.app, ["check"])

    assert result.exit_code == 0
    assert "PostgreSQL 16.11" in result.stdout
    assert sorted(calls) == [("legacy_crm", 5), ("target_crm", 5)]


@pytest.mark.usefixtures("example_env")
def test_check_reports_an_auth_error_and_incompatible_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def read_version(database: DatabaseSettings, timeout: float) -> int:
        if database.name == "legacy_crm":
            raise InvalidPasswordError("private connection details")
        return 170001

    monkeypatch.setattr(cli, "read_server_version", read_version)

    result = runner.invoke(cli.app, ["check"], terminal_width=120)

    assert result.exit_code == 1
    assert "Ошибка аутентификации PostgreSQL" in result.stdout
    assert "Требуется версия 16" in result.stdout
    assert "private connection details" not in result.output


@pytest.mark.usefixtures("example_env")
def test_init_requires_admin_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TARGET_DB_ADMIN_PASSWORD", "")

    result = runner.invoke(cli.app, ["init"])

    assert result.exit_code == 2
    assert "target_db_admin_password" in result.stderr


@pytest.mark.usefixtures("example_env")
def test_init_applies_both_schemas_in_order(monkeypatch: pytest.MonkeyPatch) -> None:
    applied: list[tuple[DatabaseSide, str]] = []

    async def upgrade_schema(settings: SchemaSettings, side: DatabaseSide) -> None:
        applied.append((side, settings.admin_database(side).user))

    monkeypatch.setattr(cli, "upgrade_schema", upgrade_schema)

    result = runner.invoke(cli.app, ["init"])

    assert result.exit_code == 0
    assert applied == [("legacy", "legacy_admin"), ("target", "target_admin")]


@pytest.mark.usefixtures("example_env")
def test_init_reports_the_failed_schema_without_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def upgrade_schema(settings: SchemaSettings, side: DatabaseSide) -> None:
        if side == "target":
            raise InvalidPasswordError("private administrative details")

    monkeypatch.setattr(cli, "upgrade_schema", upgrade_schema)

    result = runner.invoke(cli.app, ["init"])

    assert result.exit_code == 1
    assert "legacy — схема обновлена" in result.stdout
    assert "target — Ошибка аутентификации PostgreSQL" in result.stderr
    assert "private administrative details" not in result.output


@pytest.mark.usefixtures("isolated_environment")
def test_seed_rejects_invalid_rate_before_reading_database_configuration() -> None:
    result = runner.invoke(cli.app, ["seed", "--invalid-rate", "NaN"])

    assert result.exit_code == 2
    assert "invalid_rate" in result.stderr
    assert "legacy_db" not in result.stderr


@pytest.mark.usefixtures("example_env")
def test_seed_reports_a_sealed_source(monkeypatch: pytest.MonkeyPatch) -> None:
    async def seed_legacy(*_args: object) -> None:
        raise SeedError("Снимок legacy уже зафиксирован.")

    monkeypatch.setattr(cli, "seed_legacy", seed_legacy)

    result = runner.invoke(cli.app, ["seed", "--profile", "small"])

    assert result.exit_code == 1
    assert '"event": "seed_failed"' in result.stderr
    assert "Снимок legacy уже зафиксирован." in result.stderr
