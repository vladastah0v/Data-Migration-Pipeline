import pytest
from pydantic import ValidationError

from migration_pipeline.config import Settings

pytestmark = pytest.mark.usefixtures("example_env")


def test_reads_env_file_and_process_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    password = "local:@%/password"
    monkeypatch.setenv("LEGACY_DB__PASSWORD", password)
    monkeypatch.setenv("LEGACY_DB__PORT", "5432")

    settings = Settings()

    assert settings.legacy_db.port == 5432
    assert settings.target_db.port == 55433
    assert settings.legacy_db.url.drivername == "postgresql+asyncpg"
    assert settings.legacy_db.url.password == password
    assert password not in repr(settings)
    assert password not in str(settings.legacy_db.url)


@pytest.mark.parametrize(
    ("variable", "value", "field"),
    [
        ("LEGACY_DB__PORT", "65536", "legacy_db.port"),
        ("TARGET_DB__PASSWORD", "", "target_db.password"),
        ("DB_TIMEOUT_SECONDS", "0", "db_timeout_seconds"),
    ],
)
def test_rejects_invalid_settings(
    monkeypatch: pytest.MonkeyPatch, variable: str, value: str, field: str
) -> None:
    monkeypatch.setenv(variable, value)

    with pytest.raises(ValidationError, match=field):
        Settings()


def test_rejects_identical_database_addresses(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TARGET_DB__PORT", "55432")
    monkeypatch.setenv("TARGET_DB__NAME", "legacy_crm")

    with pytest.raises(ValidationError, match="укажите разные базы"):
        Settings()
