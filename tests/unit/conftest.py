import os
from pathlib import Path

import pytest

ENV_EXAMPLE = Path(__file__).resolve().parents[2] / ".env.example"


@pytest.fixture
def isolated_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    for name in tuple(os.environ):
        key = name.upper()
        if key in {"LEGACY_DB", "TARGET_DB", "DB_TIMEOUT_SECONDS"} or key.startswith(
            ("LEGACY_DB__", "LEGACY_DB_ADMIN_", "TARGET_DB__", "TARGET_DB_ADMIN_")
        ):
            monkeypatch.delenv(name)
    monkeypatch.chdir(tmp_path)


@pytest.fixture
def example_env(isolated_environment: None, tmp_path: Path) -> None:
    (tmp_path / ".env").write_text(ENV_EXAMPLE.read_text(encoding="utf-8"), encoding="utf-8")
