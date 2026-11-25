import os

import pytest

from migration_pipeline.config import SchemaSettings


@pytest.fixture(scope="module")
def settings() -> SchemaSettings:
    env_file = os.getenv("MIGRATION_TEST_ENV_FILE")
    if not env_file:
        pytest.skip("Укажите MIGRATION_TEST_ENV_FILE для отдельного тестового стенда PostgreSQL.")
    return SchemaSettings(_env_file=env_file)
