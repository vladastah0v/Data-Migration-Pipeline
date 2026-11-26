import pytest
from pydantic import ValidationError

from migration_pipeline.loaders.base import ConflictPolicy, LoadStrategy
from migration_pipeline.pipeline.orchestrator import MigrationOptions
from migration_pipeline.transformers.errors import ErrorPolicy


def test_configuration_hash_is_reproducible_and_includes_options() -> None:
    default = MigrationOptions()
    assert len(default.fingerprint()) == 64
    assert default.fingerprint() == MigrationOptions(transaction_timeout=60).fingerprint()
    assert default.fingerprint() != MigrationOptions(policy=ErrorPolicy.SKIP).fingerprint()
    assert default.fingerprint() != MigrationOptions(batch_size=500).fingerprint()


@pytest.mark.parametrize(
    "values",
    [
        {"batch_size": 10_001},
        {"transaction_timeout": 0},
        {"transaction_timeout": float("nan")},
        {"strategy": LoadStrategy.INSERT, "conflict_policy": ConflictPolicy.UPDATE},
    ],
)
def test_rejects_invalid_configuration(values: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        MigrationOptions(**values)
