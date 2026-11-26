import asyncio
import errno
from types import SimpleNamespace

import pytest
from asyncpg import (
    AdminShutdownError,
    DeadlockDetectedError,
    InvalidPasswordError,
    SerializationError,
    UniqueViolationError,
)

from migration_pipeline.pipeline.orchestrator import _configuration_hash, _stored_options
from migration_pipeline.pipeline.retry import is_transient, with_retry
from migration_pipeline.pipeline.state import MigrationStateError


@pytest.mark.parametrize(
    "error",
    [
        TimeoutError(),
        ConnectionRefusedError(),
        AdminShutdownError(),
        DeadlockDetectedError(),
        SerializationError(),
        OSError(errno.ENETUNREACH, ""),
    ],
)
def test_classifies_temporary_errors(error: Exception) -> None:
    assert is_transient(error)


@pytest.mark.parametrize(
    "error",
    [InvalidPasswordError(), UniqueViolationError(), ValueError(), asyncio.CancelledError()],
)
def test_never_retries_permanent_errors(error: BaseException) -> None:
    assert not is_transient(error)


@pytest.mark.asyncio
async def test_bounded_exponential_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    pauses = []
    attempts = []

    async def sleep(delay: float) -> None:
        pauses.append(delay)

    async def operation() -> None:
        raise ConnectionRefusedError

    async def on_retry(_error: Exception, attempt: int, delay: float) -> None:
        attempts.append((attempt, delay))

    monkeypatch.setattr(asyncio, "sleep", sleep)
    with pytest.raises(ConnectionRefusedError):
        await with_retry(operation, max_retries=4, delay=2, on_retry=on_retry)
    assert attempts == [(1, 2), (2, 4), (3, 8), (4, 10)]
    assert pauses == [2, 4, 8, 10]


def test_stored_stage_seven_configuration_uses_retry_defaults() -> None:
    configuration = {
        "batch_size": 1000,
        "strategy": "upsert",
        "policy": "strict",
        "conflict_policy": "verify",
        "transaction_timeout": 60.0,
    }
    saved = SimpleNamespace(
        configuration=configuration, configuration_hash=_configuration_hash(configuration)
    )
    assert _stored_options(saved).max_retries == 3
    configuration["batch_size"] = 500
    with pytest.raises(MigrationStateError, match="Хеш"):
        _stored_options(saved)
