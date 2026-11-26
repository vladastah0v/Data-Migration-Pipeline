import asyncio
import errno
import socket
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy.exc import DBAPIError

DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY = 0.25
MAX_RETRY_DELAY = 10.0

_CONNECTION_CODES = {"08000", "08001", "08003", "08006", "08007", "57P01", "57P02", "57P03"}
_TRANSIENT_CODES = _CONNECTION_CODES | {"40001", "40P01", "55P03", "53300"}
_NETWORK_ERRNOS = {
    errno.ECONNABORTED,
    errno.ECONNREFUSED,
    errno.ECONNRESET,
    errno.EHOSTDOWN,
    errno.EHOSTUNREACH,
    errno.ENETUNREACH,
    errno.EPIPE,
    errno.ETIMEDOUT,
}


@dataclass(frozen=True, slots=True)
class RetryEntry:
    id: UUID
    operation: str
    entity_type: str | None
    batch_number: int | None
    attempt: int
    reason: str


def _cause(error: BaseException) -> BaseException:
    return error.orig if isinstance(error, DBAPIError) else error


def sqlstate(error: BaseException) -> str | None:
    cause = _cause(error)
    return getattr(cause, "sqlstate", None)


def is_connection_failure(error: BaseException) -> bool:
    if isinstance(error, DBAPIError) and error.connection_invalidated:
        return True
    cause = _cause(error)
    return (
        sqlstate(error) in _CONNECTION_CODES
        or isinstance(cause, ConnectionError)
        or isinstance(cause, socket.gaierror)
        and cause.errno == socket.EAI_AGAIN
        or isinstance(cause, OSError)
        and cause.errno in _NETWORK_ERRNOS
    )


def is_transient(error: BaseException) -> bool:
    return (
        isinstance(_cause(error), TimeoutError)
        or sqlstate(error) in _TRANSIENT_CODES
        or is_connection_failure(error)
    )


async def with_retry[T](
    operation: Callable[[], Awaitable[T]],
    *,
    max_retries: int,
    delay: float,
    on_retry: Callable[[Exception, int, float], Awaitable[None]],
) -> T:
    attempt = 0
    pause = min(delay, MAX_RETRY_DELAY)
    while True:
        try:
            return await operation()
        except Exception as error:
            if attempt >= max_retries or not is_transient(error):
                raise
            attempt += 1
            await on_retry(error, attempt, pause)
        await asyncio.sleep(pause)
        pause = min(pause * 2, MAX_RETRY_DELAY)
