from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from migration_pipeline.database.legacy import LegacySource, SourceSnapshot
from migration_pipeline.extractors.base import BIGINT_MAX, BIGINT_MIN, LegacyEntity, extract_batches


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kwargs",
    [
        {"batch_size": 0},
        {"batch_size": 10_001},
        {"after_id": BIGINT_MIN - 1},
        {"after_id": BIGINT_MAX + 1},
        {"start_batch_number": 0},
    ],
)
async def test_rejects_invalid_bounds_before_querying_source(kwargs: dict[str, int]) -> None:
    connection = AsyncMock()
    source = LegacySource(connection, SourceSnapshot(uuid4(), datetime.now(UTC)))

    with pytest.raises(ValueError):
        await anext(extract_batches(source, LegacyEntity.CUSTOMERS, **kwargs))

    connection.execute.assert_not_awaited()
