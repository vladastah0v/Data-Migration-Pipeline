import asyncio
import logging
from collections.abc import Iterator
from threading import Lock
from uuid import UUID

from asyncpg import InterfaceError, PostgresError
from prometheus_client import CollectorRegistry, start_http_server
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily, HistogramMetricFamily
from prometheus_client.metrics_core import Metric
from sqlalchemy.exc import SQLAlchemyError

from migration_pipeline.config import Settings
from migration_pipeline.database.legacy import LegacySourceError
from migration_pipeline.extractors.base import LegacyEntity
from migration_pipeline.monitoring.database import (
    BATCH_BUCKETS,
    CurrentRun,
    TargetMetrics,
    count_snapshot,
    load_target_metrics,
)
from migration_pipeline.pipeline.state import COMPLETED_STATES

logger = logging.getLogger(__name__)
_DATABASE_ERRORS = (OSError, SQLAlchemyError, PostgresError, InterfaceError)


class MigrationCollector:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._lock = Lock()
        self._last: tuple[Metric, ...] = ()
        self._snapshot_count: tuple[UUID, int] | None = None

    def collect(self) -> tuple[Metric, ...]:
        if not self._lock.acquire(blocking=False):
            return self._last
        try:
            self._last = asyncio.run(self._collect())
            return self._last
        finally:
            self._lock.release()

    async def _collect(self) -> tuple[Metric, ...]:
        try:
            target = await load_target_metrics(self.settings)
        except _DATABASE_ERRORS as exc:
            logger.debug("Не удалось собрать метрики target (%s).", type(exc).__name__)
            return tuple(_metrics(None, None))
        progress = await self._progress(target.recent)
        return tuple(_metrics(target, progress))

    async def _progress(self, run: CurrentRun | None) -> float | None:
        if run is None:
            return None
        if run.status in COMPLETED_STATES:
            return 100
        if self._snapshot_count is None or self._snapshot_count[0] != run.snapshot_id:
            try:
                count = await count_snapshot(self.settings, run.snapshot_id)
            except (LegacySourceError, *_DATABASE_ERRORS) as exc:
                logger.debug("Не удалось прочитать размер снимка (%s).", type(exc).__name__)
                return None
            self._snapshot_count = (run.snapshot_id, count)
        expected = self._snapshot_count[1]
        return min(100.0, run.processed * 100 / expected) if expected else 0


def _metrics(target: TargetMetrics | None, progress: float | None) -> Iterator[Metric]:
    yield GaugeMetricFamily(
        "migration_exporter_target_up",
        "Target migration metadata is readable.",
        int(target is not None),
    )
    if target is None:
        return

    for attribute, name, help_text in (
        ("processed", "records_processed", "Source records processed in committed batches."),
        ("inserted", "records_inserted", "Source records inserted in committed batches."),
        ("rejected", "records_rejected", "Source records rejected in committed batches."),
        ("errors", "errors", "Entries in the migration load error audit."),
        ("retries", "retries", "Persisted migration retry attempts."),
    ):
        yield CounterMetricFamily(f"migration_{name}", help_text, getattr(target, attribute))

    batch_count = CounterMetricFamily(
        "migration_batches", "Committed batches by source entity.", labels=["entity_type"]
    )
    durations = HistogramMetricFamily(
        "migration_batch_duration_seconds",
        "Elapsed target transaction time of committed batches.",
        labels=["entity_type"],
    )
    observed = {batch.entity: batch for batch in target.batches}
    for entity in LegacyEntity:
        batch = observed.get(entity.value)
        count = batch.count if batch else 0
        values = batch.buckets if batch else (0,) * len(BATCH_BUCKETS)
        buckets = list(zip(map(str, BATCH_BUCKETS), values, strict=True))
        buckets.append(("+Inf", count))
        batch_count.add_metric([entity.value], count)
        durations.add_metric([entity.value], buckets, batch.seconds if batch else 0)
    yield batch_count
    yield durations

    recent = target.recent
    if recent is not None:
        state = GaugeMetricFamily(
            "migration_run_state", "Status of the latest load activity.", labels=["status"]
        )
        state.add_metric([recent.status], 1)
        yield state
    yield GaugeMetricFamily(
        "migration_progress_available",
        "Source size is known for the selected run.",
        int(progress is not None),
    )
    if progress is not None:
        yield GaugeMetricFamily(
            "migration_progress_percent", "Load progress percent for the selected run.", progress
        )
    yield GaugeMetricFamily(
        "migration_records_per_second",
        "Committed source records per second over the last 60 seconds of the selected load.",
        recent.records_per_second if recent is not None else 0,
    )


def serve_metrics(settings: Settings, host: str, port: int) -> None:
    registry = CollectorRegistry()
    registry.register(MigrationCollector(settings))
    server, thread = start_http_server(port, addr=host, registry=registry)
    try:
        thread.join()
    finally:
        server.shutdown()
        server.server_close()
