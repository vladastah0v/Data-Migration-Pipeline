from uuid import uuid4

import pytest
from prometheus_client import CollectorRegistry, generate_latest
from prometheus_client.parser import text_string_to_metric_families
from typer.testing import CliRunner

from migration_pipeline import cli
from migration_pipeline.config import Settings
from migration_pipeline.database.legacy import LegacySourceError
from migration_pipeline.monitoring import exporter
from migration_pipeline.monitoring.database import (
    BATCH_BUCKETS,
    BatchHistogram,
    CurrentRun,
    TargetMetrics,
)


@pytest.fixture
def collector(example_env: None) -> exporter.MigrationCollector:
    return exporter.MigrationCollector(Settings())


def _scrape(collector: exporter.MigrationCollector) -> dict[tuple, float]:
    registry = CollectorRegistry()
    registry.register(collector)
    exposition = generate_latest(registry).decode()
    return {
        (sample.name, tuple(sorted(sample.labels.items()))): sample.value
        for metric in text_string_to_metric_families(exposition)
        for sample in metric.samples
    }


def test_exporter_reuses_immutable_progress_and_recovers_from_source_error(
    collector: exporter.MigrationCollector,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    initial = uuid4()
    selected = CurrentRun(initial, "RUNNING", 15, 2.5)
    counted = []
    batch = BatchHistogram(
        "orders", 2, 0.09, tuple(int(limit >= 0.03) + int(limit >= 0.06) for limit in BATCH_BUCKETS)
    )

    async def target(_settings):
        return TargetMetrics(30, 25, 1, 2, 3, (batch,), selected)

    async def source(_settings, snapshot_id):
        counted.append(snapshot_id)
        if snapshot_id != initial:
            raise LegacySourceError("Изменился UUID снимка.")
        return 20

    monkeypatch.setattr(exporter, "load_target_metrics", target)
    monkeypatch.setattr(exporter, "count_snapshot", source)

    values = _scrape(collector)
    assert values["migration_exporter_target_up", ()] == 1
    assert values["migration_records_processed_total", ()] == 30
    assert values["migration_records_inserted_total", ()] == 25
    assert values["migration_records_rejected_total", ()] == 1
    assert values["migration_errors_total", ()] == 2
    assert values["migration_retries_total", ()] == 3
    assert values["migration_progress_percent", ()] == 75
    assert values["migration_progress_available", ()] == 1
    assert values["migration_records_per_second", ()] == 2.5
    labels = (("entity_type", "orders"),)
    assert values["migration_batches_total", labels] == 2
    assert values["migration_batch_duration_seconds_count", labels] == 2
    assert values["migration_batch_duration_seconds_sum", labels] == 0.09
    assert values["migration_batch_duration_seconds_bucket", (*labels, ("le", "0.05"))] == 1
    assert values["migration_batch_duration_seconds_bucket", (*labels, ("le", "+Inf"))] == 2

    selected = CurrentRun(initial, "RUNNING", 16, 1.5)
    assert _scrape(collector)["migration_progress_percent", ()] == 80
    assert counted == [initial]

    changed = uuid4()
    selected = CurrentRun(changed, "RUNNING", 5, 1)
    values = _scrape(collector)
    assert ("migration_progress_percent", ()) not in values
    assert values["migration_progress_available", ()] == 0
    assert values["migration_records_per_second", ()] == 1

    selected = CurrentRun(changed, "LOADED", 40, 4)
    assert _scrape(collector)["migration_progress_percent", ()] == 100
    assert counted == [initial, changed]


def test_exporter_omits_migration_metrics_during_target_outage(
    collector: exporter.MigrationCollector,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connected = True

    async def target(_settings):
        if not connected:
            raise ConnectionRefusedError
        return TargetMetrics(0, 0, 0, 0, 0, (), None)

    monkeypatch.setattr(exporter, "load_target_metrics", target)

    assert _scrape(collector)["migration_records_processed_total", ()] == 0
    connected = False
    assert _scrape(collector) == {("migration_exporter_target_up", ()): 0}
    connected = True
    values = _scrape(collector)
    assert values["migration_exporter_target_up", ()] == 1
    assert values["migration_progress_available", ()] == 0


@pytest.mark.usefixtures("isolated_environment")
def test_exporter_command_validates_http_options() -> None:
    runner = CliRunner()
    assert runner.invoke(cli.app, ["serve-metrics", "--help"]).exit_code == 0
    assert runner.invoke(cli.app, ["serve-metrics", "--port", "65536"]).exit_code == 2
