import csv
import json
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from typer.testing import CliRunner

from migration_pipeline import cli
from migration_pipeline.benchmarking import service
from migration_pipeline.benchmarking.measurement import (
    BatchMeasurements,
    ProcessMeasurement,
    batch_percentiles,
    rate,
)
from migration_pipeline.benchmarking.report import BATCH_COLUMNS, stage_benchmark_report
from migration_pipeline.benchmarking.sandbox import _credentials, _docker_environment, _settings
from migration_pipeline.benchmarking.service import TrialResult, summarize, trial_plan
from migration_pipeline.loaders.base import LoadStrategy
from migration_pipeline.seeding.data import SeedOptions


def test_plan_rotates_order_keeps_warmup_and_runs_all_strategies() -> None:
    strategies = tuple(LoadStrategy)
    plan = list(trial_plan(strategies, warmups=1, repeats=2))

    assert [entry.execution for entry in plan] == list(range(1, 13))
    assert [entry.strategy for entry in plan[:4]] == list(strategies)
    assert [entry.strategy for entry in plan[4:8]] == [*strategies[1:], strategies[0]]
    assert [entry.strategy for entry in plan[8:]] == [*strategies[2:], *strategies[:2]]
    assert [entry.phase for entry in plan] == ["warmup"] * 4 + ["measured"] * 8


def test_throughput_and_summary_exclude_unverified_and_incomplete_runs() -> None:
    strategy = LoadStrategy.COPY
    plan = list(trial_plan((strategy,), warmups=1, repeats=4))
    results = []
    for trial, duration, processed, status in zip(
        plan,
        (3, 4, 5, 7, 10),
        (100, 100, 50, 100, 100),
        ("SUCCESS", "SUCCESS", "SUCCESS", "FAILED_VERIFICATION", "SUCCESS"),
        strict=True,
    ):
        results.append(
            TrialResult(
                run_id=str(uuid4()),
                trial=trial,
                verified_status=status,
                records_processed=processed,
                records_inserted=processed,
                records_rejected=0,
                retry_count=0,
                duration_seconds=duration,
                records_per_second=rate(processed, duration),
                cpu_seconds=1,
                cpu_percent=15,
                initial_rss_bytes=10,
                peak_rss_bytes=99,
                target_bytes_before=1000,
                target_bytes_after=2000,
                error_rate=0,
                batch_count=5,
                batch_duration_p50_seconds=1,
                batch_duration_p95_seconds=2,
                extract_seconds=1,
                transform_seconds=1,
                load_seconds=1,
                transaction_seconds=2,
            )
        )
    copy, upsert = summarize((strategy, LoadStrategy.UPSERT), results, expected=100)
    assert copy["measured_runs"] == 2
    assert copy["median_duration_seconds"] == 7
    assert copy["median_records_per_second"] == 17.5
    assert copy["peak_rss_bytes"] == 99
    assert upsert["measurement"] == "NOT_MEASURED"
    assert upsert["median_records_per_second"] is None
    assert rate(100, 0) == 0


def test_batch_metrics_keep_unknown_timings_distinct_from_zero() -> None:
    measurements = BatchMeasurements()
    assert batch_percentiles(measurements.durations) == (None, None)
    assert measurements.total("load_duration_seconds") is None
    for duration, load in ((5, 0.0), (3, 0.5), (1, None), (2, 0.5)):
        measurements.add(
            {
                "batch_duration_seconds": duration,
                "extraction_duration_seconds": 0.125,
                "load_duration_seconds": load,
            }
        )
    assert batch_percentiles(measurements.durations) == (2, 5)
    assert measurements.total("extraction_duration_seconds") == 0.5
    assert measurements.total("load_duration_seconds") is None


def test_process_sample_uses_rss_and_cpu_delta(monkeypatch: pytest.MonkeyPatch) -> None:
    rss = iter((100, 175))
    cpu = iter((0.2, 0.5))
    wall = iter((50.0, 50.25))
    process = SimpleNamespace(
        memory_info=lambda: SimpleNamespace(rss=next(rss, 175)),
        cpu_times=lambda: SimpleNamespace(user=next(cpu), system=0.1),
    )
    monkeypatch.setattr(
        "migration_pipeline.benchmarking.measurement.perf_counter", lambda: next(wall)
    )
    sample = ProcessMeasurement(_process=process)
    with sample.measure():
        pass
    assert sample.initial_rss_bytes == 100
    assert sample.peak_rss_bytes == 175
    assert sample.duration_seconds == 0.25
    assert sample.cpu_percent == pytest.approx(120)


def test_compose_discards_ambient_project_and_database_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for key in (
        "COMPOSE_PROJECT_NAME",
        "COMPOSE_FILE",
        "COMPOSE_ENV_FILES",
        "LEGACY_DB__NAME",
        "TARGET_DB__PORT",
        "TARGET_DB_ADMIN_PASSWORD",
        "MIGRATION_LAB_MODE",
    ):
        monkeypatch.setenv(key, "test-override")
        assert key not in _docker_environment()
    monkeypatch.setenv("DOCKER_CONTEXT", "desktop-linux")
    assert _docker_environment()["DOCKER_CONTEXT"] == "desktop-linux"


def test_benchmark_connections_use_only_allocated_ports(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TARGET_DB", "unparseable")
    monkeypatch.setenv("DB_TIMEOUT_SECONDS", "bad")
    monkeypatch.setenv("MIGRATION_LAB_MODE", "true")
    credentials = {side: _credentials(side, "test") for side in ("legacy", "target")}

    settings = _settings(credentials, {"legacy": 55300, "target": 55301})

    assert settings.target_db.port == 55301
    assert settings.legacy_db.name == "benchmark_legacy_test"
    assert not settings.migration_lab_mode
    assert settings.db_timeout_seconds == 20


def test_report_streams_same_batches_to_csv_and_json(tmp_path: Path) -> None:
    identifier = str(uuid4())
    batch = dict.fromkeys(BATCH_COLUMNS) | {
        "execution": 2,
        "strategy": LoadStrategy.COPY,
        "phase": "measured",
        "repeat": 1,
        "records_extracted": 50,
        "batch_duration_seconds": 0.12,
    }
    with stage_benchmark_report(tmp_path) as writer:
        for number in range(1, 81):
            writer.append_batch({**batch, "batch_number": number})
        saved = writer.publish({"benchmark_id": identifier, "runs": [{"execution": 2}]})
    dataset = json.loads((saved / "results.json").read_bytes())
    with (saved / "batches.csv").open(encoding="utf-8", newline="") as file:
        csv_batches = list(csv.DictReader(file))
    with (saved / "runs.csv").open(encoding="utf-8", newline="") as file:
        assert list(csv.DictReader(file)) == [{"execution": "2"}]

    assert len(dataset["batches"]) == len(csv_batches) == 80
    assert [item["batch_number"] for item in dataset["batches"]] == list(range(1, 81))
    assert [int(item["batch_number"]) for item in csv_batches] == list(range(1, 81))
    assert dataset["batches"][0]["load_duration_seconds"] is None
    assert csv_batches[0]["load_duration_seconds"] == ""
    assert list(tmp_path.iterdir()) == [saved]


@pytest.mark.usefixtures("isolated_environment")
def test_benchmark_rejects_single_measured_repeat() -> None:
    runner = CliRunner()
    assert runner.invoke(cli.app, ["benchmark", "--help"]).exit_code == 0
    assert runner.invoke(cli.app, ["benchmark", "--repeats", "1"]).exit_code == 2


@pytest.mark.usefixtures("isolated_environment")
def test_large_benchmark_reports_host_and_docker_capacity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def run_benchmark(seed, *, observer, **options):
        assert seed.profile.value == "large"
        observer(
            "benchmark_resources",
            {
                "host": {
                    "logical_cpus": 8,
                    "available_memory_bytes": 10 * 2**30,
                    "output_disk_free_bytes": 30 * 2**30,
                },
                "docker": {"NCPU": 4, "MemTotal": 6 * 2**30},
                "docker_data_free_bytes": 12 * 2**30,
            },
        )
        return {
            "benchmark_id": str(uuid4()),
            "status": "COMPLETED",
            "failure": None,
            "summary": [],
            "runs": [],
        }

    monkeypatch.setattr(cli, "run_benchmark", run_benchmark)
    result = CliRunner().invoke(cli.app, ["benchmark", "--profile", "large"], terminal_width=130)
    assert result.exit_code == 0
    assert "Large включает 1 000 000 исходных записей" in result.stderr
    assert "Хост — CPU 8" in result.stderr
    assert "Docker — CPU 4, RAM 6.0 ГиБ" in result.stderr


@pytest.mark.asyncio
async def test_benchmark_stops_on_failed_verification_and_keeps_completed_runs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    snapshot = uuid4()
    operations = []
    settings = SimpleNamespace(legacy_db="legacy", target_db="target", db_timeout_seconds=20)
    events = []
    run_ids = []

    async def reset_target():
        operations.append("reset")

    async def constant(*args):
        return 10

    async def pg_info(*args):
        return {"server_version": "16"}

    @asynccontextmanager
    async def open_sandbox():
        yield SimpleNamespace(
            settings=settings, reset_target=reset_target, docker_free_bytes=constant
        )

    async def seed_legacy(*args):
        return SimpleNamespace(
            snapshot_id=snapshot, tables=[SimpleNamespace(name="legacy_customers", rows=10)]
        )

    class Orchestrator:
        def __init__(self, settings, options, observe):
            self.options = options
            self.observe = observe

        async def run(self, *, expected_snapshot_id):
            assert expected_snapshot_id == snapshot
            operations.append(self.options.strategy)
            run_id = uuid4()
            run_ids.append(run_id)
            self.observe(
                "migration_batch_committed",
                {"run_id": str(run_id), "records_extracted": 10, "batch_duration_seconds": 0.05},
            )
            return SimpleNamespace(
                id=run_id, records_processed=10, records_inserted=10, records_rejected=0
            )

    async def verify_run(settings, run_id):
        operations.append("verify")
        assert run_id == run_ids[-1]
        status = "FAILED_VERIFICATION" if len(run_ids) == 3 else "SUCCESS"
        return SimpleNamespace(run=SimpleNamespace(status=status))

    for name, function in {
        "docker_resources": constant,
        "open_sandbox": open_sandbox,
        "_pg_info": pg_info,
        "_database_bytes": constant,
        "seed_legacy": seed_legacy,
        "MigrationOrchestrator": Orchestrator,
        "verify_run": verify_run,
    }.items():
        monkeypatch.setattr(service, name, function)

    report = await service.run_benchmark(
        SeedOptions(),
        strategies=(LoadStrategy.COPY,),
        batch_size=12,
        repeats=3,
        warmups=1,
        report_root=tmp_path,
        observer=lambda event, data: events.append((event, data)),
    )

    assert operations == ["reset", LoadStrategy.COPY, "verify"] * 3
    assert report["status"] == "FAILED"
    assert report["planned_trials"] == 4
    assert len(report["runs"]) == 3
    assert report["summary"][0]["measured_runs"] == 1
    assert report["source_snapshot_id"] == str(snapshot)
    assert [row["run_id"] for event, row in events if event == "benchmark_batch_committed"] == [
        str(run_id) for run_id in run_ids
    ]
