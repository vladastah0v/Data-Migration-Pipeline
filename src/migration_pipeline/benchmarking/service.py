import platform
from collections.abc import Callable, Iterator, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from uuid import UUID, uuid4

import psutil
from sqlalchemy import text

from migration_pipeline.benchmarking.measurement import (
    BatchMeasurements,
    ProcessMeasurement,
    batch_percentiles,
    rate,
)
from migration_pipeline.benchmarking.sandbox import BenchmarkSandbox, docker_resources, open_sandbox
from migration_pipeline.config import DatabaseSettings
from migration_pipeline.database.connections import create_database_engine
from migration_pipeline.loaders.base import LoadStrategy
from migration_pipeline.pipeline.orchestrator import (
    MigrationFailed,
    MigrationOptions,
    MigrationOrchestrator,
    migration_status,
)
from migration_pipeline.pipeline.state import RunState
from migration_pipeline.seeding.data import SeedOptions
from migration_pipeline.seeding.service import seed_legacy
from migration_pipeline.verification.service import verify_run

type BenchmarkObserver = Callable[[str, dict[str, object]], None]
BATCH_FIELDS = (
    "entity_type",
    "batch_number",
    "records_extracted",
    "records_inserted",
    "records_updated",
    "records_unchanged",
    "records_rejected",
    "extraction_duration_seconds",
    "transform_duration_seconds",
    "load_duration_seconds",
    "transaction_duration_seconds",
    "batch_duration_seconds",
    "reconciled",
)
PG_CONFIGURATION = (
    "server_version",
    "shared_buffers",
    "work_mem",
    "maintenance_work_mem",
    "effective_cache_size",
    "max_connections",
    "wal_level",
    "max_wal_size",
    "synchronous_commit",
    "fsync",
    "full_page_writes",
    "jit",
)


@dataclass(frozen=True, slots=True)
class Trial:
    execution: int
    phase: str
    repeat: int
    strategy: LoadStrategy


@dataclass(slots=True)
class TrialResult:
    run_id: str | None
    trial: Trial
    verified_status: str
    records_processed: int
    records_inserted: int
    records_rejected: int
    retry_count: int
    duration_seconds: float
    records_per_second: float
    cpu_seconds: float
    cpu_percent: float
    initial_rss_bytes: int
    peak_rss_bytes: int
    target_bytes_before: int
    target_bytes_after: int | None
    error_rate: float | None
    batch_count: int
    batch_duration_p50_seconds: float | None
    batch_duration_p95_seconds: float | None
    extract_seconds: float | None
    transform_seconds: float | None
    load_seconds: float | None
    transaction_seconds: float | None


def trial_plan(strategies: Sequence[LoadStrategy], warmups: int, repeats: int) -> Iterator[Trial]:
    execution = 0
    for phase, rounds, offset in (("warmup", warmups, 0), ("measured", repeats, warmups)):
        for index in range(rounds):
            start = (index + offset) % len(strategies)
            for strategy in (*strategies[start:], *strategies[:start]):
                execution += 1
                yield Trial(execution, phase, index + 1, strategy)


async def run_benchmark(
    seed: SeedOptions,
    *,
    strategies: tuple[LoadStrategy, ...],
    batch_size: int,
    repeats: int,
    warmups: int,
    report_root: Path,
    observer: BenchmarkObserver,
) -> dict[str, object]:
    benchmark_id = uuid4()
    started_at = datetime.now(UTC).isoformat()
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage(report_root)
    resources = {
        "host": {
            "system": platform.system(),
            "release": platform.release(),
            "architecture": platform.machine(),
            "python_version": platform.python_version(),
            "logical_cpus": psutil.cpu_count(),
            "physical_cpus": psutil.cpu_count(logical=False),
            "total_memory_bytes": memory.total,
            "available_memory_bytes": memory.available,
            "output_disk_free_bytes": disk.free,
        },
        "docker": await docker_resources(),
    }
    async with open_sandbox() as sandbox:
        resources["docker_data_free_bytes"] = await sandbox.docker_free_bytes()
        observer("benchmark_resources", resources)
        settings = sandbox.settings
        legacy = await _pg_info(settings.legacy_db, settings.db_timeout_seconds)
        target = await _pg_info(settings.target_db, settings.db_timeout_seconds)
        observer("benchmark_seeding", {"profile": seed.profile, "seed": seed.seed})
        seeded = await seed_legacy(settings, seed, lambda *args: None)
        source_rows = {table.name: table.rows for table in seeded.tables}
        expected = sum(source_rows.values())
        observer(
            "benchmark_dataset_ready",
            {"snapshot_id": str(seeded.snapshot_id), "records": expected, "tables": source_rows},
        )

        results = []
        failure = None
        for trial in trial_plan(strategies, warmups, repeats):
            observer("benchmark_trial_started", asdict(trial))
            result = await _run_trial(sandbox, seeded.snapshot_id, trial, batch_size, observer)
            results.append(result)
            observer("benchmark_trial_finished", flatten_trial(result))
            if not _is_comparable(result, expected):
                failure = f"Прогон {trial.execution} не прошёл контроль результата."
                break

    return {
        "format_version": 1,
        "benchmark_id": str(benchmark_id),
        "status": "FAILED" if failure else "COMPLETED",
        "failure": failure,
        "started_at": started_at,
        "finished_at": datetime.now(UTC).isoformat(),
        "profile": seed.profile,
        "seed": seed.seed,
        "source_snapshot_id": str(seeded.snapshot_id),
        "source_records": source_rows,
        "source_total": expected,
        "batch_size": batch_size,
        "warmups_per_strategy": warmups,
        "measured_repeats_per_strategy": repeats,
        "planned_trials": len(strategies) * (warmups + repeats),
        "resources": resources,
        "postgresql": {"legacy": legacy, "target": target},
        "summary": summarize(strategies, results, expected),
        "runs": [flatten_trial(result) for result in results],
    }


async def _run_trial(
    sandbox: BenchmarkSandbox,
    snapshot_id: UUID,
    trial: Trial,
    batch_size: int,
    observer: BenchmarkObserver,
) -> TrialResult:
    await sandbox.reset_target()
    settings = sandbox.settings
    size_before = await _database_bytes(settings.target_db, settings.db_timeout_seconds)
    batches = BatchMeasurements()
    retry_events = 0
    details = asdict(trial)

    def watch(event: str, fields: dict[str, object]) -> None:
        nonlocal retry_events
        if event == "migration_retry":
            retry_events += 1
            observer("benchmark_retry", {**details, **fields})
        if event == "migration_batch_committed":
            batch = {
                **details,
                "run_id": fields["run_id"],
                **{key: fields.get(key) for key in BATCH_FIELDS},
            }
            batches.add(batch)
            observer("benchmark_batch_committed", batch)

    options = MigrationOptions(batch_size=batch_size, strategy=trial.strategy)
    measurement = ProcessMeasurement()
    state: RunState | None = None
    size_after = None
    migration_error = None
    try:
        with measurement.measure():
            state = await MigrationOrchestrator(settings, options, watch).run(
                expected_snapshot_id=snapshot_id
            )
    except MigrationFailed as exc:
        migration_error = exc

    if migration_error is None:
        size_after = await _database_bytes(settings.target_db, settings.db_timeout_seconds)
        status = (await verify_run(settings, state.id)).run.status
    else:
        status = "FAILED_MIGRATION"
        if migration_error.audited:
            state = await migration_status(settings, migration_error.run_id)

    run_id = str(state.id if state is not None else migration_error.run_id)
    processed = state.records_processed if state is not None else 0
    rejected = state.records_rejected if state is not None else 0
    p50, p95 = batch_percentiles(batches.durations)
    return TrialResult(
        run_id=run_id,
        trial=trial,
        verified_status=status,
        records_processed=processed,
        records_inserted=state.records_inserted if state is not None else 0,
        records_rejected=rejected,
        retry_count=retry_events,
        duration_seconds=measurement.duration_seconds,
        records_per_second=rate(processed, measurement.duration_seconds),
        cpu_seconds=measurement.cpu_seconds,
        cpu_percent=measurement.cpu_percent,
        initial_rss_bytes=measurement.initial_rss_bytes,
        peak_rss_bytes=measurement.peak_rss_bytes,
        target_bytes_before=size_before,
        target_bytes_after=size_after,
        error_rate=rejected / processed if processed else None,
        batch_count=len(batches.durations),
        batch_duration_p50_seconds=p50,
        batch_duration_p95_seconds=p95,
        extract_seconds=batches.total("extraction_duration_seconds"),
        transform_seconds=batches.total("transform_duration_seconds"),
        load_seconds=batches.total("load_duration_seconds"),
        transaction_seconds=batches.total("transaction_duration_seconds"),
    )


def flatten_trial(result: TrialResult) -> dict[str, object]:
    row = asdict(result)
    return {**row.pop("trial"), **row}


def summarize(
    strategies: Sequence[LoadStrategy], results: Sequence[TrialResult], expected: int
) -> list[dict[str, object]]:
    summary = []
    for strategy in strategies:
        selected = [
            item
            for item in results
            if item.trial.phase == "measured"
            and item.trial.strategy == strategy
            and _is_comparable(item, expected)
        ]
        summary.append(
            {
                "strategy": strategy,
                "measurement": "MEASURED" if selected else "NOT_MEASURED",
                "measured_runs": len(selected),
                "median_duration_seconds": median(item.duration_seconds for item in selected)
                if selected
                else None,
                "median_records_per_second": median(item.records_per_second for item in selected)
                if selected
                else None,
                "peak_rss_bytes": max((item.peak_rss_bytes for item in selected), default=None),
            }
        )
    return summary


def _is_comparable(result: TrialResult, expected: int) -> bool:
    return result.verified_status == "SUCCESS" and (
        result.records_processed == result.records_inserted == expected
    )


async def _database_bytes(database: DatabaseSettings, timeout: float) -> int:
    engine = create_database_engine(database, timeout)
    try:
        async with engine.connect() as connection:
            return await connection.scalar(text("SELECT pg_database_size(current_database())"))
    finally:
        await engine.dispose()


async def _pg_info(database: DatabaseSettings, timeout: float) -> dict[str, object]:
    engine = create_database_engine(database, timeout)
    try:
        async with engine.connect() as connection:
            rows = await connection.execute(
                text("SELECT name, setting FROM pg_settings WHERE name = ANY(:names)"),
                {"names": list(PG_CONFIGURATION)},
            )
            return {name: setting for name, setting in rows}
    finally:
        await engine.dispose()
