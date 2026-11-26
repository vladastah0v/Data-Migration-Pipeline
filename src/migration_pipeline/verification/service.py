from collections.abc import Callable, Collection
from dataclasses import asdict, dataclass
from time import perf_counter
from uuid import UUID

from sqlalchemy import func, select, true
from sqlalchemy.ext.asyncio import AsyncConnection

from migration_pipeline.config import Settings
from migration_pipeline.database.legacy import LegacySource, open_legacy_source
from migration_pipeline.database.schema import lock_schema
from migration_pipeline.extractors.base import SOURCE_TABLES, LegacyEntity, extract_batches
from migration_pipeline.models.migration import MigrationBatch, MigrationError
from migration_pipeline.pipeline.checkpoint import Checkpoint
from migration_pipeline.pipeline.orchestrator import target_engine
from migration_pipeline.pipeline.sessions import MigrationSessions
from migration_pipeline.pipeline.state import (
    COMPLETED_STATES,
    MigrationStateError,
    RunState,
    read_run,
    save_verification,
)
from migration_pipeline.transformers.base import PARENT_KEYS, TRANSFORMERS, RawRecord
from migration_pipeline.transformers.errors import FieldError
from migration_pipeline.verification.checksums import CANONICAL_VERSION, BusinessChecksum
from migration_pipeline.verification.finance import FinancialChecks
from migration_pipeline.verification.projection import (
    PROJECTIONS,
    expected_rows,
    foreign_key_violations,
)

type VerificationObserver = Callable[[str, dict[str, object]], None]


@dataclass(slots=True)
class EntityCounts:
    source: int = 0
    matched: int = 0
    rejected: int = 0
    quarantined: int = 0
    rejected_present_in_target: int = 0
    target: int = 0
    missing: int = 0
    unexpected: int = 0
    invalid_source: int = 0
    rejected_parent_references: int = 0
    last_source_id: int | None = None
    committed_records: int = 0
    checkpoint_matches: bool = False


@dataclass(frozen=True, slots=True)
class VerificationResult:
    run: RunState
    report: dict[str, object]


async def verify_run(
    settings: Settings, run_id: UUID, observer: VerificationObserver | None = None
) -> VerificationResult:
    sessions = MigrationSessions(
        target_engine(settings),
        lambda expected: open_legacy_source(settings, expected_snapshot_id=expected),
    )
    try:
        target = await sessions.target()
        await target.execution_options(isolation_level="REPEATABLE READ")
        async with target.begin():
            await lock_schema(target)
            run = await read_run(target, run_id)
            if run is None:
                raise MigrationStateError(f"Запуск {run_id} не найден.")
            if run.status not in COMPLETED_STATES:
                raise MigrationStateError(
                    f"Запуск {run_id} со статусом {run.status} не завершил загрузку."
                )
            sessions.snapshot_id = run.source_snapshot_id
            source = await sessions.source()
            report = await _verify_data(target, source, run, observer)
            status = "FAILED_VERIFICATION"
            if report["passed"]:
                status = "SUCCESS_WITH_REJECTIONS" if run.records_rejected else "SUCCESS"
            saved = await save_verification(target, run.id, status, report)
        return VerificationResult(saved, report)
    finally:
        await sessions.close()


async def _verify_data(
    target: AsyncConnection,
    source: LegacySource,
    run: RunState,
    observer: VerificationObserver | None,
) -> dict[str, object]:
    started = perf_counter()
    finance = FinancialChecks()
    records = {}
    checksums = {}
    checkpoints = {item.entity: item for item in run.checkpoints}
    batches = await _batch_counts(target, run.id)
    rejection_total, invalid_quarantine_flags = await _rejection_audit(target, run)

    source_count = 0
    for entity in LegacyEntity:
        counts, hashes = await _verify_entity(
            target, source, run.id, entity, finance, observer, started, source_count
        )
        _compare_checkpoint(
            counts, checkpoints.get(entity), batches.pop(entity.value, (0, 0, None))
        )
        records[entity.value] = counts
        checksums.update(hashes)
        source_count += counts.source

    rejected_count = sum(counts.rejected for counts in records.values())
    counters_match = (
        source_count == run.records_processed
        and rejected_count == rejection_total == run.records_rejected
        and source_count - rejected_count
        == run.records_inserted + run.records_updated + run.records_unchanged
        and not invalid_quarantine_flags
        and not batches
        and all(counts.checkpoint_matches for counts in records.values())
    )
    complete = (
        all(
            not (
                counts.missing
                or counts.unexpected
                or counts.invalid_source
                or counts.rejected_parent_references
            )
            for counts in records.values()
        )
        and counters_match
    )
    orphans = await foreign_key_violations(target)
    financial = finance.report()
    checks = {
        "record_counts": counters_match
        and all(
            counts.source == counts.matched + counts.rejected
            and counts.target == counts.matched + counts.rejected_present_in_target
            for counts in records.values()
        ),
        "completeness": complete,
        "foreign_keys": not any(orphans.values()),
        "financial": bool(financial["passed"]),
        "checksums": all(entry["matches"] for entry in checksums.values()),
    }
    return {
        "canonical_version": CANONICAL_VERSION,
        "passed": all(checks.values()),
        "checks": checks,
        "records": {entity: asdict(counts) for entity, counts in records.items()},
        "rejection_audit": {
            "recorded": rejection_total,
            "matched_to_source": rejected_count,
            "invalid_quarantine_flags": invalid_quarantine_flags,
        },
        "run_counters_match": counters_match,
        "foreign_key_violations": orphans,
        "financial": financial,
        "checksums": checksums,
        "duration_seconds": round(perf_counter() - started, 6),
    }


async def _verify_entity(
    target: AsyncConnection,
    source: LegacySource,
    run_id: UUID,
    entity: LegacyEntity,
    finance: FinancialChecks,
    observer: VerificationObserver | None,
    started: float,
    checked_before: int,
) -> tuple[EntityCounts, dict[str, dict[str, object]]]:
    projections = PROJECTIONS[entity]
    primary = projections[0].table.table
    key = projections[0].legacy_key
    (source_id,) = SOURCE_TABLES[entity].primary_key.columns
    source_hashes = {projection.table.table.name: BusinessChecksum() for projection in projections}
    target_hashes = {name: BusinessChecksum() for name in source_hashes}
    counts = EntityCounts()

    async for batch in extract_batches(source, entity):
        counts.source += len(batch.rows)
        counts.last_source_id = batch.last_id
        raw = {row[source_id]: row for row in batch.rows}
        rejected = await _rejected_ids(target, run_id, entity, set(raw))
        accepted = {legacy_id: row for legacy_id, row in raw.items() if legacy_id not in rejected}
        counts.rejected += len(rejected)
        counts.quarantined += sum(rejected.values())
        if rejected:
            counts.rejected_present_in_target += await target.scalar(
                select(func.count(key.distinct())).where(key.in_(rejected))
            )
        counts.rejected_parent_references += await _count_rejected_parents(
            target, run_id, entity, accepted.values()
        )
        transformed = []
        for row in accepted.values():
            try:
                transformed.append(TRANSFORMERS[entity](row))
            except FieldError:
                counts.invalid_source += 1
        expected = await expected_rows(source.connection, entity, transformed)
        actual = {
            projection.table.table.name: await projection.read(target, list(accepted))
            for projection in projections
        }
        counts.matched += len({row[0] for row in actual[primary.name]})
        for name in expected:
            source_hashes[name].add(expected[name])
            target_hashes[name].add(actual[name])
        finance.add(expected, actual)
        if observer is not None:
            elapsed = perf_counter() - started
            observer(
                "verification_batch_checked",
                {
                    "run_id": str(run_id),
                    **batch.log_fields(),
                    "entity_records_checked": counts.source,
                    "elapsed_seconds": round(elapsed, 6),
                    "records_per_second": round((checked_before + counts.source) / elapsed, 2),
                },
            )

    counts.target = await target.scalar(select(func.count()).select_from(primary))
    counts.missing = counts.source - counts.rejected - counts.matched
    counts.unexpected = counts.target - counts.matched - counts.rejected_present_in_target
    return counts, {
        name: _compare(source_hashes[name], target_hashes[name]) for name in source_hashes
    }


async def _count_rejected_parents(
    target: AsyncConnection, run_id: UUID, entity: LegacyEntity, rows: Collection[RawRecord]
) -> int:
    parent_field = PARENT_KEYS.get(entity)
    if parent_field is None:
        return 0
    parent = LegacyEntity.CUSTOMERS if entity is LegacyEntity.ORDERS else LegacyEntity.ORDERS
    rejected = await _rejected_ids(target, run_id, parent, {row[parent_field] for row in rows})
    return sum(row[parent_field] in rejected for row in rows)


def _compare_checkpoint(
    counts: EntityCounts, checkpoint: Checkpoint | None, batches: tuple[int, int, int | None]
) -> None:
    counts.committed_records, batch_count, last_id = batches
    if counts.committed_records != counts.source or last_id != counts.last_source_id:
        return
    if counts.source == 0:
        counts.checkpoint_matches = checkpoint is None
    else:
        counts.checkpoint_matches = (
            checkpoint is not None
            and checkpoint.last_id == last_id
            and checkpoint.processed == counts.source
            and checkpoint.next_batch == batch_count + 1
        )


def _compare(source: BusinessChecksum, target: BusinessChecksum) -> dict[str, object]:
    source_hash, target_hash = source.hexdigest(), target.hexdigest()
    return {
        "source_sha256": source_hash,
        "target_sha256": target_hash,
        "source_rows": source.count,
        "target_rows": target.count,
        "matches": source.count == target.count and source_hash == target_hash,
    }


async def _rejected_ids(
    target: AsyncConnection, run_id: UUID, entity: LegacyEntity, ids: set[int]
) -> dict[int, bool]:
    if not ids:
        return {}
    rows = await target.execute(
        select(MigrationError.legacy_record_id, func.bool_or(MigrationError.is_quarantined))
        .where(
            MigrationError.migration_run_id == run_id,
            MigrationError.entity_type == entity.value,
            MigrationError.legacy_record_id.in_(ids),
            MigrationError.is_batch_rejection.is_(True),
        )
        .group_by(MigrationError.legacy_record_id)
    )
    return {legacy_id: quarantined for legacy_id, quarantined in rows}


async def _rejection_audit(target: AsyncConnection, run: RunState) -> tuple[int, int]:
    policy = (run.configuration or {}).get("policy")
    invalid = (
        MigrationError.is_quarantined.is_(policy == "skip")
        if policy in ("skip", "quarantine")
        else true()
    )
    row = (
        await target.execute(
            select(func.count(), func.count().filter(invalid)).where(
                MigrationError.migration_run_id == run.id,
                MigrationError.is_batch_rejection.is_(True),
            )
        )
    ).one()
    return tuple(row)


async def _batch_counts(
    target: AsyncConnection, run_id: UUID
) -> dict[str, tuple[int, int, int | None]]:
    rows = await target.execute(
        select(
            MigrationBatch.entity_type,
            func.sum(MigrationBatch.records_count),
            func.count(),
            func.max(MigrationBatch.last_legacy_id),
        )
        .where(MigrationBatch.migration_run_id == run_id, MigrationBatch.status == "COMMITTED")
        .group_by(MigrationBatch.entity_type)
    )
    return {entity: (int(records), count, last_id) for entity, records, count, last_id in rows}
