from datetime import datetime
from uuid import UUID

from sqlalchemy import UUID as UuidType
from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from migration_pipeline.models.base import schema_metadata


class MigrationBase(DeclarativeBase):
    metadata = schema_metadata()


class MigrationRun(MigrationBase):
    __tablename__ = "migration_runs"
    __table_args__ = (
        CheckConstraint(
            "status IN ('RUNNING', 'LOADED', 'SUCCESS', 'SUCCESS_WITH_REJECTIONS', "
            "'FAILED_VERIFICATION', 'FAILED_MIGRATION')",
            name="status",
        ),
        CheckConstraint("(status = 'RUNNING') = (finished_at IS NULL)", name="finished"),
        CheckConstraint("finished_at >= started_at", name="duration"),
        CheckConstraint(
            "(verified_at IS NULL) = (verification_report IS NULL)", name="verification_report"
        ),
        CheckConstraint("records_unchanged >= 0", name="records_unchanged"),
        CheckConstraint("peak_rss_bytes > 0", name="peak_rss_bytes"),
        CheckConstraint("configuration_hash ~ '^[0-9a-f]{64}$'", name="configuration_hash"),
        CheckConstraint(
            "records_processed >= 0 AND records_inserted >= 0 "
            "AND records_updated >= 0 AND records_rejected >= 0",
            name="record_counts",
        ),
        Index("ix_migration_runs_snapshot_started", "source_snapshot_id", "started_at"),
    )

    id: Mapped[UUID] = mapped_column(
        UuidType, primary_key=True, server_default=text("gen_random_uuid()")
    )
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()")
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    verification_report: Mapped[dict[str, object] | None] = mapped_column(JSONB(none_as_null=True))
    status: Mapped[str] = mapped_column(Text, server_default=text("'RUNNING'"))
    configuration_hash: Mapped[str] = mapped_column(Text)
    configuration: Mapped[dict[str, object] | None] = mapped_column(JSONB(none_as_null=True))
    source_snapshot_id: Mapped[UUID] = mapped_column(UuidType)
    records_processed: Mapped[int] = mapped_column(BigInteger, server_default=text("0"))
    records_inserted: Mapped[int] = mapped_column(BigInteger, server_default=text("0"))
    records_updated: Mapped[int] = mapped_column(BigInteger, server_default=text("0"))
    records_unchanged: Mapped[int] = mapped_column(BigInteger, server_default=text("0"))
    records_rejected: Mapped[int] = mapped_column(BigInteger, server_default=text("0"))
    peak_rss_bytes: Mapped[int | None] = mapped_column(BigInteger)
    error_message: Mapped[str | None] = mapped_column(Text)


class MigrationCheckpoint(MigrationBase):
    __tablename__ = "migration_checkpoints"
    __table_args__ = (
        UniqueConstraint("migration_run_id", "entity_type"),
        CheckConstraint("processed_count >= 0", name="processed_count"),
    )

    id: Mapped[UUID] = mapped_column(
        UuidType, primary_key=True, server_default=text("gen_random_uuid()")
    )
    migration_run_id: Mapped[UUID] = mapped_column(ForeignKey("migration_runs.id"))
    entity_type: Mapped[str] = mapped_column(Text)
    last_processed_legacy_id: Mapped[int] = mapped_column(BigInteger)
    processed_count: Mapped[int] = mapped_column(BigInteger)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()")
    )


class MigrationError(MigrationBase):
    __tablename__ = "migration_errors"
    __table_args__ = (
        UniqueConstraint("migration_run_id", "entity_type", "legacy_record_id", "error_code"),
    )

    id: Mapped[UUID] = mapped_column(
        UuidType, primary_key=True, server_default=text("gen_random_uuid()")
    )
    migration_run_id: Mapped[UUID] = mapped_column(ForeignKey("migration_runs.id"))
    entity_type: Mapped[str] = mapped_column(Text)
    legacy_record_id: Mapped[int | None] = mapped_column(BigInteger)
    error_code: Mapped[str] = mapped_column(Text)
    error_message: Mapped[str] = mapped_column(Text)
    is_quarantined: Mapped[bool] = mapped_column(server_default=text("false"))
    is_batch_rejection: Mapped[bool] = mapped_column(server_default=text("true"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()")
    )


class MigrationBatch(MigrationBase):
    __tablename__ = "migration_batches"
    __table_args__ = (
        UniqueConstraint("migration_run_id", "entity_type", "batch_number"),
        CheckConstraint("batch_number > 0", name="number"),
        CheckConstraint("records_count > 0", name="records_count"),
        CheckConstraint("first_legacy_id <= last_legacy_id", name="legacy_id_range"),
        CheckConstraint("status IN ('STARTED', 'COMMITTED', 'FAILED')", name="status"),
        CheckConstraint("(status = 'COMMITTED') = (committed_at IS NOT NULL)", name="committed"),
        CheckConstraint("committed_at >= started_at", name="duration"),
    )

    id: Mapped[UUID] = mapped_column(
        UuidType, primary_key=True, server_default=text("gen_random_uuid()")
    )
    migration_run_id: Mapped[UUID] = mapped_column(ForeignKey("migration_runs.id"))
    entity_type: Mapped[str] = mapped_column(Text)
    batch_number: Mapped[int] = mapped_column(BigInteger)
    first_legacy_id: Mapped[int] = mapped_column(BigInteger)
    last_legacy_id: Mapped[int] = mapped_column(BigInteger)
    records_count: Mapped[int] = mapped_column(BigInteger)
    status: Mapped[str] = mapped_column(Text, server_default=text("'STARTED'"))
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()")
    )
    committed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class MigrationRetry(MigrationBase):
    __tablename__ = "migration_retries"

    id: Mapped[UUID] = mapped_column(UuidType, primary_key=True)
    migration_run_id: Mapped[UUID] = mapped_column(ForeignKey("migration_runs.id"), index=True)
    operation: Mapped[str] = mapped_column(Text)
    entity_type: Mapped[str | None] = mapped_column(Text)
    batch_number: Mapped[int | None] = mapped_column(BigInteger)
    attempt: Mapped[int] = mapped_column(BigInteger)
    reason: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()")
    )
