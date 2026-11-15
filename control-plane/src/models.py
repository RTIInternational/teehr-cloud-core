"""Control-plane tables (all in the ``control_plane`` schema)."""

import uuid
from datetime import UTC, datetime
from enum import StrEnum

from sqlalchemy import (
    JSON,
    BigInteger,
    DateTime,
    Double,
    Enum,
    ForeignKey,
    Index,
    String,
    Text,
    UniqueConstraint,
    Uuid,
    func,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.db import SCHEMA, Base


class ProjectKind(StrEnum):
    SYSTEM = "system"  # public, platform (ADR-0001); provisioned at bootstrap
    TENANT = "tenant"  # created through the API; members only


class ProjectStatus(StrEnum):
    PENDING = "pending"  # recorded, namespace/storage not provisioned yet
    READY = "ready"
    FAILED = "failed"


class Role(StrEnum):
    READ = "read"
    WRITE = "write"
    ADMIN = "admin"

    @property
    def rank(self) -> int:
        return {Role.READ: 1, Role.WRITE: 2, Role.ADMIN: 3}[self]


def _enum(enum_cls: type[StrEnum]) -> Enum:
    # VARCHAR + CHECK rather than a native Postgres enum: portable to SQLite and
    # easy to extend in a migration.
    return Enum(
        enum_cls,
        native_enum=False,
        length=16,
        values_callable=lambda e: [m.value for m in e],
        validate_strings=True,
    )


def _fk(table: str) -> ForeignKey:
    return ForeignKey(f"{SCHEMA}.{table}.id", ondelete="CASCADE")


class Project(Base):
    __tablename__ = "projects"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(60), unique=True)
    display_name: Mapped[str | None] = mapped_column(String(200))
    description: Mapped[str | None] = mapped_column(Text)
    kind: Mapped[ProjectKind] = mapped_column(
        _enum(ProjectKind), default=ProjectKind.TENANT
    )
    status: Mapped[ProjectStatus] = mapped_column(
        _enum(ProjectStatus), default=ProjectStatus.PENDING
    )
    # Why provisioning last failed (None when pending or ready).
    status_detail: Mapped[str | None] = mapped_column(Text)
    # The project's storage prefix (its namespace location), once provisioned.
    storage_location: Mapped[str | None] = mapped_column(Text)
    created_by: Mapped[str] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    members: Mapped[list["Membership"]] = relationship(
        back_populates="project", cascade="all, delete-orphan", passive_deletes=True
    )


class Membership(Base):
    __tablename__ = "memberships"

    project_id: Mapped[uuid.UUID] = mapped_column(_fk("projects"), primary_key=True)
    # Keycloak `sub` of the member.
    subject: Mapped[str] = mapped_column(String(255), primary_key=True)
    username: Mapped[str | None] = mapped_column(String(255))
    role: Mapped[Role] = mapped_column(_enum(Role))
    added_by: Mapped[str] = mapped_column(String(255))
    added_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    project: Mapped[Project] = relationship(back_populates="members")


class Dataset(Base):
    """A project's registered dataset: one Iceberg table in the project namespace,
    with its declared columns (the schema writes are checked against)."""

    __tablename__ = "datasets"
    __table_args__ = (UniqueConstraint("project_id", "name"),)

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    project_id: Mapped[uuid.UUID] = mapped_column(_fk("projects"))
    name: Mapped[str] = mapped_column(String(60))
    kind: Mapped[str] = mapped_column(String(32))
    description: Mapped[str | None] = mapped_column(Text)
    # [{"name", "type", "required", "doc"}], as declared at registration.
    columns: Mapped[list[dict] | None] = mapped_column(JSON)
    # Columns that identify a row (Iceberg identifier fields); used by upserts.
    key_columns: Mapped[list[str] | None] = mapped_column(JSON)
    # Typed datasets: their dataset-level metadata (role, variable, unit, ...),
    # also mirrored into the Iceberg table properties.
    properties: Mapped[dict | None] = mapped_column(JSON)
    table_identifier: Mapped[str | None] = mapped_column(String(200))
    created_by: Mapped[str | None] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class DatasetWrite(Base):
    """Lineage for one write through the API."""

    __tablename__ = "dataset_writes"
    __table_args__ = (
        Index("ix_dataset_writes_dataset_written_at", "dataset_id", "written_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    dataset_id: Mapped[uuid.UUID] = mapped_column(_fk("datasets"))
    mode: Mapped[str] = mapped_column(String(16))
    filter: Mapped[str | None] = mapped_column(Text)
    rows: Mapped[int] = mapped_column(BigInteger)
    snapshot_id: Mapped[int | None] = mapped_column(BigInteger)
    written_by: Mapped[str] = mapped_column(String(255))
    # Set here (microseconds) so quick successive writes keep their order.
    written_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
    )


# --- domains ----------------------------------------------------------------
#
# Units, periods and variables a dataset's metadata must name. A row with no
# project is public (managed by platform admins); a project's own rows are added
# by its writers. A name is defined once across public and a project's rows, so
# it means the same thing everywhere it's visible.


def _public_unique(table: str, column: str) -> Index:
    """Public names are unique (a plain unique constraint lets NULL projects
    repeat)."""
    return Index(
        f"uq_{table}_public_{column}",
        column,
        unique=True,
        postgresql_where=text("project_id IS NULL"),
        sqlite_where=text("project_id IS NULL"),
    )


class Unit(Base):
    """A UCUM unit and its affine conversion to SI: ``si = value * to_si + offset``
    (ADR-0005)."""

    __tablename__ = "units"
    __table_args__ = (
        UniqueConstraint("project_id", "symbol"),
        _public_unique("units", "symbol"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    project_id: Mapped[uuid.UUID | None] = mapped_column(_fk("projects"))
    symbol: Mapped[str] = mapped_column(String(64))
    dimension: Mapped[str] = mapped_column(String(64))
    to_si: Mapped[float] = mapped_column(Double)
    offset: Mapped[float] = mapped_column(Double, default=0.0)
    description: Mapped[str | None] = mapped_column(Text)


class Period(Base):
    """The span of time a value covers (ADR-0012); a row fills only the columns
    its form uses."""

    __tablename__ = "periods"
    __table_args__ = (
        UniqueConstraint("project_id", "name"),
        _public_unique("periods", "name"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    project_id: Mapped[uuid.UUID | None] = mapped_column(_fk("projects"))
    name: Mapped[str] = mapped_column(String(60))
    form: Mapped[str] = mapped_column(String(16))
    length: Mapped[str | None] = mapped_column(String(32))  # ISO 8601 duration
    value_time_at: Mapped[str | None] = mapped_column(String(8))  # start | end
    # --MM-DD, inclusive ("start"/"end" are SQL keywords, hence the names).
    start: Mapped[str | None] = mapped_column("span_start", String(8))
    end: Mapped[str | None] = mapped_column("span_end", String(8))
    description: Mapped[str | None] = mapped_column(Text)


class Variable(Base):
    """What is measured; values of it are stored in its canonical unit."""

    __tablename__ = "variables"
    __table_args__ = (
        UniqueConstraint("project_id", "name"),
        _public_unique("variables", "name"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    project_id: Mapped[uuid.UUID | None] = mapped_column(_fk("projects"))
    name: Mapped[str] = mapped_column(String(60))
    canonical_unit: Mapped[str] = mapped_column(String(64))
    description: Mapped[str | None] = mapped_column(Text)


class Run(Base):
    """Stub: an evaluation run in a project (filled in by later phases)."""

    __tablename__ = "runs"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    project_id: Mapped[uuid.UUID] = mapped_column(_fk("projects"))
    status: Mapped[str] = mapped_column(String(16))
    created_by: Mapped[str] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
