"""Control-plane tables (all in the ``control_plane`` schema)."""

import uuid
from datetime import datetime
from enum import StrEnum

from sqlalchemy import (
    DateTime,
    Enum,
    ForeignKey,
    String,
    Text,
    UniqueConstraint,
    Uuid,
    func,
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
    """Stub: a project's registered dataset (filled in by later phases)."""

    __tablename__ = "datasets"
    __table_args__ = (UniqueConstraint("project_id", "name"),)

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    project_id: Mapped[uuid.UUID] = mapped_column(_fk("projects"))
    name: Mapped[str] = mapped_column(String(60))
    kind: Mapped[str] = mapped_column(String(32))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


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
