"""Projects, memberships, and dataset/run stubs.

Revision ID: 0002
Revises: 0001
Create Date: 2026-11-13
"""

import sqlalchemy as sa
from alembic import op

from src.db import SCHEMA

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def _enum(name: str, *values: str) -> sa.Enum:
    return sa.Enum(*values, name=name, native_enum=False, length=16)


def _created_at(column: str = "created_at") -> sa.Column:
    return sa.Column(
        column, sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
    )


def _project_fk() -> sa.ForeignKeyConstraint:
    return sa.ForeignKeyConstraint(
        ["project_id"], [f"{SCHEMA}.projects.id"], ondelete="CASCADE"
    )


def upgrade() -> None:
    op.create_table(
        "projects",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("name", sa.String(60), nullable=False, unique=True),
        sa.Column("display_name", sa.String(200)),
        sa.Column("description", sa.Text()),
        sa.Column("kind", _enum("projectkind", "system", "tenant"), nullable=False),
        sa.Column(
            "status",
            _enum("projectstatus", "pending", "ready", "failed"),
            nullable=False,
        ),
        sa.Column("created_by", sa.String(255), nullable=False),
        _created_at(),
        schema=SCHEMA,
    )
    op.create_table(
        "memberships",
        sa.Column("project_id", sa.Uuid(), primary_key=True),
        sa.Column("subject", sa.String(255), primary_key=True),
        sa.Column("username", sa.String(255)),
        sa.Column("role", _enum("role", "read", "write", "admin"), nullable=False),
        sa.Column("added_by", sa.String(255), nullable=False),
        _created_at("added_at"),
        _project_fk(),
        schema=SCHEMA,
    )
    op.create_table(
        "datasets",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(60), nullable=False),
        sa.Column("kind", sa.String(32), nullable=False),
        _created_at(),
        _project_fk(),
        sa.UniqueConstraint("project_id", "name"),
        schema=SCHEMA,
    )
    op.create_table(
        "runs",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("created_by", sa.String(255), nullable=False),
        _created_at(),
        _project_fk(),
        schema=SCHEMA,
    )


def downgrade() -> None:
    for table in ("runs", "datasets", "memberships", "projects"):
        op.drop_table(table, schema=SCHEMA)
