"""Uploads: files staged to be loaded into datasets (Phase 3).

Revision ID: 0010
Revises: 0009
Create Date: 2026-11-15
"""

import sqlalchemy as sa
from alembic import op

from src.db import schema_for

revision = "0010"
down_revision = "0009"
branch_labels = None
depends_on = None

STATUSES = (
    "staged",
    "inspected",
    "queued",
    "running",
    "succeeded",
    "failed",
    "cancelled",
)


def upgrade() -> None:
    schema = schema_for(op.get_bind())
    projects = f"{schema}.projects" if schema else "projects"
    op.create_table(
        "uploads",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("filename", sa.String(255), nullable=False),
        sa.Column("format", sa.String(16), nullable=False),
        sa.Column("kind", sa.String(32), nullable=False),
        sa.Column("object_key", sa.String(512), nullable=False),
        sa.Column(
            "status",
            sa.Enum(*STATUSES, name="uploadstatus", native_enum=False, length=16),
            nullable=False,
        ),
        sa.Column("size_bytes", sa.BigInteger()),
        sa.Column("preview", sa.JSON()),
        sa.Column("mapping", sa.JSON()),
        sa.Column("dataset", sa.JSON()),
        sa.Column("rows_done", sa.BigInteger(), nullable=False),
        sa.Column("result", sa.JSON()),
        sa.Column("problems", sa.JSON()),
        sa.Column("error", sa.Text()),
        sa.Column("created_by", sa.String(255), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.ForeignKeyConstraint(["project_id"], [f"{projects}.id"], ondelete="CASCADE"),
        schema=schema,
    )
    op.create_index(
        "ix_uploads_project_created",
        "uploads",
        ["project_id", "created_at"],
        schema=schema,
    )


def downgrade() -> None:
    schema = schema_for(op.get_bind())
    op.drop_index("ix_uploads_project_created", "uploads", schema=schema)
    op.drop_table("uploads", schema=schema)
