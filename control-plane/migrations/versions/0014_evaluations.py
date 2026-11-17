"""Saved evaluations and their spec versions (Phase 5).

Revision ID: 0014
Revises: 0013
Create Date: 2026-11-17
"""

import sqlalchemy as sa
from alembic import op

from src.db import schema_for

revision = "0014"
down_revision = "0013"
branch_labels = None
depends_on = None


def upgrade() -> None:
    schema = schema_for(op.get_bind())
    projects = f"{schema}.projects" if schema else "projects"
    evaluations = f"{schema}.evaluations" if schema else "evaluations"
    op.create_table(
        "evaluations",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("slug", sa.String(45), nullable=False),
        sa.Column("created_by", sa.String(255), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["project_id"], [f"{projects}.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("project_id", "slug"),
        schema=schema,
    )
    op.create_table(
        "evaluation_versions",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("evaluation_id", sa.Uuid(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("spec", sa.JSON(), nullable=False),
        sa.Column("spec_hash", sa.String(64), nullable=False),
        sa.Column("created_by", sa.String(255), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["evaluation_id"], [f"{evaluations}.id"], ondelete="CASCADE"
        ),
        sa.UniqueConstraint("evaluation_id", "version"),
        schema=schema,
    )


def downgrade() -> None:
    schema = schema_for(op.get_bind())
    op.drop_table("evaluation_versions", schema=schema)
    op.drop_table("evaluations", schema=schema)
