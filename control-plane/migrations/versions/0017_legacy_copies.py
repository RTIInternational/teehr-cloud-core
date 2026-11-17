"""Copies of legacy warehouse data into public, done by jobs (Phase 7 groundwork).

Revision ID: 0017
Revises: 0016
Create Date: 2026-11-17
"""

import sqlalchemy as sa
from alembic import op

from src.db import schema_for

revision = "0017"
down_revision = "0016"
branch_labels = None
depends_on = None


def upgrade() -> None:
    schema = schema_for(op.get_bind())
    jobs = f"{schema}.jobs" if schema else "jobs"
    op.create_table(
        "legacy_copies",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("job_id", sa.Uuid()),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("request", sa.JSON(), nullable=False),
        sa.Column("result", sa.JSON()),
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
        sa.ForeignKeyConstraint(["job_id"], [f"{jobs}.id"], ondelete="SET NULL"),
        schema=schema,
    )


def downgrade() -> None:
    op.drop_table("legacy_copies", schema=schema_for(op.get_bind()))
