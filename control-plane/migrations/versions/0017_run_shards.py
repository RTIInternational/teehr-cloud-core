"""A run's shard queue, which the run's pods work (ADR-0017).

Revision ID: 0017
Revises: 0016
Create Date: 2026-11-19
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
    runs = f"{schema}.runs" if schema else "runs"
    with op.batch_alter_table("runs", schema=schema) as batch:
        batch.add_column(sa.Column("shard_plan", sa.JSON()))
    op.create_table(
        "run_shards",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("index", sa.Integer(), nullable=False),
        sa.Column("keys", sa.JSON()),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("worker", sa.String(255)),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True)),
        sa.Column("outputs", sa.JSON()),
        sa.Column("error", sa.Text()),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.ForeignKeyConstraint(["run_id"], [f"{runs}.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("run_id", "index"),
        schema=schema,
    )


def downgrade() -> None:
    schema = schema_for(op.get_bind())
    op.drop_table("run_shards", schema=schema)
    with op.batch_alter_table("runs", schema=schema) as batch:
        batch.drop_column("shard_plan")
