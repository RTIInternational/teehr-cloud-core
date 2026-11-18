"""Lineage for every write through the API: who, when, how, how many rows, and
the Iceberg snapshot it produced.

Revision ID: 0007
Revises: 0006
Create Date: 2026-11-15
"""

import sqlalchemy as sa
from alembic import op

from src.db import schema_for

revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    schema = schema_for(op.get_bind())
    datasets = f"{schema}.datasets" if schema else "datasets"
    op.create_table(
        "dataset_writes",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("dataset_id", sa.Uuid(), nullable=False),
        sa.Column("mode", sa.String(16), nullable=False),
        sa.Column("filter", sa.Text()),
        sa.Column("rows", sa.BigInteger(), nullable=False),
        sa.Column("snapshot_id", sa.BigInteger()),
        sa.Column("written_by", sa.String(255), nullable=False),
        sa.Column(
            "written_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["dataset_id"], [f"{datasets}.id"], ondelete="CASCADE"),
        schema=schema,
    )
    op.create_index(
        "ix_dataset_writes_dataset_written_at",
        "dataset_writes",
        ["dataset_id", "written_at"],
        schema=schema,
    )


def downgrade() -> None:
    schema = schema_for(op.get_bind())
    op.drop_index(
        "ix_dataset_writes_dataset_written_at", "dataset_writes", schema=schema
    )
    op.drop_table("dataset_writes", schema=schema)
