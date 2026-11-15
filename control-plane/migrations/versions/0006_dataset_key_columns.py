"""Datasets may declare key columns: the columns that identify a row (Iceberg
identifier fields), used to match rows when upserting.

Revision ID: 0006
Revises: 0005
Create Date: 2026-11-15
"""

import sqlalchemy as sa
from alembic import op

from src.db import schema_for

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "datasets",
        sa.Column("key_columns", sa.JSON()),
        schema=schema_for(op.get_bind()),
    )


def downgrade() -> None:
    with op.batch_alter_table("datasets", schema=schema_for(op.get_bind())) as batch:
        batch.drop_column("key_columns")
