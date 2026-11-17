"""The months of value_time each timeseries write touched (incremental runs).

Revision ID: 0016
Revises: 0015
Create Date: 2026-11-17
"""

import sqlalchemy as sa
from alembic import op

from src.db import schema_for

revision = "0016"
down_revision = "0015"
branch_labels = None
depends_on = None


def upgrade() -> None:
    schema = schema_for(op.get_bind())
    with op.batch_alter_table("dataset_writes", schema=schema) as batch:
        batch.add_column(sa.Column("months", sa.JSON()))


def downgrade() -> None:
    schema = schema_for(op.get_bind())
    with op.batch_alter_table("dataset_writes", schema=schema) as batch:
        batch.drop_column("months")
