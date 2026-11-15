"""Write lineage records the unit values arrived in and the series added (#182).

Revision ID: 0009
Revises: 0008
Create Date: 2026-11-15
"""

import sqlalchemy as sa
from alembic import op

from src.db import schema_for

revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    schema = schema_for(op.get_bind())
    op.add_column("dataset_writes", sa.Column("unit", sa.String(64)), schema=schema)
    op.add_column(
        "dataset_writes", sa.Column("series_added", sa.BigInteger()), schema=schema
    )


def downgrade() -> None:
    schema = schema_for(op.get_bind())
    op.drop_column("dataset_writes", "series_added", schema=schema)
    op.drop_column("dataset_writes", "unit", schema=schema)
