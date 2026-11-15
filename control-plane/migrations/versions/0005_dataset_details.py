"""Datasets get what the registry needs: their declared columns, the Iceberg table
they back onto, who registered them, and a description.

Revision ID: 0005
Revises: 0004
Create Date: 2026-11-15
"""

import sqlalchemy as sa
from alembic import op

from src.db import schema_for

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None

COLUMNS = (
    ("description", sa.Text()),
    ("columns", sa.JSON()),
    ("table_identifier", sa.String(200)),
    ("created_by", sa.String(255)),
)


def upgrade() -> None:
    for name, type_ in COLUMNS:
        op.add_column(
            "datasets", sa.Column(name, type_), schema=schema_for(op.get_bind())
        )


def downgrade() -> None:
    with op.batch_alter_table("datasets", schema=schema_for(op.get_bind())) as batch:
        for name, _ in reversed(COLUMNS):
            batch.drop_column(name)
