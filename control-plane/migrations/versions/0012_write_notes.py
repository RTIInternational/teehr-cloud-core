"""Write lineage keeps notes: keys a crosswalk or series row points at that
weren't found (layers and crosswalks, Phase 3).

Revision ID: 0012
Revises: 0011
Create Date: 2026-11-15
"""

import sqlalchemy as sa
from alembic import op

from src.db import schema_for

revision = "0012"
down_revision = "0011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    schema = schema_for(op.get_bind())
    op.add_column("dataset_writes", sa.Column("notes", sa.JSON()), schema=schema)


def downgrade() -> None:
    schema = schema_for(op.get_bind())
    op.drop_column("dataset_writes", "notes", schema=schema)
