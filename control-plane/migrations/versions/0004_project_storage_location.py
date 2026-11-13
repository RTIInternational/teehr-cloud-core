"""Record each project's storage prefix (its namespace location).

Revision ID: 0004
Revises: 0003
Create Date: 2026-11-13
"""

import sqlalchemy as sa
from alembic import op

from src.db import schema_for

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "projects",
        sa.Column("storage_location", sa.Text()),
        schema=schema_for(op.get_bind()),
    )


def downgrade() -> None:
    with op.batch_alter_table("projects", schema=schema_for(op.get_bind())) as batch:
        batch.drop_column("storage_location")
