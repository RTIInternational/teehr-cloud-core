"""Record why a project's provisioning failed.

Revision ID: 0003
Revises: 0002
Create Date: 2026-11-13
"""

import sqlalchemy as sa
from alembic import op

from src.db import schema_for

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "projects",
        sa.Column("status_detail", sa.Text()),
        schema=schema_for(op.get_bind()),
    )


def downgrade() -> None:
    with op.batch_alter_table("projects", schema=schema_for(op.get_bind())) as batch:
        batch.drop_column("status_detail")
