"""Baseline: start the migration chain.

The ``control_plane`` schema itself is created by ``env.py`` (Alembic's version
table lives in it). Tables arrive in later revisions (#152).

Revision ID: 0001
Revises:
Create Date: 2026-11-13
"""

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
