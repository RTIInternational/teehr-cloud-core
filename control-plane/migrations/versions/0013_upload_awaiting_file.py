"""Uploads: the first status is `awaiting_file`, as in the shared upload contract
(teehr-shared-eval-spec `standards/uploads.md`), not `staged`.

Revision ID: 0013
Revises: 0012
Create Date: 2026-11-16
"""

import sqlalchemy as sa
from alembic import op

from src.db import schema_for

revision = "0013"
down_revision = "0012"
branch_labels = None
depends_on = None


def _rename(old: str, new: str) -> None:
    schema = schema_for(op.get_bind())
    uploads = f"{schema}.uploads" if schema else "uploads"
    op.execute(
        sa.text(f"UPDATE {uploads} SET status = :new WHERE status = :old").bindparams(
            new=new, old=old
        )
    )


def upgrade() -> None:
    _rename("staged", "awaiting_file")


def downgrade() -> None:
    _rename("awaiting_file", "staged")
