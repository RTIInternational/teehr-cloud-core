"""Personal tokens: a user's bearer tokens, acting through offline sessions
(ADR-0018).

Revision ID: 0019
Revises: 0018
Create Date: 2026-11-19
"""

import sqlalchemy as sa
from alembic import op

from src.db import schema_for

revision = "0019"
down_revision = "0018"
branch_labels = None
depends_on = None


def upgrade() -> None:
    schema = schema_for(op.get_bind())
    sessions = f"{schema}.delegated_sessions" if schema else "delegated_sessions"
    op.create_table(
        "personal_tokens",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("name", sa.String(100), nullable=False),
        sa.Column("subject", sa.String(255), nullable=False),
        sa.Column("username", sa.String(255)),
        sa.Column("token_hash", sa.String(64), nullable=False, unique=True),
        sa.Column("hint", sa.String(16), nullable=False),
        sa.Column("scopes", sa.JSON(), nullable=False),
        sa.Column("projects", sa.JSON()),
        sa.Column("session_id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_used_at", sa.DateTime(timezone=True)),
        sa.Column("revoked_at", sa.DateTime(timezone=True)),
        sa.ForeignKeyConstraint(["session_id"], [f"{sessions}.id"], ondelete="CASCADE"),
        schema=schema,
    )
    op.create_index("ix_personal_tokens_subject", "personal_tokens", ["subject"],
                    schema=schema)  # fmt: skip


def downgrade() -> None:
    schema = schema_for(op.get_bind())
    op.drop_index("ix_personal_tokens_subject", "personal_tokens", schema=schema)
    op.drop_table("personal_tokens", schema=schema)
