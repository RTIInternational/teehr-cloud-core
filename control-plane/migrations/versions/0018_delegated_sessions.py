"""Delegated sessions: jobs act as their caller through one (ADR-0018).

Each job's sealed refresh token moves into a session of kind ``job``.

Revision ID: 0018
Revises: 0017
Create Date: 2026-11-19
"""

import uuid
from datetime import UTC, datetime

import sqlalchemy as sa
from alembic import op

from src.db import schema_for

revision = "0018"
down_revision = "0017"
branch_labels = None
depends_on = None


def _tables(schema: str | None):
    """The columns the data moves between, typed (reflection loses UUIDs on
    SQLite)."""
    jobs = sa.table(
        "jobs",
        sa.column("id", sa.Uuid()),
        sa.column("created_by", sa.String()),
        sa.column("refresh_token", sa.Text()),
        sa.column("grant_expires_at", sa.DateTime(timezone=True)),
        sa.column("session_id", sa.Uuid()),
        schema=schema,
    )
    sessions = sa.table(
        "delegated_sessions",
        sa.column("id", sa.Uuid()),
        sa.column("subject", sa.String()),
        sa.column("kind", sa.String()),
        sa.column("refresh_token", sa.Text()),
        sa.column("created_at", sa.DateTime(timezone=True)),
        sa.column("expires_at", sa.DateTime(timezone=True)),
        schema=schema,
    )
    return jobs, sessions


def upgrade() -> None:
    bind = op.get_bind()
    schema = schema_for(bind)
    op.create_table(
        "delegated_sessions",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("subject", sa.String(255), nullable=False),
        sa.Column("kind", sa.String(16), nullable=False),
        sa.Column("refresh_token", sa.Text()),
        sa.Column("access_token", sa.Text()),
        sa.Column("access_expires_at", sa.DateTime(timezone=True)),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True)),
        sa.Column("revoked_at", sa.DateTime(timezone=True)),
        sa.Column("last_used_at", sa.DateTime(timezone=True)),
        schema=schema,
    )
    with op.batch_alter_table("jobs", schema=schema) as batch:
        batch.add_column(sa.Column("session_id", sa.Uuid()))
        batch.create_foreign_key(
            "fk_jobs_session_id", "delegated_sessions", ["session_id"], ["id"],
            referent_schema=schema, ondelete="SET NULL",
        )  # fmt: skip

    jobs, table = _tables(schema)
    held = bind.execute(
        sa.select(jobs.c.id, jobs.c.created_by, jobs.c.refresh_token,
                  jobs.c.grant_expires_at)
        .where(jobs.c.refresh_token.is_not(None))
    ).all()  # fmt: skip
    now = datetime.now(UTC)
    for job_id, subject, refresh, expires in held:
        session_id = uuid.uuid4()
        bind.execute(table.insert().values(
            id=session_id, subject=subject, kind="job", refresh_token=refresh,
            created_at=now, expires_at=expires,
        ))  # fmt: skip
        bind.execute(
            jobs.update().where(jobs.c.id == job_id).values(session_id=session_id)
        )
    with op.batch_alter_table("jobs", schema=schema) as batch:
        batch.drop_column("refresh_token")


def downgrade() -> None:
    bind = op.get_bind()
    schema = schema_for(bind)
    with op.batch_alter_table("jobs", schema=schema) as batch:
        batch.add_column(sa.Column("refresh_token", sa.Text()))
    jobs, table = _tables(schema)
    for job_id, refresh in bind.execute(
        sa.select(jobs.c.id, table.c.refresh_token).join(
            table, table.c.id == jobs.c.session_id
        )
    ).all():
        bind.execute(
            jobs.update().where(jobs.c.id == job_id).values(refresh_token=refresh)
        )
    with op.batch_alter_table("jobs", schema=schema) as batch:
        batch.drop_constraint("fk_jobs_session_id", type_="foreignkey")
        batch.drop_column("session_id")
    op.drop_table("delegated_sessions", schema=schema)
