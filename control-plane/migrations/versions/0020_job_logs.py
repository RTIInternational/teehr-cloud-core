"""Job logs: the lines a job's pods log, kept with the job (runs logs, uploads logs).

Revision ID: 0020
Revises: 0019
Create Date: 2026-11-19
"""

import sqlalchemy as sa
from alembic import op

from src.db import schema_for

revision = "0020"
down_revision = "0019"
branch_labels = None
depends_on = None


def upgrade() -> None:
    schema = schema_for(op.get_bind())
    jobs = f"{schema}.jobs" if schema else "jobs"
    op.create_table(
        "job_logs",
        sa.Column(
            "id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            primary_key=True,
            autoincrement=True,
        ),
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column("at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("level", sa.String(16), nullable=False),
        sa.Column("pod", sa.String(255)),
        sa.Column("message", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(["job_id"], [f"{jobs}.id"], ondelete="CASCADE"),
        schema=schema,
    )
    op.create_index("ix_job_logs_job_id", "job_logs", ["job_id"], schema=schema)


def downgrade() -> None:
    schema = schema_for(op.get_bind())
    op.drop_index("ix_job_logs_job_id", "job_logs", schema=schema)
    op.drop_table("job_logs", schema=schema)
