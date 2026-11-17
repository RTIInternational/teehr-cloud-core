"""Runs of evaluation versions, done by jobs (Phase 5).

Revision ID: 0015
Revises: 0014
Create Date: 2026-11-17
"""

import sqlalchemy as sa
from alembic import op

from src.db import schema_for

revision = "0015"
down_revision = "0014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    schema = schema_for(op.get_bind())
    with op.batch_alter_table("runs", schema=schema) as batch:
        batch.add_column(sa.Column("evaluation_version_id", sa.Uuid()))
        batch.add_column(sa.Column("job_id", sa.Uuid()))
        batch.add_column(sa.Column("task", sa.JSON()))
        batch.add_column(sa.Column("result", sa.JSON()))
        batch.add_column(sa.Column("error", sa.Text()))
        batch.add_column(sa.Column("started_at", sa.DateTime(timezone=True)))
        batch.add_column(sa.Column("finished_at", sa.DateTime(timezone=True)))
        batch.create_foreign_key(
            "fk_runs_evaluation_version_id", "evaluation_versions",
            ["evaluation_version_id"], ["id"], referent_schema=schema,
            ondelete="CASCADE",
        )  # fmt: skip
        batch.create_foreign_key(
            "fk_runs_job_id", "jobs", ["job_id"], ["id"],
            referent_schema=schema, ondelete="SET NULL",
        )  # fmt: skip


def downgrade() -> None:
    schema = schema_for(op.get_bind())
    with op.batch_alter_table("runs", schema=schema) as batch:
        batch.drop_constraint("fk_runs_job_id", type_="foreignkey")
        batch.drop_constraint("fk_runs_evaluation_version_id", type_="foreignkey")
        for column in ("finished_at", "started_at", "error", "result", "task",
                       "job_id", "evaluation_version_id"):  # fmt: skip
            batch.drop_column(column)
