"""Jobs the API starts (ADR-0014), and each upload's load job.

Revision ID: 0011
Revises: 0010
Create Date: 2026-11-15
"""

import sqlalchemy as sa
from alembic import op

from src.db import schema_for

revision = "0011"
down_revision = "0010"
branch_labels = None
depends_on = None

STATUSES = ("queued", "running", "succeeded", "failed", "cancelled")


def upgrade() -> None:
    schema = schema_for(op.get_bind())
    projects = f"{schema}.projects" if schema else "projects"
    op.create_table(
        "jobs",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("kind", sa.String(32), nullable=False),
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("created_by", sa.String(255), nullable=False),
        sa.Column(
            "status",
            sa.Enum(*STATUSES, name="jobstatus", native_enum=False, length=16),
            nullable=False,
        ),
        sa.Column("k8s_name", sa.String(128)),
        sa.Column("grant_hash", sa.String(64), nullable=False, unique=True),
        sa.Column("grant_expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("refresh_token", sa.Text()),
        sa.Column("error", sa.Text()),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.ForeignKeyConstraint(["project_id"], [f"{projects}.id"], ondelete="CASCADE"),
        schema=schema,
    )
    with op.batch_alter_table("uploads", schema=schema) as batch:
        batch.add_column(sa.Column("job_id", sa.Uuid()))
        batch.create_foreign_key(
            "fk_uploads_job_id", "jobs", ["job_id"], ["id"],
            referent_schema=schema, ondelete="SET NULL",
        )  # fmt: skip


def downgrade() -> None:
    schema = schema_for(op.get_bind())
    with op.batch_alter_table("uploads", schema=schema) as batch:
        batch.drop_constraint("fk_uploads_job_id", type_="foreignkey")
        batch.drop_column("job_id")
    op.drop_table("jobs", schema=schema)
