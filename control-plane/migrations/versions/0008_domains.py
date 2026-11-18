"""Domains (units, periods, variables) and typed-dataset metadata (#182).

Seeds the public rows: the five shared periods (ADR-0012, fixed in both TEEHR
and VeriRace), a starting set of UCUM units, and a starting set of variables.
Changing public rows later is a new migration.

Revision ID: 0008
Revises: 0007
Create Date: 2026-11-15
"""

import uuid

import sqlalchemy as sa
from alembic import op

from src.db import schema_for

revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None

# symbol, dimension, to_si, offset, description
UNITS = [
    ("1", "dimensionless", 1.0, 0.0, "dimensionless"),
    ("m3/s", "volume_flow", 1.0, 0.0, "cubic meters per second"),
    ("L/s", "volume_flow", 0.001, 0.0, "liters per second"),
    ("[cft_i]/s", "volume_flow", 0.028316846592, 0.0, "cubic feet per second"),
    ("m", "length", 1.0, 0.0, "meters"),
    ("cm", "length", 0.01, 0.0, "centimeters"),
    ("mm", "length", 0.001, 0.0, "millimeters"),
    ("[ft_i]", "length", 0.3048, 0.0, "feet"),
    ("[in_i]", "length", 0.0254, 0.0, "inches"),
    ("m/s", "length_rate", 1.0, 0.0, "meters per second"),
    ("mm/h", "length_rate", 0.001 / 3600, 0.0, "millimeters per hour"),
    ("[in_i]/h", "length_rate", 0.0254 / 3600, 0.0, "inches per hour"),
    ("K", "temperature", 1.0, 0.0, "kelvin"),
    ("Cel", "temperature", 1.0, 273.15, "degrees Celsius"),
    ("[degF]", "temperature", 5 / 9, 273.15 - 32 * 5 / 9, "degrees Fahrenheit"),
    ("m3", "volume", 1.0, 0.0, "cubic meters"),
]

# name, form, length, value_time_at, start, end, description
PERIODS = [
    ("instant", "instant", None, None, None, None, "A value at a single instant"),
    ("hour_starting", "duration", "PT1H", "start", None, None,
     "The hour starting at value_time"),
    ("hour_ending", "duration", "PT1H", "end", None, None,
     "The hour ending at value_time"),
    ("day_starting", "duration", "P1D", "start", None, None,
     "The day starting at value_time"),
    ("day_ending", "duration", "P1D", "end", None, None,
     "The day ending at value_time"),
]  # fmt: skip

# name, canonical_unit, description
VARIABLES = [
    ("streamflow", "m3/s", "Volumetric flow rate in a channel"),
    ("stage", "m", "Water surface height above a gauge datum"),
    ("precipitation", "mm", "Precipitation depth"),
    ("air_temperature", "Cel", "Near-surface air temperature"),
]


def _project_fk(schema):
    projects = f"{schema}.projects" if schema else "projects"
    return sa.ForeignKeyConstraint(
        ["project_id"], [f"{projects}.id"], ondelete="CASCADE"
    )


def _public_index(table: str, column: str, schema) -> None:
    op.create_index(
        f"uq_{table}_public_{column}",
        table,
        [column],
        unique=True,
        schema=schema,
        postgresql_where=sa.text("project_id IS NULL"),
        sqlite_where=sa.text("project_id IS NULL"),
    )


def upgrade() -> None:
    schema = schema_for(op.get_bind())
    op.add_column("datasets", sa.Column("properties", sa.JSON()), schema=schema)

    units = op.create_table(
        "units",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("project_id", sa.Uuid()),
        sa.Column("symbol", sa.String(64), nullable=False),
        sa.Column("dimension", sa.String(64), nullable=False),
        sa.Column("to_si", sa.Double(), nullable=False),
        sa.Column("offset", sa.Double(), nullable=False),
        sa.Column("description", sa.Text()),
        _project_fk(schema),
        sa.UniqueConstraint("project_id", "symbol"),
        schema=schema,
    )
    _public_index("units", "symbol", schema)

    periods = op.create_table(
        "periods",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("project_id", sa.Uuid()),
        sa.Column("name", sa.String(60), nullable=False),
        sa.Column("form", sa.String(16), nullable=False),
        sa.Column("length", sa.String(32)),
        sa.Column("value_time_at", sa.String(8)),
        sa.Column("span_start", sa.String(8)),
        sa.Column("span_end", sa.String(8)),
        sa.Column("description", sa.Text()),
        _project_fk(schema),
        sa.UniqueConstraint("project_id", "name"),
        schema=schema,
    )
    _public_index("periods", "name", schema)

    variables = op.create_table(
        "variables",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("project_id", sa.Uuid()),
        sa.Column("name", sa.String(60), nullable=False),
        sa.Column("canonical_unit", sa.String(64), nullable=False),
        sa.Column("description", sa.Text()),
        _project_fk(schema),
        sa.UniqueConstraint("project_id", "name"),
        schema=schema,
    )
    _public_index("variables", "name", schema)

    op.bulk_insert(
        units,
        [
            dict(zip(("symbol", "dimension", "to_si", "offset", "description"), row,
                     strict=True), id=uuid.uuid4())
            for row in UNITS
        ],
    )  # fmt: skip
    op.bulk_insert(
        periods,
        [
            dict(zip(("name", "form", "length", "value_time_at", "span_start",
                      "span_end", "description"), row, strict=True), id=uuid.uuid4())
            for row in PERIODS
        ],
    )  # fmt: skip
    op.bulk_insert(
        variables,
        [
            dict(zip(("name", "canonical_unit", "description"), row, strict=True),
                 id=uuid.uuid4())
            for row in VARIABLES
        ],
    )  # fmt: skip


def downgrade() -> None:
    schema = schema_for(op.get_bind())
    for table, column in (
        ("variables", "name"),
        ("periods", "name"),
        ("units", "symbol"),
    ):
        op.drop_index(f"uq_{table}_public_{column}", table, schema=schema)
        op.drop_table(table, schema=schema)
    op.drop_column("datasets", "properties", schema=schema)
