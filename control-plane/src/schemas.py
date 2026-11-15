"""Request and response bodies - the API contract.

These models generate the published OpenAPI spec (``control-plane/openapi.json``,
pinned by a snapshot test), which is what a CLI or SDK is built from (ADR-0007,
ADR-0011). Inputs are strict: an unknown field is an error, never silently dropped.
"""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.models import ProjectKind, ProjectStatus, Role

# Same rule as spec dataset references; also a valid Polaris namespace, S3 prefix
# and Keycloak role-name fragment.
PROJECT_NAME_PATTERN = r"^[a-z][a-z0-9_]{0,59}$"

# public / platform are the system projects (ADR-0001); teehr is today's live
# namespace, which a new project must not collide with.
RESERVED_PROJECT_NAMES = frozenset({"public", "platform", "teehr"})


class StrictInput(BaseModel):
    """Base for request bodies: reject fields the API doesn't know."""

    model_config = ConfigDict(extra="forbid")


class ProjectCreate(StrictInput):
    name: str = Field(pattern=PROJECT_NAME_PATTERN, description="Unique project name")
    display_name: str | None = Field(default=None, max_length=200)
    description: str | None = Field(default=None, max_length=2000)


class ProjectUpdate(StrictInput):
    display_name: str | None = Field(default=None, max_length=200)
    description: str | None = Field(default=None, max_length=2000)


class ProjectOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    name: str
    display_name: str | None
    description: str | None
    kind: ProjectKind
    status: ProjectStatus
    status_detail: str | None = Field(
        default=None, description="Why provisioning last failed"
    )
    storage_location: str | None = Field(
        default=None, description="The project's storage prefix"
    )
    created_by: str
    created_at: datetime
    my_role: Role | None = Field(
        default=None,
        description=(
            "The caller's role (null if not a member: a platform admin, or anyone "
            "reading a system project)"
        ),
    )


class MemberAdd(StrictInput):
    username: str = Field(min_length=1, max_length=255, description="Keycloak username")
    role: Role


class MemberPut(StrictInput):
    role: Role
    username: str | None = Field(
        default=None,
        max_length=255,
        description="Ignored when Keycloak is configured (taken from Keycloak)",
    )


class MemberOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    subject: str = Field(description="Keycloak user id")
    username: str | None
    role: Role
    added_by: str
    added_at: datetime


class ErrorOut(BaseModel):
    """Every error response (4xx/5xx other than 422) has this shape."""

    detail: str
    code: str | None = Field(
        default=None,
        description=(
            "Machine-readable reason where a client can act on it, e.g. "
            "`token_refresh_required`"
        ),
    )


# --- datasets ---------------------------------------------------------------

COLUMN_NAME_PATTERN = r"^[A-Za-z_][A-Za-z0-9_]{0,127}$"
ColumnType = Literal[
    "string",
    "int32",
    "int64",
    "float32",
    "float64",
    "boolean",
    "date",
    "timestamp",
    "timestamptz",
    "binary",
]


class DatasetColumn(StrictInput):
    name: str = Field(pattern=COLUMN_NAME_PATTERN)
    type: ColumnType
    required: bool = Field(default=False, description="Values may not be null")
    doc: str | None = Field(default=None, max_length=1000)


class DatasetCreate(StrictInput):
    name: str = Field(
        pattern=PROJECT_NAME_PATTERN, description="Unique within the project"
    )
    description: str | None = Field(default=None, max_length=2000)
    columns: list[DatasetColumn] = Field(min_length=1, max_length=500)
    key_columns: list[str] | None = Field(
        default=None,
        max_length=10,
        description=(
            "Columns that identify a row (e.g. location_id, value_time); used to "
            "match rows when upserting. Each must be a declared, required column "
            "and not a float."
        ),
    )

    @field_validator("columns")
    @classmethod
    def _unique_column_names(cls, columns: list[DatasetColumn]):
        seen: set[str] = set()
        for col in columns:
            key = col.name.lower()
            if key in seen:
                raise ValueError(f"duplicate column name '{col.name}'")
            seen.add(key)
        return columns

    @model_validator(mode="after")
    def _valid_key_columns(self):
        if self.key_columns is None:
            return self
        if len(set(self.key_columns)) != len(self.key_columns):
            raise ValueError("key_columns has duplicates")
        declared = {col.name: col for col in self.columns}
        for name in self.key_columns:
            col = declared.get(name)
            if col is None:
                raise ValueError(f"key column '{name}' is not a declared column")
            if not col.required:
                raise ValueError(f"key column '{name}' must be required")
            if col.type in ("float32", "float64"):
                raise ValueError(f"key column '{name}' can't be a float")
        return self


class DatasetOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    name: str
    kind: str
    description: str | None
    columns: list[DatasetColumn]
    key_columns: list[str] | None
    table: str = Field(
        validation_alias="table_identifier",
        description="The Iceberg table: <project namespace>.<dataset name>",
    )
    created_by: str | None
    created_at: datetime
