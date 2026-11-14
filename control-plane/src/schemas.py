"""Request and response bodies - the API contract.

These models generate the published OpenAPI spec (``control-plane/openapi.json``,
pinned by a snapshot test), which is what a CLI or SDK is built from (ADR-0007,
ADR-0011). Inputs are strict: an unknown field is an error, never silently dropped.
"""

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

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
