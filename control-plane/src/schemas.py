"""Request and response bodies."""

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from src.models import ProjectKind, ProjectStatus, Role

# Same rule as spec dataset references; also a valid Polaris namespace, S3 prefix
# and Keycloak role-name fragment.
PROJECT_NAME_PATTERN = r"^[a-z][a-z0-9_]{0,59}$"

# public / platform are the system projects (ADR-0001); teehr is today's live
# namespace, which a new project must not collide with.
RESERVED_PROJECT_NAMES = frozenset({"public", "platform", "teehr"})


class ProjectCreate(BaseModel):
    name: str = Field(pattern=PROJECT_NAME_PATTERN)
    display_name: str | None = Field(default=None, max_length=200)
    description: str | None = Field(default=None, max_length=2000)


class ProjectUpdate(BaseModel):
    display_name: str | None = Field(default=None, max_length=200)
    description: str | None = Field(default=None, max_length=2000)


class ProjectOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    name: str
    display_name: str | None
    description: str | None
    kind: ProjectKind
    status: ProjectStatus
    created_by: str
    created_at: datetime
    # The caller's role in this project (None for a platform admin who isn't a member).
    my_role: Role | None = None


class MemberPut(BaseModel):
    role: Role
    username: str | None = Field(default=None, max_length=255)


class MemberOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    subject: str
    username: str | None
    role: Role
    added_by: str
    added_at: datetime
