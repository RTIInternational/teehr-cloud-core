"""Who the caller is and what they can do: ``GET /v1/me``.

For a UI (or ``teehr auth status``) to show what someone can do without trying it:
their platform roles, as the control plane reads them, and their project
memberships (the membership table, the source of truth). System projects aren't
listed: every signed-in user can read them (``GET /v1/projects`` lists them).
"""

from datetime import datetime

from fastapi import APIRouter
from pydantic import BaseModel, Field
from sqlalchemy import select

from src.models import Membership, PersonalToken, Project, Role
from src.routes.projects import IdentityDep, SessionDep, SettingsDep, _errors

router = APIRouter(prefix="/v1", tags=["me"])


class PlatformOut(BaseModel):
    admin: bool = Field(description="A platform admin: every project, every role")
    viewer: bool = Field(description="A platform viewer: reads every project")
    can_create_projects: bool


class MembershipOut(BaseModel):
    project: str
    role: Role


class TokenUsedOut(BaseModel):
    """The personal token this request used (its limits apply on top of roles)."""

    id: str
    name: str
    scopes: list[str]
    projects: list[str] | None = Field(
        default=None, description="The only projects it may act on (null: any)"
    )
    expires_at: datetime


class MeOut(BaseModel):
    subject: str
    username: str | None
    platform: PlatformOut
    projects: list[MembershipOut] = Field(
        description="Projects the caller is a member of, with their role"
    )
    token: TokenUsedOut | None = Field(
        default=None, description="Set when the request used a personal token"
    )


@router.get("/me", operation_id="get_me", responses=_errors(401, 503))
def get_me(session: SessionDep, identity: IdentityDep, settings: SettingsDep) -> MeOut:
    """The caller: their platform roles, project memberships and, when they signed
    in with a personal token, that token's limits."""
    admin = identity.has_any_role(settings.admin_roles)
    creators = settings.project_creator_roles
    rows = session.execute(
        select(Project.name, Membership.role)
        .join(Membership, Membership.project_id == Project.id)
        .where(Membership.subject == identity.subject)
        .order_by(Project.name)
    ).all()
    token = None
    if identity.personal_token_id is not None:
        used = session.get(PersonalToken, identity.personal_token_id)
        if used is not None:
            token = TokenUsedOut(
                id=str(used.id), name=used.name, scopes=list(used.scopes),
                projects=used.projects, expires_at=used.expires_at,
            )  # fmt: skip
    return MeOut(
        subject=identity.subject,
        username=identity.username,
        platform=PlatformOut(
            admin=admin,
            viewer=identity.has_any_role(settings.viewer_roles),
            can_create_projects=admin
            or not creators
            or identity.has_any_role(creators),
        ),
        projects=[MembershipOut(project=name, role=role) for name, role in rows],
        token=token,
    )
