"""Project CRUD and membership.

Access rules:
- Any signed-in user may create a project and becomes its admin.
- Reading a project needs membership; changing it needs the admin role.
- Platform admins (``CONTROL_PLANE_ADMIN_ROLES``) may act on every project.
- Platform viewers (``CONTROL_PLANE_VIEWER_ROLES``) may see every project and its
  members, and change none of them. This is project metadata only, not data.
- A non-member gets 404, not 403, so project names can't be probed.
- A project always keeps at least one admin.
- The system projects (``public``, ``platform``) are readable by everyone, managed
  by platform admins, and never deleted.

Creating a project records it (``pending``) and then provisions it: its Polaris
namespace, roles and storage prefix, and data access for its members (``ready``, or
``failed`` with the reason). Changing membership updates that member's data access
in Keycloak and Polaris immediately. Deleting a project removes its namespace and
roles first, and refuses while the namespace still holds tables.
"""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, Response, status
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.access import AccessError, delete_project_roles
from src.auth import Identity, get_identity
from src.config import Settings, get_settings
from src.db import get_session
from src.keycloak_admin import KeycloakAdmin, KeycloakError
from src.models import Membership, Project, ProjectKind, ProjectStatus, Role
from src.polaris import NamespaceNotEmpty, PolarisAdmin, PolarisError, deprovision
from src.provisioning import (
    apply_member_access,
    get_keycloak,
    get_polaris,
    provision_project,
)
from src.schemas import (
    PROJECT_NAME_PATTERN,
    RESERVED_PROJECT_NAMES,
    ErrorOut,
    MemberAdd,
    MemberOut,
    MemberPut,
    ProjectCreate,
    ProjectOut,
    ProjectUpdate,
)

router = APIRouter(prefix="/v1/projects", tags=["projects"])

SessionDep = Annotated[Session, Depends(get_session)]
IdentityDep = Annotated[Identity, Depends(get_identity)]
SettingsDep = Annotated[Settings, Depends(get_settings)]
PolarisDep = Annotated[PolarisAdmin | None, Depends(get_polaris)]
KeycloakDep = Annotated[KeycloakAdmin | None, Depends(get_keycloak)]

ProjectName = Annotated[
    str, Path(pattern=PROJECT_NAME_PATTERN, description="Project name")
]
Subject = Annotated[str, Path(max_length=255, description="Keycloak user id")]

_ERROR_TEXT = {
    401: "Missing or invalid access token",
    403: "Signed in, but without the project role this needs",
    404: "No such project (or the caller isn't a member), member or user",
    409: "Conflicts with the project's current state",
    502: "A Keycloak or Polaris call failed",
    503: "Provisioning or user lookup isn't configured, or Keycloak is down",
}


def _errors(*codes: int) -> dict:
    """OpenAPI ``responses`` entry documenting these error codes."""
    return {c: {"model": ErrorOut, "description": _ERROR_TEXT[c]} for c in codes}


def _not_found() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_404_NOT_FOUND, detail="Project not found"
    )


def _conflict(detail: str) -> HTTPException:
    return HTTPException(status_code=status.HTTP_409_CONFLICT, detail=detail)


def _unavailable(detail: str) -> HTTPException:
    return HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=detail)


def _access_failed(project: Project, exc: AccessError) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_502_BAD_GATEWAY,
        detail=(
            "Membership saved, but data access could not be updated "
            f"({exc}). Retry with POST /v1/projects/{project.name}/provision."
        ),
    )


def _membership(session: Session, project: Project, subject: str) -> Membership | None:
    return session.get(Membership, (project.id, subject))


def _is_viewer(identity: Identity, settings: Settings) -> bool:
    return identity.has_any_role(settings.viewer_roles)


def _authorize(
    session: Session,
    name: str,
    identity: Identity,
    settings: Settings,
    min_role: Role,
) -> tuple[Project, Role | None]:
    """Load a project the caller may act on with at least ``min_role``."""
    project = session.scalar(select(Project).where(Project.name == name))
    if project is None:
        raise _not_found()
    member = _membership(session, project, identity.subject)
    my_role = member.role if member else None
    if identity.has_any_role(settings.admin_roles):
        return project, my_role
    if my_role is None and (
        project.kind == ProjectKind.SYSTEM or _is_viewer(identity, settings)
    ):
        # Every signed-in user can read a system project; a platform viewer can
        # read every project. Neither can change it.
        if min_role == Role.READ:
            return project, None
    elif my_role is None:
        raise _not_found()
    if my_role is None or my_role.rank < min_role.rank:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Requires the {min_role.value} role in this project",
        )
    return project, my_role


def _project_out(project: Project, my_role: Role | None) -> ProjectOut:
    return ProjectOut.model_validate(project).model_copy(update={"my_role": my_role})


def _admin_count(session: Session, project: Project) -> int:
    return session.scalar(
        select(func.count())
        .select_from(Membership)
        .where(Membership.project_id == project.id, Membership.role == Role.ADMIN)
    )


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    operation_id="create_project",
    responses=_errors(401, 409, 503),
)
def create_project(
    body: ProjectCreate,
    session: SessionDep,
    identity: IdentityDep,
    polaris: PolarisDep,
    keycloak: KeycloakDep,
) -> ProjectOut:
    """Create a project; the response's ``status`` says whether provisioning worked."""
    if body.name in RESERVED_PROJECT_NAMES:
        raise _conflict(f"'{body.name}' is a reserved project name")
    project = Project(
        name=body.name,
        display_name=body.display_name,
        description=body.description,
        kind=ProjectKind.TENANT,
        created_by=identity.subject,
    )
    project.members.append(
        Membership(
            subject=identity.subject,
            username=identity.username,
            role=Role.ADMIN,
            added_by=identity.subject,
        )
    )
    session.add(project)
    try:
        session.commit()
    except IntegrityError as exc:
        session.rollback()
        raise _conflict(f"Project '{body.name}' already exists") from exc
    provision_project(session, project, polaris, keycloak)
    session.refresh(project)
    return _project_out(project, Role.ADMIN)


@router.post(
    "/{name}/provision",
    operation_id="provision_project",
    responses=_errors(401, 403, 404, 409, 503),
)
def reprovision_project(
    name: ProjectName,
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
    polaris: PolarisDep,
    keycloak: KeycloakDep,
) -> ProjectOut:
    """Run provisioning again (idempotent) - e.g. after a ``failed`` status.

    Also re-applies every member's data access.
    """
    project, my_role = _authorize(session, name, identity, settings, Role.ADMIN)
    if polaris is None:
        raise _unavailable("Provisioning is not configured")
    provision_project(session, project, polaris, keycloak, settings=settings)
    session.refresh(project)
    return _project_out(project, my_role)


@router.get("", operation_id="list_projects", responses=_errors(401, 503))
def list_projects(
    session: SessionDep, identity: IdentityDep, settings: SettingsDep
) -> list[ProjectOut]:
    """The system projects plus the projects the caller belongs to (every project,
    for a platform admin or viewer)."""
    rows = session.execute(
        select(Project, Membership.role)
        .outerjoin(
            Membership,
            (Membership.project_id == Project.id)
            & (Membership.subject == identity.subject),
        )
        .order_by(Project.name)
    ).all()
    sees_all = identity.has_any_role(settings.admin_roles) or _is_viewer(
        identity, settings
    )
    return [
        _project_out(p, role)
        for p, role in rows
        if role is not None or sees_all or p.kind == ProjectKind.SYSTEM
    ]


@router.get("/{name}", operation_id="get_project", responses=_errors(401, 404, 503))
def get_project(
    name: ProjectName, session: SessionDep, identity: IdentityDep, settings: SettingsDep
) -> ProjectOut:
    project, my_role = _authorize(session, name, identity, settings, Role.READ)
    return _project_out(project, my_role)


@router.patch(
    "/{name}", operation_id="update_project", responses=_errors(401, 403, 404, 503)
)
def update_project(
    name: ProjectName,
    body: ProjectUpdate,
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
) -> ProjectOut:
    project, my_role = _authorize(session, name, identity, settings, Role.ADMIN)
    for field_name, value in body.model_dump(exclude_unset=True).items():
        setattr(project, field_name, value)
    session.commit()
    session.refresh(project)
    return _project_out(project, my_role)


@router.delete(
    "/{name}",
    status_code=status.HTTP_204_NO_CONTENT,
    operation_id="delete_project",
    responses=_errors(401, 403, 404, 409, 502, 503),
)
def delete_project(
    name: ProjectName,
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
    polaris: PolarisDep,
    keycloak: KeycloakDep,
) -> Response:
    project, _ = _authorize(session, name, identity, settings, Role.ADMIN)
    if project.kind == ProjectKind.SYSTEM:
        raise _conflict("System projects cannot be deleted")
    if polaris is not None:
        try:
            # Drops the namespace (only if empty) and the Polaris roles, which
            # also removes every member's binding to them.
            deprovision(polaris, project.name, str(project.id))
            if keycloak is not None:
                delete_project_roles(keycloak, project.name)
        except NamespaceNotEmpty as exc:
            raise _conflict(
                "The project still has tables; drop them before deleting it"
            ) from exc
        except (PolarisError, KeycloakError) as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Could not remove the project's catalog or identity resources",
            ) from exc
    elif project.status != ProjectStatus.PENDING:
        # It may own Polaris resources we can't reach to remove.
        raise _unavailable("Provisioning is not configured")
    session.delete(project)
    session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get(
    "/{name}/members", operation_id="list_members", responses=_errors(401, 404, 503)
)
def list_members(
    name: ProjectName, session: SessionDep, identity: IdentityDep, settings: SettingsDep
) -> list[MemberOut]:
    project, _ = _authorize(session, name, identity, settings, Role.READ)
    members = session.scalars(
        select(Membership)
        .where(Membership.project_id == project.id)
        .order_by(Membership.added_at, Membership.subject)
    )
    return [MemberOut.model_validate(m) for m in members]


def _set_member(
    session: Session,
    project: Project,
    subject: str,
    role: Role,
    username: str | None,
    identity: Identity,
    response: Response,
    polaris: PolarisAdmin | None,
    keycloak: KeycloakAdmin | None,
) -> MemberOut:
    """Add a member or change their role, then give them that access."""
    member = _membership(session, project, subject)
    if member is None:
        member = Membership(
            project_id=project.id,
            subject=subject,
            username=username,
            role=role,
            added_by=identity.subject,
        )
        session.add(member)
        response.status_code = status.HTTP_201_CREATED
    else:
        if (
            member.role == Role.ADMIN
            and role != Role.ADMIN
            and _admin_count(session, project) == 1
        ):
            raise _conflict("A project must keep at least one admin")
        member.role = role
        if username is not None:
            member.username = username
    session.commit()
    try:
        resolved = apply_member_access(
            session, project, subject, role, polaris=polaris, keycloak=keycloak
        )
    except AccessError as exc:
        raise _access_failed(project, exc) from exc
    if resolved and member.username != resolved:
        member.username = resolved
        session.commit()
    session.refresh(member)
    return MemberOut.model_validate(member)


@router.post(
    "/{name}/members",
    operation_id="add_member",
    responses={
        **_errors(401, 403, 404, 409, 502, 503),
        201: {"model": MemberOut, "description": "Member added"},
    },
)
def add_member_by_username(
    name: ProjectName,
    body: MemberAdd,
    response: Response,
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
    polaris: PolarisDep,
    keycloak: KeycloakDep,
) -> MemberOut:
    """Add a member (or change their role) by Keycloak username."""
    project, _ = _authorize(session, name, identity, settings, Role.ADMIN)
    if keycloak is None:
        raise _unavailable("Looking up users is not configured")
    try:
        user = keycloak.find_user(body.username)
    except KeycloakError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY, detail="Could not look up the user"
        ) from exc
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No user named '{body.username}'",
        )
    return _set_member(
        session,
        project,
        user["id"],
        body.role,
        user["username"],
        identity,
        response,
        polaris,
        keycloak,
    )


@router.put(
    "/{name}/members/{subject}",
    operation_id="set_member",
    responses={
        **_errors(401, 403, 404, 409, 502, 503),
        201: {"model": MemberOut, "description": "Member added"},
    },
)
def put_member(
    name: ProjectName,
    subject: Subject,
    body: MemberPut,
    response: Response,
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
    polaris: PolarisDep,
    keycloak: KeycloakDep,
) -> MemberOut:
    """Add a member, or change a member's role, by Keycloak user id (``sub``)."""
    project, _ = _authorize(session, name, identity, settings, Role.ADMIN)
    username = body.username
    if keycloak is not None:
        # Only real users can become members - access can't be granted otherwise.
        try:
            user = keycloak.get_user(subject)
        except KeycloakError as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Could not look up the user",
            ) from exc
        if user is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No user with id '{subject}'",
            )
        username = user["username"]
    return _set_member(
        session,
        project,
        subject,
        body.role,
        username,
        identity,
        response,
        polaris,
        keycloak,
    )


@router.delete(
    "/{name}/members/{subject}",
    status_code=status.HTTP_204_NO_CONTENT,
    operation_id="remove_member",
    responses=_errors(401, 403, 404, 409, 502, 503),
)
def remove_member(
    name: ProjectName,
    subject: Subject,
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
    polaris: PolarisDep,
    keycloak: KeycloakDep,
) -> Response:
    """Remove a member, and their data access. Admins may remove anyone; any member
    may leave."""
    leaving = subject == identity.subject
    project, _ = _authorize(
        session, name, identity, settings, Role.READ if leaving else Role.ADMIN
    )
    member = _membership(session, project, subject)
    if member is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Member not found"
        )
    if member.role == Role.ADMIN and _admin_count(session, project) == 1:
        raise _conflict("A project must keep at least one admin")
    username = member.username
    session.delete(member)
    session.commit()
    try:
        apply_member_access(
            session,
            project,
            subject,
            None,
            polaris=polaris,
            keycloak=keycloak,
            username_hint=username,
        )
    except AccessError as exc:
        raise _access_failed(project, exc) from exc
    return Response(status_code=status.HTTP_204_NO_CONTENT)
