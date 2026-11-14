"""Turn a recorded project into a provisioned one, and record how that went.

A project row is committed first (``pending``); provisioning then runs and sets
``ready`` or ``failed`` (with the reason in ``status_detail``). Provisioning covers
the Polaris namespace and roles (:mod:`src.polaris`) and, when Keycloak is
configured, turning every member into data access (:mod:`src.access`). Every step is
idempotent, so a failed or interrupted project is simply provisioned again - on
request, or by the reconcile pass that runs when the service starts.
"""

import logging
from functools import lru_cache

from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from src.access import AccessError, ensure_project_roles, sync_member
from src.config import get_settings
from src.keycloak_admin import KeycloakAdmin, KeycloakError
from src.models import Project, ProjectKind, ProjectStatus, Role
from src.polaris import PolarisAdmin, PolarisError, provision

logger = logging.getLogger("control-plane.provisioning")


@lru_cache
def get_polaris() -> PolarisAdmin | None:
    """The Polaris admin client, or None when provisioning isn't configured."""
    settings = get_settings()
    if not (settings.polaris_url and settings.polaris_credentials):
        return None
    client_id, client_secret = settings.polaris_credentials.split(":", 1)
    return PolarisAdmin(
        settings.polaris_url,
        settings.polaris_realm,
        settings.polaris_catalog,
        client_id,
        client_secret,
        warehouse=settings.polaris_warehouse,
    )


@lru_cache
def get_keycloak() -> KeycloakAdmin | None:
    """The Keycloak admin client, or None when membership isn't synced to access."""
    settings = get_settings()
    if not (
        settings.keycloak_admin_url
        and settings.keycloak_admin_user
        and settings.keycloak_admin_password
    ):
        return None
    return KeycloakAdmin(
        settings.keycloak_admin_url,
        settings.keycloak_realm,
        settings.keycloak_admin_user,
        settings.keycloak_admin_password,
    )


UNREADY = (ProjectStatus.PENDING, ProjectStatus.FAILED)


def _grant_members(
    project: Project, polaris: PolarisAdmin, keycloak: KeycloakAdmin
) -> None:
    ensure_project_roles(keycloak, project.name)
    for member in project.members:
        username = sync_member(
            keycloak,
            polaris,
            project.name,
            member.subject,
            member.role,
            member.username,
        )
        if username:
            member.username = username


def provision_project(
    session: Session,
    project: Project,
    admin: PolarisAdmin | None,
    keycloak: KeycloakAdmin | None = None,
    *,
    only_if_unready: bool = False,
) -> bool:
    """Provision one tenant project and commit its new status.

    Holds a row lock on the project for the duration, so two workers (or a user's
    retry and the startup reconcile) never provision the same project at once and
    never write contradictory statuses. With ``only_if_unready`` the status is
    re-checked under the lock and a project someone else just finished is skipped.
    Returns whether provisioning ran.
    """
    if admin is None:
        logger.warning(
            "Polaris not configured; '%s' stays %s", project.name, project.status
        )
        return False
    session.refresh(project, with_for_update=True)
    if only_if_unready and project.status not in UNREADY:
        session.commit()  # release the lock
        return False
    location = project.storage_location
    try:
        location = provision(admin, project.name, str(project.id), admin.warehouse)
        if keycloak is not None:
            _grant_members(project, admin, keycloak)
    except (PolarisError, KeycloakError, AccessError) as exc:
        logger.error("Provisioning '%s' failed: %s", project.name, exc)
        status, detail = ProjectStatus.FAILED, str(exc)[:1000]
    else:
        status, detail = ProjectStatus.READY, None
    project.status = status
    project.status_detail = detail
    project.storage_location = location
    session.commit()
    return True


def apply_member_access(
    session: Session,
    project: Project,
    subject: str,
    role: Role | None,
    *,
    polaris: PolarisAdmin | None,
    keycloak: KeycloakAdmin | None,
    username_hint: str | None = None,
) -> str | None:
    """Give one member exactly ``role`` (``None`` = none) after a membership change.

    Only for a ``ready`` project: a pending or failed one gets every member's access
    when it is (re)provisioned. If it fails, the project is marked ``failed`` - so
    the next reprovision or startup reconcile repairs it - and the error is raised.
    """
    if polaris is None or keycloak is None or project.status != ProjectStatus.READY:
        return None
    try:
        return sync_member(
            keycloak, polaris, project.name, subject, role, username_hint
        )
    except AccessError as exc:
        logger.error(
            "Access sync for '%s' in '%s' failed: %s", subject, project.name, exc
        )
        project.status = ProjectStatus.FAILED
        project.status_detail = f"access out of date: {exc}"[:1000]
        session.commit()
        raise


def reconcile(
    engine: Engine, admin: PolarisAdmin | None, keycloak: KeycloakAdmin | None = None
) -> int:
    """Provision every tenant project that is still pending or failed.

    Safe to run from several workers at once: each project is claimed under its
    row lock and skipped if another worker already provisioned it.
    """
    if admin is None:
        return 0
    with Session(engine) as session:
        projects = session.scalars(
            select(Project).where(
                Project.kind == ProjectKind.TENANT, Project.status.in_(UNREADY)
            )
        ).all()
        return sum(
            provision_project(session, project, admin, keycloak, only_if_unready=True)
            for project in projects
        )
