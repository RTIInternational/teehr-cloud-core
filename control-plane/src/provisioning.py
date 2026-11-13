"""Turn a recorded project into a provisioned one, and record how that went.

A project row is committed first (``pending``); provisioning then runs and sets
``ready`` or ``failed`` (with the reason in ``status_detail``). Every step is
idempotent, so a failed or interrupted project is simply provisioned again - on
request, or by the reconcile pass that runs when the service starts.
"""

import logging
from functools import lru_cache

from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from src.config import get_settings
from src.models import Project, ProjectKind, ProjectStatus
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
    )


UNREADY = (ProjectStatus.PENDING, ProjectStatus.FAILED)


def provision_project(
    session: Session,
    project: Project,
    admin: PolarisAdmin | None,
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
    try:
        provision(admin, project.name, str(project.id))
    except PolarisError as exc:
        logger.error("Provisioning '%s' failed: %s", project.name, exc)
        status, detail = ProjectStatus.FAILED, str(exc)[:1000]
    else:
        status, detail = ProjectStatus.READY, None
    project.status = status
    project.status_detail = detail
    session.commit()
    return True


def reconcile(engine: Engine, admin: PolarisAdmin | None) -> int:
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
            provision_project(session, project, admin, only_if_unready=True)
            for project in projects
        )
