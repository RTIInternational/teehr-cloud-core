"""Platform admin operations. For now: copying legacy warehouse data into ``public``.

``POST /v1/admin/legacy-copies`` starts a job, as the admin, that reads the legacy
tables (namespace ``teehr``) for one forecast configuration. ``mode: count`` (the only
mode so far) says how much a copy would move, per month, without writing anything.
"""

import contextlib
import uuid
from datetime import datetime
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.delegation import (
    Delegation,
    DelegationError,
    Sealer,
    get_delegation,
    get_sealer,
)
from src.jobs import JobRefused, reconcile, start_job
from src.kube import Kube, KubeError, get_kube
from src.models import Job, LegacyCopy, Project
from src.routes.projects import IdentityDep, SessionDep, SettingsDep, _errors

router = APIRouter(prefix="/v1/admin", tags=["admin"])


class LegacyCopyRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    configuration: str = Field(
        pattern=r"^[a-z][a-z0-9_]{0,99}$",
        description="A legacy forecast configuration, e.g. nwm30_medium_range",
    )
    observations: str = Field(
        default="usgs_observations",
        pattern=r"^[a-z][a-z0-9_]{0,99}$",
        description="The legacy observations configuration it's evaluated against",
    )
    mode: Literal["count"] = Field(
        default="count", description="count: how much a copy would move, per month"
    )


class LegacyCopyOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    status: str
    request: dict[str, Any]
    result: dict[str, Any] | None
    error: str | None
    created_by: str
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


def _admin(identity: IdentityDep, settings: SettingsDep) -> None:
    if not identity.has_any_role(settings.admin_roles):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Platform admins only")


def _sync(session: Session, kube: Kube | None, copy: LegacyCopy) -> None:
    job = session.get(Job, copy.job_id) if copy.job_id else None
    if job is None:
        return
    if reconcile(kube, job) or copy.status != job.status.value:
        copy.status, copy.error = job.status.value, job.error or copy.error
        copy.started_at = copy.started_at or job.started_at
        copy.finished_at = job.finished_at
        session.commit()


@router.post(
    "/legacy-copies",
    operation_id="start_legacy_copy",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=LegacyCopyOut,
    responses=_errors(401, 403, 429, 502, 503),
)
def start_legacy_copy(
    body: LegacyCopyRequest,
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
    kube: Annotated[Kube | None, Depends(get_kube)],
    delegation: Annotated[Delegation | None, Depends(get_delegation)],
    sealer: Annotated[Sealer | None, Depends(get_sealer)],
):
    """Start a job, as you, on legacy warehouse data for one configuration
    (``mode: count``: how much a copy would move). Platform admins only."""
    _admin(identity, settings)
    if kube is None or delegation is None or sealer is None:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, "Jobs aren't configured"
        )
    platform = session.scalar(select(Project).where(Project.name == "platform"))
    if platform is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "No platform project")
    try:
        job = start_job(
            session, settings, kube, delegation, sealer,
            kind="legacy_copy", project=platform,
            caller=identity.subject, caller_token=identity.token,
        )  # fmt: skip
    except JobRefused as exc:
        session.rollback()
        raise HTTPException(status.HTTP_429_TOO_MANY_REQUESTS, str(exc)) from exc
    except DelegationError as exc:
        session.rollback()
        raise HTTPException(
            status.HTTP_502_BAD_GATEWAY,
            "Couldn't get a token for the job to act as you; sign in again and retry",
        ) from exc
    except KubeError as exc:
        session.rollback()
        raise HTTPException(
            status.HTTP_502_BAD_GATEWAY, "Couldn't start the job"
        ) from exc
    copy = LegacyCopy(
        job_id=job.id,
        status=job.status.value,
        request=body.model_dump(),
        created_by=identity.subject,
    )
    session.add(copy)
    try:
        session.commit()
    except Exception:
        session.rollback()
        with contextlib.suppress(KubeError):
            kube.delete_job(job.k8s_name)
        raise
    session.refresh(copy)
    return copy


@router.get(
    "/legacy-copies",
    operation_id="list_legacy_copies",
    response_model=list[LegacyCopyOut],
    responses=_errors(401, 403),
)
def list_legacy_copies(
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
    kube: Annotated[Kube | None, Depends(get_kube)],
):
    """Legacy copies, newest first. Platform admins only."""
    _admin(identity, settings)
    copies = session.scalars(
        select(LegacyCopy).order_by(LegacyCopy.created_at.desc())
    ).all()
    for copy in copies:
        _sync(session, kube, copy)
    return copies


@router.get(
    "/legacy-copies/{copy_id}",
    operation_id="get_legacy_copy",
    response_model=LegacyCopyOut,
    responses=_errors(401, 403, 404),
)
def get_legacy_copy(
    copy_id: uuid.UUID,
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
    kube: Annotated[Kube | None, Depends(get_kube)],
):
    """A legacy copy's status and, when done, what it found. Platform admins only."""
    _admin(identity, settings)
    copy = session.get(LegacyCopy, copy_id)
    if copy is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Legacy copy not found")
    _sync(session, kube, copy)
    return copy
