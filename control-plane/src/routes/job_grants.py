"""What a job's pod asks the control plane for, with its job grant (ADR-0014).

``Authorization: Bearer tjg_...`` (the grant, from the pod's environment), never a
user token. A grant works only while its job is queued or running:

- ``POST /v1/job-grants/token``: a fresh access token as the job's caller.
- ``GET /v1/job-grants/task``: what to do (for an upload: the mapping, the dataset,
  and a URL to read the staged file); marks the job running.
- ``POST /v1/job-grants/progress``: rows done so far.
- ``POST /v1/job-grants/finish``: the outcome; the grant stops working.

Internal: not in the published API.
"""

import time
from datetime import UTC, datetime
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.delegation import (
    Delegation,
    DelegationError,
    Sealer,
    get_delegation,
    get_sealer,
)
from src.errors import APIError
from src.jobs import by_grant, end
from src.models import Job, JobStatus, Project, Upload, UploadStatus
from src.routes.projects import SessionDep, SettingsDep
from src.staging import Staging, get_staging

router = APIRouter(prefix="/v1/job-grants", include_in_schema=False)

SESSION_ENDED = "session_ended"


def _job(
    session: SessionDep, authorization: Annotated[str | None, Header()] = None
) -> Job:
    token = (authorization or "").removeprefix("Bearer ").strip()
    job = by_grant(session, token) if token else None
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unknown, expired or finished job grant",
        )
    return job


JobDep = Annotated[Job, Depends(_job)]


def _upload(session: Session, job: Job) -> Upload | None:
    return session.scalar(select(Upload).where(Upload.job_id == job.id))


@router.post("/token")
def token(
    job: JobDep,
    session: SessionDep,
    delegation: Annotated[Delegation | None, Depends(get_delegation)],
    sealer: Annotated[Sealer | None, Depends(get_sealer)],
) -> dict:
    """A fresh access token as the caller, from their stored refresh token."""
    if delegation is None or sealer is None or not job.refresh_token:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, "Jobs aren't configured"
        )
    try:
        tokens = delegation.refresh(sealer.open(job.refresh_token))
    except DelegationError as exc:
        raise APIError(
            status.HTTP_401_UNAUTHORIZED,
            "The caller's login session has ended; the job can't act as them",
            code=SESSION_ENDED,
        ) from exc
    job.refresh_token = sealer.seal(tokens.refresh_token)  # it may rotate
    session.commit()
    return {
        "access_token": tokens.access_token,
        "expires_in": max(0, int(tokens.expires_at - time.time())),
    }


@router.get("/task")
def task(
    job: JobDep,
    session: SessionDep,
    settings: SettingsDep,
    staging: Annotated[Staging | None, Depends(get_staging)],
) -> dict:
    """What the job is to do. The first call marks it running."""
    project = session.get(Project, job.project_id)
    body: dict = {"job": str(job.id), "kind": job.kind, "project": project.name}
    upload = _upload(session, job)
    if job.kind == "upload" and upload is not None:
        if staging is None:
            raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "No staging")
        body["upload"] = {
            "id": str(upload.id),
            "filename": upload.filename,
            "format": upload.format,
            "mapping": upload.mapping,
            "dataset": upload.dataset,
            # Long enough for a big file to download.
            "download_url": staging.download_url(upload.object_key, 6 * 3600),
        }
    if job.status == JobStatus.QUEUED:
        now = datetime.now(UTC)
        job.status, job.started_at = JobStatus.RUNNING, now
        if upload is not None:
            upload.status, upload.started_at = UploadStatus.RUNNING, now
        session.commit()
    return body


class Progress(BaseModel):
    rows_done: int = Field(ge=0)


@router.post("/progress", status_code=status.HTTP_204_NO_CONTENT)
def progress(body: Progress, job: JobDep, session: SessionDep) -> Response:
    upload = _upload(session, job)
    if upload is not None:
        upload.rows_done = body.rows_done
        session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


class Finish(BaseModel):
    status: Literal["succeeded", "failed"]
    result: dict | None = None
    problems: list[str] | None = Field(default=None, max_length=1000)
    error: str | None = Field(default=None, max_length=10_000)


@router.post("/finish", status_code=status.HTTP_204_NO_CONTENT)
def finish(body: Finish, job: JobDep, session: SessionDep) -> Response:
    """The job's outcome. Its grant stops working."""
    ok = body.status == "succeeded"
    end(job, JobStatus.SUCCEEDED if ok else JobStatus.FAILED, body.error)
    upload = _upload(session, job)
    if upload is not None:
        upload.status = UploadStatus.SUCCEEDED if ok else UploadStatus.FAILED
        upload.result, upload.problems, upload.error = (
            body.result,
            body.problems,
            body.error,
        )
        upload.finished_at = job.finished_at
    session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
