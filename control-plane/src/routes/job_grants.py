"""What a job's pod asks the control plane for, with its job grant (ADR-0014).

``Authorization: Bearer tjg_...`` (the grant, from the pod's environment), never a
user token. A grant works only while its job is queued or running:

- ``POST /v1/job-grants/token``: a fresh access token as the job's caller.
- ``GET /v1/job-grants/task``: what to do (for an upload: the mapping, the dataset,
  and a URL to read the staged file); marks the job running.
- ``POST /v1/job-grants/progress``: rows done so far.
- ``POST /v1/job-grants/finish``: the outcome; the grant stops working.
- ``POST /v1/job-grants/run-table``: one of a run's tables (``results``,
  ``pairs``, ``state``: ``<kind>_<slug>_v<version>``), created as the caller and
  registered (runs may write them; the caller needn't be able to register untyped
  datasets).
- ``POST /v1/job-grants/work``, ``DELETE /v1/job-grants/work``: URLs to put and get
  a run's work files (its shards' state parts, ADR-0017) in the staging bucket, under
  ``runs/<run id>/``, and their deletion (also when the job finishes).
- ``POST /v1/job-grants/writes``: a write the job committed to one of its run's
  tables itself (ADR-0016): checked against the table's snapshots and recorded as
  the dataset's lineage, as a write through ``/data`` is.

Internal: not in the published API.
"""

import re
import time
from datetime import UTC, datetime
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.dataset_types import TABLE
from src.delegation import (
    Delegation,
    DelegationError,
    Sealer,
    get_delegation,
    get_sealer,
)
from src.errors import APIError
from src.jobs import by_grant, end
from src.models import (
    Dataset,
    DatasetWrite,
    Job,
    JobStatus,
    Project,
    Run,
    Upload,
    UploadStatus,
)
from src.routes.projects import SessionDep, SettingsDep
from src.schemas import DatasetColumn
from src.staging import Staging, get_staging
from src.tables import TableError, TableExists, Tables, get_tables

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


def _run(session: Session, job: Job) -> Run | None:
    return session.scalar(select(Run).where(Run.job_id == job.id))


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
    run = _run(session, job)
    if job.kind == "run" and run is not None:
        body["run"] = {"id": str(run.id), **(run.task or {})}
        # Where the job reads its inputs from, as the caller (src/jobs/reads.py).
        body["catalog"] = (
            {
                "uri": f"{settings.polaris_url.rstrip('/')}/api/catalog",
                "warehouse": settings.polaris_catalog,
                "realm": settings.polaris_realm,
                "region": settings.storage_region,
                "endpoint": settings.storage_endpoint,
                "path_style": settings.storage_path_style,
            }
            if settings.polaris_url
            else None
        )
    if job.status == JobStatus.QUEUED:
        now = datetime.now(UTC)
        job.status, job.started_at = JobStatus.RUNNING, now
        if upload is not None:
            upload.status, upload.started_at = UploadStatus.RUNNING, now
        if run is not None:
            run.status, run.started_at = JobStatus.RUNNING.value, now
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
def finish(
    body: Finish,
    job: JobDep,
    session: SessionDep,
    staging: Annotated[Staging | None, Depends(get_staging)],
) -> Response:
    """The job's outcome. Its grant stops working; a run's work files go."""
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
    run = _run(session, job)
    if run is not None and staging is not None:
        try:
            staging.delete_prefix(_work_prefix(run))
        except Exception:  # noqa: BLE001 - leftovers don't change the outcome
            pass
    if run is not None:
        run.status = job.status.value
        run.result, run.error, run.finished_at = (
            body.result,
            body.error,
            job.finished_at,
        )
    session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


RUN_TABLES = {
    "results": "Results",
    "pairs": "Pairs",
    "state": "Metric state (per group and month)",
}


class RunTable(BaseModel):
    kind: Literal["results", "pairs", "state"] = "results"
    columns: list[DatasetColumn] = Field(min_length=1, max_length=1000)


def _caller_token(
    job: Job, session: Session, delegation: Delegation | None, sealer: Sealer | None
) -> str:
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
    job.refresh_token = sealer.seal(tokens.refresh_token)
    return tokens.access_token


@router.post("/run-table")
def run_table(
    body: RunTable,
    job: JobDep,
    session: SessionDep,
    delegation: Annotated[Delegation | None, Depends(get_delegation)],
    sealer: Annotated[Sealer | None, Depends(get_sealer)],
    tables: Annotated[Tables | None, Depends(get_tables)],
) -> dict:
    """Create and register one of a run's tables (``<kind>_<slug>_v<version>``) as
    the caller, or confirm the one there has these columns."""
    run = _run(session, job)
    if job.kind != "run" or run is None or not run.task:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Not a run's job")
    if tables is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "No table storage")
    project = session.get(Project, job.project_id)
    name = f"{body.kind}_{run.task['evaluation']}_v{run.task['version']}"
    columns = [c.model_dump(exclude_none=True) for c in body.columns]
    existing = session.scalar(
        select(Dataset).where(Dataset.project_id == project.id, Dataset.name == name)
    )
    if existing is not None:
        if existing.columns != columns:
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                f"'{name}' has other columns than this run's {body.kind} (an "
                "earlier engine's?); delete it and run again",
            )
        return {"dataset": name, "created": False}
    token = _caller_token(job, session, delegation, sealer)
    try:
        tables.create_table(token, project.name, name, columns, None)
    except TableExists as exc:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"A table '{name}' exists in '{project.name}' but isn't a registered dataset",
        ) from exc
    except TableError as exc:
        raise HTTPException(
            status.HTTP_502_BAD_GATEWAY, "The catalog could not create the table"
        ) from exc
    session.add(
        Dataset(
            project_id=project.id,
            name=name,
            kind=TABLE,
            description=(
                f"{RUN_TABLES[body.kind]} of evaluation '{run.task['evaluation']}' "
                f"version {run.task['version']}"
            ),
            columns=columns,
            properties={
                "evaluation": run.task["evaluation"],
                "version": run.task["version"],
                "run_table": body.kind,
            },
            table_identifier=f"{project.name}.{name}",
            created_by=job.created_by,
        )
    )
    session.commit()
    return {"dataset": name, "created": True}


class WriteReport(BaseModel):
    dataset: str = Field(max_length=255)
    mode: Literal["append", "overwrite"]
    filter: str | None = Field(default=None, max_length=2000)
    rows: int = Field(ge=0)
    snapshot_id: int


@router.post("/writes", status_code=status.HTTP_201_CREATED)
def record_write(
    body: WriteReport,
    job: JobDep,
    session: SessionDep,
    delegation: Annotated[Delegation | None, Depends(get_delegation)],
    sealer: Annotated[Sealer | None, Depends(get_sealer)],
    tables: Annotated[Tables | None, Depends(get_tables)],
) -> dict:
    """Record a write the job committed to one of its run's tables (ADR-0016), once
    its snapshot is on the table. Reporting the same snapshot again is a no-op."""
    run = _run(session, job)
    if job.kind != "run" or run is None or not run.task:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Not a run's job")
    own = {f"{k}_{run.task['evaluation']}_v{run.task['version']}" for k in RUN_TABLES}
    if body.dataset not in own:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Not one of this run's tables")
    if tables is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "No table storage")
    project = session.get(Project, job.project_id)
    record = session.scalar(
        select(Dataset).where(
            Dataset.project_id == project.id, Dataset.name == body.dataset
        )
    )
    if record is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"No dataset '{body.dataset}'")
    done = session.scalar(
        select(DatasetWrite).where(
            DatasetWrite.dataset_id == record.id,
            DatasetWrite.snapshot_id == body.snapshot_id,
        )
    )
    if done is None:
        token = _caller_token(job, session, delegation, sealer)
        try:
            found = tables.has_snapshot(
                token, project.name, body.dataset, body.snapshot_id
            )
        except TableError as exc:
            raise HTTPException(
                status.HTTP_502_BAD_GATEWAY, "The catalog could not be read"
            ) from exc
        if not found:
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                f"'{body.dataset}' has no snapshot {body.snapshot_id}",
            )
        done = DatasetWrite(
            dataset_id=record.id,
            mode=body.mode,
            filter=body.filter,
            rows=body.rows,
            snapshot_id=body.snapshot_id,
            written_by=job.created_by,
        )
        session.add(done)
    session.commit()
    return {"id": str(done.id), "snapshot_id": done.snapshot_id}


_WORK_NAME = re.compile(r"^[A-Za-z0-9_\-]+(/[A-Za-z0-9_\-][A-Za-z0-9_.\-]*)*$")
WORK_URL_TTL = 6 * 3600


def _work_prefix(run: Run) -> str:
    return f"runs/{run.id}/"


class WorkFiles(BaseModel):
    put: list[str] = Field(default_factory=list, max_length=1000)
    get: list[str] = Field(default_factory=list, max_length=10_000)


def _run_staging(
    job: Job, session: Session, staging: Staging | None
) -> tuple[Run, Staging]:
    run = _run(session, job)
    if job.kind != "run" or run is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Not a run's job")
    if staging is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "No staging")
    return run, staging


@router.post("/work")
def work_urls(
    body: WorkFiles,
    job: JobDep,
    session: SessionDep,
    staging: Annotated[Staging | None, Depends(get_staging)],
) -> dict:
    """URLs to put and get the run's work files (names relative to its prefix)."""
    run, staging = _run_staging(job, session, staging)
    bad = [n for n in body.put + body.get if not _WORK_NAME.match(n) or len(n) > 200]
    if bad:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT, f"Bad work file name: {bad[0]}"
        )
    prefix = _work_prefix(run)
    return {
        "put": {
            n: staging.internal_upload_url(prefix + n, WORK_URL_TTL) for n in body.put
        },
        "get": {n: staging.download_url(prefix + n, WORK_URL_TTL) for n in body.get},
    }


@router.delete("/work")
def delete_work(
    job: JobDep,
    session: SessionDep,
    staging: Annotated[Staging | None, Depends(get_staging)],
) -> dict:
    """Delete the run's work files."""
    run, staging = _run_staging(job, session, staging)
    return {"deleted": staging.delete_prefix(_work_prefix(run))}
