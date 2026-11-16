"""Uploads: stage a file, inspect it, then load it as a job (Phase 3; ADR-0014).

1. ``POST .../uploads`` returns a short-lived URL to ``PUT`` the file to: staging
   storage, not the API, so large files don't pass through it.
2. ``POST .../uploads/{id}/inspect`` reads the start of the file and proposes how it
   maps to a timeseries, with what's still needed (role, variable, unit, ...).
3. ``POST .../uploads/{id}/ingest`` confirms the mapping and the dataset, and starts
   the load: a Kubernetes Job acting as the caller (ADR-0014; ``src.jobs.load``).
4. ``GET .../uploads/{id}`` shows status, the preview, progress and problems.
5. ``POST .../uploads/{id}/cancel`` stops a load.

Uploading and inspecting need the write role; listing and reading need read.
"""

import contextlib
import uuid
from datetime import UTC, datetime, timedelta
from typing import Annotated
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Response, status
from fastapi.exceptions import RequestValidationError
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.dataset_types import TIMESERIES
from src.delegation import (
    Delegation,
    DelegationError,
    Sealer,
    get_delegation,
    get_sealer,
)
from src.domains import lookup
from src.jobs import ACTIVE as ACTIVE_JOBS
from src.jobs import JobRefused, cancel, reconcile, start_job
from src.kube import Kube, KubeError, get_kube
from src.models import (
    Dataset,
    Job,
    Project,
    Role,
    Unit,
    Upload,
    UploadStatus,
    Variable,
)
from src.routes.projects import (
    IdentityDep,
    ProjectName,
    SessionDep,
    SettingsDep,
    _authorize,
    _errors,
)
from src.schemas import IngestRequest, UploadCreate, UploadOut
from src.staging import StagedMissing, Staging, get_staging
from src.uploads import UploadError, detect_format, inspect, read_head

router = APIRouter(prefix="/v1/projects/{name}/uploads", tags=["uploads"])

StagingDep = Annotated[Staging | None, Depends(get_staging)]
KubeDep = Annotated[Kube | None, Depends(get_kube)]
UploadId = Annotated[uuid.UUID, Path(description="Upload id")]
ACTIVE = (UploadStatus.QUEUED, UploadStatus.RUNNING)


def _require_staging(staging: Staging | None) -> Staging:
    if staging is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Upload staging is not configured",
        )
    return staging


def _upload(session: Session, project: Project, upload_id: uuid.UUID) -> Upload:
    upload = session.get(Upload, upload_id)
    if upload is None or upload.project_id != project.id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Upload not found"
        )
    return upload


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    operation_id="create_upload",
    responses=_errors(401, 403, 404, 503),
)
def create_upload(
    name: ProjectName,
    body: UploadCreate,
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
    staging: StagingDep,
) -> UploadOut:
    """Start an upload: returns a URL to PUT the file to (valid for an hour)."""
    project, _ = _authorize(session, name, identity, settings, Role.WRITE)
    staging = _require_staging(staging)
    fmt = body.format or detect_format(body.filename)
    if fmt is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Say the format (csv or parquet), or use a .csv / .parquet name",
        )
    upload = Upload(
        id=uuid.uuid4(),
        project_id=project.id,
        filename=body.filename,
        format=fmt,
        kind=body.kind,
        created_by=identity.subject,
        rows_done=0,
    )
    upload.object_key = f"{project.name}/{upload.id}/{body.filename}"
    session.add(upload)
    session.commit()
    session.refresh(upload)
    ttl = settings.upload_url_ttl_seconds
    out = UploadOut.model_validate(upload)
    out.upload_url = staging.upload_url(upload.object_key, ttl)
    out.upload_url_expires_at = datetime.now(UTC) + timedelta(seconds=ttl)
    return out


@router.get("", operation_id="list_uploads", responses=_errors(401, 404))
def list_uploads(
    name: ProjectName,
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
    kube: KubeDep,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
) -> list[UploadOut]:
    """The project's uploads, newest first."""
    project, _ = _authorize(session, name, identity, settings, Role.READ)
    rows = session.scalars(
        select(Upload)
        .where(Upload.project_id == project.id)
        .order_by(Upload.created_at.desc())
        .limit(limit)
    )
    uploads = list(rows)
    for upload in uploads:
        _sync(session, kube, upload)
    return [UploadOut.model_validate(u) for u in uploads]


@router.get("/{upload_id}", operation_id="get_upload", responses=_errors(401, 404))
def get_upload(
    name: ProjectName,
    upload_id: UploadId,
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
    kube: KubeDep,
) -> UploadOut:
    project, _ = _authorize(session, name, identity, settings, Role.READ)
    upload = _upload(session, project, upload_id)
    _sync(session, kube, upload)
    return UploadOut.model_validate(upload)


@router.post(
    "/{upload_id}/inspect",
    operation_id="inspect_upload",
    responses=_errors(401, 403, 404, 409, 413, 503),
)
def inspect_upload(
    name: ProjectName,
    upload_id: UploadId,
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
    staging: StagingDep,
) -> UploadOut:
    """Read the start of the uploaded file and propose how it maps to a
    timeseries. Can be repeated (e.g. after uploading a corrected file)."""
    project, _ = _authorize(session, name, identity, settings, Role.WRITE)
    staging = _require_staging(staging)
    upload = _upload(session, project, upload_id)
    if upload.status in (*ACTIVE, UploadStatus.SUCCEEDED):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Upload is {upload.status.value}",
        )
    try:
        size = staging.size(upload.object_key)
    except StagedMissing as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Nothing uploaded yet: PUT the file to the upload URL first",
        ) from exc
    if size > settings.max_upload_bytes:
        raise HTTPException(
            status_code=status.HTTP_413_CONTENT_TOO_LARGE,
            detail=(
                f"Uploads are limited to {settings.max_upload_bytes // 1024**3} GiB"
            ),
        )
    try:
        with staging.open(upload.object_key) as file:
            head, rows = read_head(file, upload.format)
    except UploadError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(exc)
        ) from exc
    upload.size_bytes = size
    upload.preview = inspect(head, rows, upload.filename).to_dict()
    upload.status = UploadStatus.INSPECTED
    session.commit()
    session.refresh(upload)
    return UploadOut.model_validate(upload)


def _sync(session: Session, kube: Kube | None, upload: Upload) -> None:
    """Fail a loading upload whose job's pod died without reporting."""
    if upload.status not in ACTIVE or upload.job_id is None:
        return
    job = session.get(Job, upload.job_id)
    try:
        changed = job is not None and reconcile(kube, job)
    except KubeError:
        return  # can't tell now; the next read checks again
    if changed:
        upload.status, upload.error = UploadStatus.FAILED, job.error
        upload.finished_at = job.finished_at
        session.commit()


TARGETS = ("series_key", "value_time", "value", "reference_time", "member",
           "quantile", "quality")  # fmt: skip
OPTIONAL = ("reference_time", "member", "quantile", "quality")


def _ingest_problems(
    session: Session, project: Project, upload: Upload, body: IngestRequest
) -> tuple[dict, dict, list[tuple[tuple, str]]]:
    """The mapping and dataset to load with, and everything wrong with them."""
    preview = upload.preview or {}
    mapping = {
        **preview.get("mapping", {}),
        **body.mapping.model_dump(exclude_unset=True),
    }
    dataset = {
        **preview.get("dataset", {}),
        **body.dataset.model_dump(exclude_unset=True),
    }
    dataset["unit"] = body.dataset.unit
    columns = {c["name"] for c in preview.get("columns", [])}
    problems: list[tuple[tuple, str]] = []

    def bad(where: tuple, msg: str) -> None:
        problems.append((where, msg))

    for target in ("value_time", "value"):
        if not mapping.get(target):
            bad(("mapping", target), "required: the column with this")
    if bool(mapping.get("series_key")) == bool(mapping.get("series_key_value")):
        bad(
            ("mapping", "series_key"),
            "give series_key (a column) or series_key_value, not both",
        )
    for target in TARGETS:
        column = mapping.get(target)
        if column and column not in columns:
            bad(("mapping", target), f"no column '{column}' in the file")
    zone = mapping.get("timezone")
    if zone:
        try:
            ZoneInfo(zone)
        except (ZoneInfoNotFoundError, ValueError):
            bad(("mapping", "timezone"), f"unknown time zone '{zone}'")
    elif "mapping.timezone" in preview.get("needed", []):
        bad(
            ("mapping", "timezone"),
            "the file's times have no zone offset; say which zone",
        )
    mapped_optional = sorted(t for t in OPTIONAL if mapping.get(t))

    name = dataset.get("name")
    existing = session.scalar(
        select(Dataset).where(Dataset.project_id == project.id, Dataset.name == name)
    )
    canonical = None
    if existing is not None:
        meta = existing.properties or {}
        if existing.kind != TIMESERIES:
            bad(("dataset", "name"), f"'{name}' isn't a timeseries dataset")
        else:
            for key in ("role", "variable", "statistic", "period"):
                given = getattr(body.dataset, key)
                if given is not None and given != meta.get(key):
                    bad(
                        ("dataset", key),
                        f"'{name}' has {key} {meta.get(key)}, not {given}",
                    )
            have = sorted(meta.get("optional_columns", []))
            if mapped_optional != have:
                bad(
                    ("mapping",),
                    f"'{name}' has optional columns {have}; the mapping gives {mapped_optional}",
                )
            canonical = meta.get("unit")
    else:
        for key in ("role", "variable", "statistic"):
            if not dataset.get(key):
                bad(
                    ("dataset", key),
                    f"required for a new dataset ('{name}' doesn't exist)",
                )
        if dataset.get("role") == "forecast" and not mapping.get("reference_time"):
            bad(("mapping", "reference_time"), "a forecast needs its issue-time column")
        if dataset.get("variable"):
            variable = lookup(session, Variable, dataset["variable"], project.id)
            if variable is None:
                bad(
                    ("dataset", "variable"), f"unknown variable '{dataset['variable']}'"
                )
            else:
                canonical = variable.canonical_unit
        dataset["optional_columns"] = mapped_optional
    unit = lookup(session, Unit, body.dataset.unit, project.id)
    if unit is None:
        bad(("dataset", "unit"), f"unknown unit '{body.dataset.unit}'")
    elif canonical:
        target = lookup(session, Unit, canonical, project.id)
        if target is not None and target.dimension != unit.dimension:
            bad(("dataset", "unit"), f"{unit.symbol} doesn't convert to {canonical}")
    dataset["existing"] = existing is not None
    return mapping, dataset, problems


@router.post(
    "/{upload_id}/ingest",
    status_code=status.HTTP_202_ACCEPTED,
    operation_id="ingest_upload",
    responses=_errors(401, 403, 404, 409, 429, 502, 503),
)
def ingest_upload(
    name: ProjectName,
    upload_id: UploadId,
    body: IngestRequest,
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
    kube: KubeDep,
    delegation: Annotated[Delegation | None, Depends(get_delegation)],
    sealer: Annotated[Sealer | None, Depends(get_sealer)],
) -> UploadOut:
    """Confirm (or change) the mapping and say which dataset to load into, then
    start the load. Everything wrong is reported at once (422) before anything
    starts; the load itself checks the whole file before writing any of it."""
    project, _ = _authorize(session, name, identity, settings, Role.WRITE)
    if kube is None or delegation is None or sealer is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Loading uploads (jobs) is not configured",
        )
    upload = _upload(session, project, upload_id)
    ready = (UploadStatus.INSPECTED, UploadStatus.FAILED, UploadStatus.CANCELLED)
    if upload.status not in ready or not upload.preview:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Upload is {upload.status.value}; inspect it first",
        )
    mapping, dataset, problems = _ingest_problems(session, project, upload, body)
    if problems:
        raise RequestValidationError(
            [
                {"loc": ("body", *w), "msg": m, "type": "value_error"}
                for w, m in problems
            ]
        )
    upload.mapping, upload.dataset = mapping, dataset
    try:
        job = start_job(
            session, settings, kube, delegation, sealer,
            kind="upload", project=project,
            caller=identity.subject, caller_token=identity.token,
        )  # fmt: skip
    except JobRefused as exc:
        session.rollback()
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail=str(exc)
        ) from exc
    except DelegationError as exc:
        session.rollback()
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Couldn't get a token for the job to act as you; sign in again and retry",
        ) from exc
    except KubeError as exc:
        session.rollback()
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Couldn't start the load job",
        ) from exc
    upload.job_id, upload.status = job.id, UploadStatus.QUEUED
    upload.rows_done, upload.result, upload.problems, upload.error = 0, None, None, None
    upload.started_at = upload.finished_at = None
    try:
        session.commit()
    except Exception:
        # Without its row the Job's grant is refused anyway; don't leave it behind.
        session.rollback()
        with contextlib.suppress(KubeError):
            kube.delete_job(job.k8s_name)
        raise
    session.refresh(upload)
    return UploadOut.model_validate(upload)


@router.post(
    "/{upload_id}/cancel",
    operation_id="cancel_upload",
    responses=_errors(401, 403, 404, 409, 502, 503),
)
def cancel_upload(
    name: ProjectName,
    upload_id: UploadId,
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
    kube: KubeDep,
) -> UploadOut:
    """Stop a load. Rows it already wrote stay (each chunk is its own write)."""
    project, _ = _authorize(session, name, identity, settings, Role.WRITE)
    upload = _upload(session, project, upload_id)
    job = session.get(Job, upload.job_id) if upload.job_id else None
    if upload.status not in ACTIVE or job is None or job.status not in ACTIVE_JOBS:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Upload is {upload.status.value}, not loading",
        )
    if kube is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Loading uploads (jobs) is not configured; can't stop the job",
        )
    try:
        cancel(kube, job)
    except KubeError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY, detail="Couldn't stop the job"
        ) from exc
    upload.status, upload.finished_at = UploadStatus.CANCELLED, job.finished_at
    session.commit()
    session.refresh(upload)
    return UploadOut.model_validate(upload)


@router.delete(
    "/{upload_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    operation_id="delete_upload",
    responses=_errors(401, 403, 404, 409, 503),
)
def delete_upload(
    name: ProjectName,
    upload_id: UploadId,
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
    staging: StagingDep,
) -> Response:
    """Delete an upload and its staged file (not data already loaded)."""
    project, _ = _authorize(session, name, identity, settings, Role.WRITE)
    staging = _require_staging(staging)
    upload = _upload(session, project, upload_id)
    if upload.status in ACTIVE:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Upload is {upload.status.value}; cancel its load first",
        )
    staging.delete(upload.object_key)
    session.delete(upload)
    session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
