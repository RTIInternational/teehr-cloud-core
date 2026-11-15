"""Uploads: stage a file, inspect it, then load it as a job (Phase 3; ADR-0014).

1. ``POST .../uploads`` returns a short-lived URL to ``PUT`` the file to: staging
   storage, not the API, so large files don't pass through it.
2. ``POST .../uploads/{id}/inspect`` reads the start of the file and proposes how it
   maps to a timeseries, with what's still needed (role, variable, unit, ...).
3. ``POST .../uploads/{id}/ingest`` confirms the mapping and starts the load job
   (next: jobs).
4. ``GET .../uploads/{id}`` shows status, the preview, progress and problems.

Uploading and inspecting need the write role; listing and reading need read.
"""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Response, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models import Project, Role, Upload, UploadStatus
from src.routes.projects import (
    IdentityDep,
    ProjectName,
    SessionDep,
    SettingsDep,
    _authorize,
    _errors,
)
from src.schemas import UploadCreate, UploadOut
from src.staging import StagedMissing, Staging, get_staging
from src.uploads import UploadError, detect_format, inspect, read_head

router = APIRouter(prefix="/v1/projects/{name}/uploads", tags=["uploads"])

StagingDep = Annotated[Staging | None, Depends(get_staging)]
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
    return [UploadOut.model_validate(u) for u in rows]


@router.get("/{upload_id}", operation_id="get_upload", responses=_errors(401, 404))
def get_upload(
    name: ProjectName,
    upload_id: UploadId,
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
) -> UploadOut:
    project, _ = _authorize(session, name, identity, settings, Role.READ)
    return UploadOut.model_validate(_upload(session, project, upload_id))


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
