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

import pyarrow as pa
import pyarrow.compute as pc
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Response, status
from fastapi.exceptions import RequestValidationError
from sqlalchemy import select
from sqlalchemy.orm import Session

from src import job_logs
from src.access import project_roles
from src.dataset_types import (
    CROSSWALK,
    CROSSWALK_COLUMNS,
    LAYER,
    LAYER_COLUMNS,
    TIMESERIES,
)
from src.delegation import (
    Delegation,
    DelegationError,
    Sealer,
    get_delegation,
    get_sealer,
)
from src.domains import lookup
from src.errors import APIError
from src.geofiles import (
    GEO_FORMATS,
    GeoFileError,
    describe_crs,
    geoparquet,
    read_geo,
)
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
from src.refs import resolve
from src.routes.projects import (
    IdentityDep,
    ProjectName,
    SessionDep,
    SettingsDep,
    _authorize,
    _errors,
)
from src.schemas import IngestRequest, LogsOut, UploadCreate, UploadOut
from src.staging import StagedMissing, Staging, get_staging
from src.uploads import (
    KIND_FORMATS,
    SAMPLE_ROWS,
    UploadError,
    attribute_name,
    detect_format,
    inspect,
    inspect_crosswalk,
    inspect_layer,
    read_head,
)

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
    allowed = KIND_FORMATS[body.kind]
    if fmt is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"Say the format ({', '.join(allowed)}), or use a file name with its "
                "extension"
            ),
        )
    if fmt not in allowed:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"A {body.kind} upload is one of: {', '.join(allowed)}",
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
    source_layer: Annotated[
        str | None,
        Query(max_length=255, description="GeoPackage: which layer to inspect"),
    ] = None,
) -> UploadOut:
    """Read the start of the uploaded file and propose how it maps to a dataset
    of the upload's kind. Can be repeated (e.g. after uploading a corrected file,
    or for another layer of a GeoPackage)."""
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
        preview = _inspect(staging, upload, source_layer)
    except (UploadError, GeoFileError) as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(exc)
        ) from exc
    upload.size_bytes = size
    upload.preview = preview.to_dict()
    upload.status = UploadStatus.INSPECTED
    session.commit()
    session.refresh(upload)
    return UploadOut.model_validate(upload)


def _inspect(staging: Staging, upload: Upload, source_layer: str | None):
    """The preview for an upload of each kind and format."""
    if upload.format in GEO_FORMATS:
        path = staging.gdal_url(upload.object_key)
        head, info = read_geo(upload.format, path, source_layer, SAMPLE_ROWS)
        return inspect_layer(
            head,
            info["features"],
            upload.filename,
            geometry="geometry",
            crs=info["crs"],
            crs_known=False,
            extra={"layers": info["layers"], "layer": info["layer"]},
        )
    with staging.open(upload.object_key) as file:
        head, rows = read_head(file, upload.format)
    if upload.kind == "crosswalk":
        return inspect_crosswalk(head, rows, upload.filename)
    if upload.kind == "layer":
        if upload.format == "parquet":
            column, crs = geoparquet(head.schema)
            # Plain Parquet: a WKB column without a stated coordinate system.
            return inspect_layer(
                head, rows, upload.filename, geometry=column, crs=crs,
                crs_known=column is not None,
            )  # fmt: skip
        return inspect_layer(head, rows, upload.filename, crs="EPSG:4326")
    return inspect(head, rows, upload.filename)


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
           "quantile", "quality", "location_key")  # fmt: skip
OPTIONAL = ("reference_time", "member", "quantile", "quality")


# Which ingest fields each kind of upload takes.
MAPPING_FIELDS = {
    "timeseries": {
        *TARGETS,
        "series_key_value",
        "timezone",
        "layout",
        "series_columns",
        "time_format",
    },  # fmt: skip
    "layer": {
        "location_key",
        "name",
        "geometry",
        "longitude",
        "latitude",
        "crs",
        "source_layer",
        "attributes",
    },  # fmt: skip
    "crosswalk": {"left_key", "right_key", "attributes"},
}
DATASET_FIELDS = {
    "timeseries": {"name", "unit", "role", "variable", "statistic", "period",
                   "description", "source", "location_layer", "time_partition"},
    "layer": {"name", "description", "source"},
    "crosswalk": {"name", "description", "source", "left", "right"},
}  # fmt: skip


def _ingest_problems(
    session: Session, project: Project, upload: Upload, body: IngestRequest
) -> tuple[dict, dict, list[tuple[tuple, str]]]:
    """The mapping and dataset to load with, and everything wrong with them."""
    preview = upload.preview or {}
    given_mapping = body.mapping.model_dump(exclude_unset=True)
    given_dataset = body.dataset.model_dump(exclude_unset=True)
    mapping = {**preview.get("mapping", {}), **given_mapping}
    dataset = {**preview.get("dataset", {}), **given_dataset}
    columns = {c["name"] for c in preview.get("columns", [])}
    problems: list[tuple[tuple, str]] = []

    def bad(where: tuple, msg: str) -> None:
        problems.append((where, msg))

    kind = upload.kind
    for part, given, allowed in (
        ("mapping", given_mapping, MAPPING_FIELDS[kind]),
        ("dataset", given_dataset, DATASET_FIELDS[kind]),
    ):
        for key in sorted(set(given) - allowed):
            bad((part, key), f"doesn't apply to a {kind} upload")
    if kind == "layer":
        _layer_problems(session, project, preview, mapping, dataset, columns, bad)
    elif kind == "crosswalk":
        _crosswalk_problems(session, project, preview, mapping, dataset, columns, bad)
    else:
        _timeseries_problems(
            session, project, preview, body, mapping, dataset, columns, bad
        )
    dataset["kind"] = kind
    return mapping, dataset, problems


def _attributes(
    preview: dict, mapping: dict, columns: set[str], used: set, reserved, bad
) -> list[dict]:
    """The file columns kept as attributes / extra columns: each with a valid,
    distinct name and a type (inspect's proposal unless the request says)."""
    proposed = {a["column"]: a for a in preview.get("mapping", {}).get("attributes")
                or []}  # fmt: skip
    out, names = [], set()
    for i, item in enumerate(mapping.get("attributes") or []):
        column = item.get("column")
        if column not in columns:
            bad(("mapping", "attributes", str(i)), f"no column '{column}' in the file")
            continue
        if column in used:
            bad(("mapping", "attributes", str(i)), f"'{column}' is already mapped")
            continue
        base = proposed.get(column, {})
        name = item.get("name") or base.get("name") or attribute_name(column)
        type_ = item.get("type") or base.get("type") or "string"
        if name.lower() in reserved or name.lower() in names:
            bad(("mapping", "attributes", str(i)), f"name '{name}' is taken")
            continue
        names.add(name.lower())
        out.append({"column": column, "name": name, "type": type_})
    return out


def _fits_existing(existing: Dataset, attributes: list[dict], reserved, bad) -> None:
    have = {c["name"] for c in existing.columns or []} - set(reserved)
    unknown = [a["name"] for a in attributes if a["name"] not in have]
    if unknown:
        bad(
            ("mapping", "attributes"),
            f"'{existing.name}' has no columns {', '.join(unknown)} "
            f"(it has: {', '.join(sorted(have)) or 'none'})",
        )


def _layer_problems(session, project, preview, mapping, dataset, columns, bad):
    if not mapping.get("location_key"):
        bad(("mapping", "location_key"), "required: the column with location keys")
    has_geometry = bool(mapping.get("geometry"))
    has_points = bool(mapping.get("longitude")) or bool(mapping.get("latitude"))
    if has_geometry == has_points:
        bad(
            ("mapping", "geometry"),
            "give geometry (a column) or longitude and latitude, not both",
        )
    elif has_points and not (mapping.get("longitude") and mapping.get("latitude")):
        bad(("mapping", "longitude"), "give both longitude and latitude")
    for target in ("location_key", "name", "geometry", "longitude", "latitude"):
        column = mapping.get(target)
        if column and column not in columns:
            bad(("mapping", target), f"no column '{column}' in the file")
    crs = mapping.get("crs")
    if crs:
        try:
            mapping["crs"] = describe_crs(crs)
        except GeoFileError as exc:
            bad(("mapping", "crs"), str(exc))
    elif "mapping.crs" in preview.get("needed", []):
        bad(("mapping", "crs"), "the file doesn't say its coordinate system")
    inspected = preview.get("mapping", {}).get("source_layer")
    if mapping.get("source_layer") != inspected:
        bad(
            ("mapping", "source_layer"),
            f"inspect that layer first (POST .../inspect?source_layer=...); "
            f"'{inspected}' was inspected",
        )
    used = {mapping.get(t) for t in ("location_key", "name", "geometry",
                                     "longitude", "latitude")} - {None}  # fmt: skip
    mapping["attributes"] = _attributes(
        preview, mapping, columns, used, LAYER_COLUMNS, bad
    )
    existing = _existing(session, project, dataset)
    if existing is not None:
        if existing.kind != LAYER:
            bad(("dataset", "name"), f"'{existing.name}' isn't a layer")
        else:
            _fits_existing(existing, mapping["attributes"], LAYER_COLUMNS, bad)
    dataset["existing"] = existing is not None


def _crosswalk_problems(session, project, preview, mapping, dataset, columns, bad):
    for target in ("left_key", "right_key"):
        column = mapping.get(target)
        if not column:
            bad(("mapping", target), "required: the column with these keys")
        elif column not in columns:
            bad(("mapping", target), f"no column '{column}' in the file")
    if mapping.get("left_key") and mapping.get("left_key") == mapping.get("right_key"):
        bad(("mapping", "right_key"), "left and right keys are the same column")
    used = {mapping.get("left_key"), mapping.get("right_key")} - {None}
    mapping["attributes"] = _attributes(
        preview, mapping, columns, used, CROSSWALK_COLUMNS, bad
    )
    existing = _existing(session, project, dataset)
    if existing is not None:
        if existing.kind != CROSSWALK:
            bad(("dataset", "name"), f"'{existing.name}' isn't a crosswalk")
        else:
            _fits_existing(existing, mapping["attributes"], CROSSWALK_COLUMNS, bad)
            for side in ("left", "right"):
                given = dataset.get(side)
                have = (existing.properties or {}).get(side)
                if given is not None and given != have:
                    bad(("dataset", side), f"'{existing.name}' has {side} {have}")
    else:
        for side in ("left", "right"):
            ref = dataset.get(side)
            if not ref:
                bad(("dataset", side), "required for a new crosswalk")
                continue
            found = resolve(session, project, ref)
            if found is None or found.dataset.kind != TIMESERIES:
                bad(("dataset", side), f"'{ref}' is not a timeseries dataset")
    dataset["existing"] = existing is not None


def _existing(session: Session, project: Project, dataset: dict) -> Dataset | None:
    return session.scalar(
        select(Dataset).where(
            Dataset.project_id == project.id, Dataset.name == dataset.get("name")
        )
    )


def _wide_problems(mapping: dict, columns: set[str], bad) -> None:
    """A wide file: one value column per series, named by its key."""
    series = mapping.get("series_columns") or []
    if not series:
        bad(
            ("mapping", "series_columns"), "required for layout wide: the value columns"
        )
    for name in ("series_key", "series_key_value", "value", "location_key"):
        if mapping.get(name):
            bad(
                ("mapping", name),
                "doesn't apply to layout wide (each value column is one series, "
                "its header the series key)",
            )
    used = {mapping.get(t) for t in TARGETS} - {None}
    for i, column in enumerate(series):
        if column not in columns:
            bad(
                ("mapping", "series_columns", str(i)),
                f"no column '{column}' in the file",
            )
        elif column in used:
            bad(("mapping", "series_columns", str(i)), f"'{column}' is already mapped")
    if len(set(series)) != len(series):
        bad(("mapping", "series_columns"), "a column is listed twice")


def _time_format_problems(preview: dict, mapping: dict, bad) -> None:
    """The time format parses the sample's times (as the load will)."""
    fmt = mapping.get("time_format")
    if not fmt:
        if "mapping.time_format" in preview.get("needed", []):
            bad(
                ("mapping", "time_format"),
                "the file's times aren't ISO 8601; give their strptime format",
            )
        return
    text = {
        c["name"]
        for c in preview.get("columns", [])
        if c.get("type") in ("string", "large_string")
    }
    for target in ("value_time", "reference_time"):
        column = mapping.get(target)
        if column not in text:
            continue  # typed times (Parquet) aren't parsed
        values = [r.get(column) for r in preview.get("sample", [])]
        values = [v for v in values if isinstance(v, str) and v]
        if not values:
            continue
        try:
            pc.strptime(pa.array(values), format=fmt, unit="us")
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as exc:
            bad(("mapping", "time_format"), f"doesn't parse '{column}': {exc}")
            return


def _timeseries_problems(
    session, project, preview, body, mapping, dataset, columns, bad
) -> None:
    if body.dataset.unit is None:
        bad(("dataset", "unit"), "required: the unit of the file's values")
    wide = mapping.get("layout") == "wide"
    if not mapping.get("value_time"):
        bad(("mapping", "value_time"), "required: the column with this")
    if wide:
        _wide_problems(mapping, columns, bad)
    else:
        if not mapping.get("value"):
            bad(("mapping", "value"), "required: the column with this")
        if bool(mapping.get("series_key")) == bool(mapping.get("series_key_value")):
            bad(
                ("mapping", "series_key"),
                "give series_key (a column) or series_key_value, not both",
            )
    _time_format_problems(preview, mapping, bad)
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
    unit = (
        lookup(session, Unit, body.dataset.unit, project.id)
        if body.dataset.unit
        else None
    )
    if unit is None:
        if body.dataset.unit:
            bad(("dataset", "unit"), f"unknown unit '{body.dataset.unit}'")
    elif canonical:
        target = lookup(session, Unit, canonical, project.id)
        if target is not None and target.dimension != unit.dimension:
            bad(("dataset", "unit"), f"{unit.symbol} doesn't convert to {canonical}")
    layer = (
        (existing.properties or {}).get("location_layer")
        if existing is not None
        else dataset.get("location_layer")
    )
    if existing is None and layer:
        found = resolve(session, project, layer)
        if found is None or found.dataset.kind != LAYER:
            bad(("dataset", "location_layer"), f"'{layer}' is not a layer")
    if mapping.get("location_key") and not layer:
        bad(
            ("mapping", "location_key"),
            "location keys need the dataset's location_layer (the layer they're in)",
        )
    dataset["existing"] = existing is not None


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
    project, my_role = _authorize(session, name, identity, settings, Role.WRITE)
    if (
        my_role is not None
        and not identity.has_any_role(settings.admin_roles)
        and project_roles(project.name)[my_role] not in identity.roles
    ):
        # The job acts with this token's roles (token exchange can't add any), so a
        # token from before the caller's project role would fail inside the job.
        raise APIError(
            status.HTTP_403_FORBIDDEN,
            f"Your access token doesn't carry your {my_role.value} role in "
            f"'{project.name}', so the load would run without it. Get a new token "
            "and retry; if a new token still lacks it, the role assignment is "
            f"missing: POST /v1/projects/{project.name}/provision restores it.",
            code="token_refresh_required",
        )
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
            caller_session=identity.session_id,
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


@router.get(
    "/{upload_id}/logs",
    operation_id="read_upload_logs",
    response_model=LogsOut,
    responses=_errors(401, 404),
)
def read_upload_logs(
    name: ProjectName,
    upload_id: UploadId,
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
    kube: KubeDep,
    after: Annotated[
        int | None, Query(description="Only lines after this one (its `id`)")
    ] = None,
    limit: Annotated[int, Query(ge=1, le=10_000)] = 1000,
):
    """A load's log, oldest first. Poll with ``after`` (the last line's ``id``) to
    follow it until ``ended``. Needs the read role."""
    project, _ = _authorize(session, name, identity, settings, Role.READ)
    upload = _upload(session, project, upload_id)
    _sync(session, kube, upload)
    lines = job_logs.read(session, upload.job_id, after, limit) if upload.job_id else []
    ended = upload.status not in (UploadStatus.QUEUED, UploadStatus.RUNNING)
    return LogsOut(lines=lines, ended=ended)


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
