"""Datasets: a project's tables, registered and managed through the API (Phase 2).

Each dataset is one Iceberg table in the project's namespace. The API creates and
drops it **as the caller** (their token against Polaris), so Polaris enforces the
caller's project roles; the control-plane DB records the declared columns that
writes are checked against.

Writes (``POST .../{dataset}/data``) send Arrow IPC, Parquet or JSON in the
request body (``src.formats``), are checked against the declared columns, and are committed as the caller,
with a lineage record each. They're for small batches: bulk loads and upserts go
through staged upload and a load job (Phase 3).

Access: listing and reading need the read role (everyone, for a system project);
registering and writing need write; deleting needs admin. Platform admins may act
anywhere.
"""

import json
from collections.abc import Callable
from typing import Annotated, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pyarrow as pa
import pyarrow.compute as pc
from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Path,
    Query,
    Request,
    Response,
    status,
)
from fastapi.responses import JSONResponse
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.auth import Identity
from src.config import Settings
from src.dataset_types import (
    BBOX,
    CROSSWALK,
    LAYER,
    SERIES,
    TABLE,
    TIMESERIES,
    TYPED,
    crosswalk_columns,
    derived_columns,
    layer_columns,
    series_columns,
    timeseries_columns,
)
from src.domains import convert, lookup
from src.errors import APIError
from src.expressions import compile_filter, overwrite_expression
from src.formats import (
    ARROW_STREAM,
    JSON,
    NDJSON,
    PARQUET,
    UnsupportedMediaType,
    decode,
    encode_binary,
    json_rows,
    negotiate,
)
from src.geometry import (
    GEOJSON,
    as_geojson,
    bbox_filter,
    feature_collection,
    json_geometry,
    prepare_layer,
    with_geoarrow,
    with_geoparquet,
)
from src.models import (
    Dataset,
    DatasetWrite,
    Period,
    Project,
    ProjectStatus,
    Role,
    Unit,
    Variable,
)
from src.notes import missing_note
from src.paging import BadCursor, decode_cursor, encode_cursor, fingerprint
from src.refs import qualify, referrers, resolve
from src.routes.projects import (
    IdentityDep,
    ProjectName,
    SessionDep,
    SettingsDep,
    _authorize,
    _errors,
)
from src.schemas import (
    PROJECT_NAME_PATTERN,
    CrosswalkCreate,
    DataPageOut,
    DatasetCreate,
    DatasetOut,
    DatasetRegistration,
    DatasetUpdate,
    DerivedCreate,
    LayerCreate,
    TimeseriesCreate,
    WriteOut,
)
from src.tables import (
    DataMismatch,
    Layout,
    NamespaceMissing,
    SnapshotGone,
    TableError,
    TableExists,
    TableForbidden,
    TableMissing,
    Tables,
    WriteConflict,
    conform,
    duplicate_keys,
    get_tables,
)

router = APIRouter(prefix="/v1/projects/{name}/datasets", tags=["datasets"])

TablesDep = Annotated[Tables | None, Depends(get_tables)]
DatasetName = Annotated[
    str, Path(pattern=PROJECT_NAME_PATTERN, description="Dataset name")
]

TOKEN_REFRESH = "token_refresh_required"


def _dataset(session: Session, project: Project, name: str) -> Dataset:
    dataset = session.scalar(
        select(Dataset).where(Dataset.project_id == project.id, Dataset.name == name)
    )
    if dataset is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found"
        )
    return dataset


def _require_tables(tables: Tables | None) -> Tables:
    if tables is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Table storage is not configured",
        )
    return tables


def _forbidden(project: Project, my_role: Role | None, needed: Role) -> APIError:
    """Polaris refused the caller although the control plane allowed them.

    The usual cause is a token issued before the caller got this project role
    (Polaris only activates roles the token carries), so a fresh token fixes it.
    """
    if my_role is not None and my_role.rank >= needed.rank:
        return APIError(
            status.HTTP_403_FORBIDDEN,
            f"Your access token predates your {my_role.value} role in "
            f"'{project.name}'. Get a new token and retry.",
            code=TOKEN_REFRESH,
        )
    return APIError(
        status.HTTP_403_FORBIDDEN,
        f"The catalog refused this operation on '{project.name}'",
    )


def _authorize_dataset_op(
    session: Session,
    name: str,
    identity: Identity,
    settings: Settings,
    min_role: Role,
) -> tuple[Project, Role | None]:
    project, my_role = _authorize(session, name, identity, settings, min_role)
    if project.status != ProjectStatus.READY:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"Project '{project.name}' is {project.status.value}; "
                f"retry with POST /v1/projects/{project.name}/provision"
            ),
        )
    return project, my_role


def _exists(session: Session, project: Project, name: str) -> bool:
    return (
        session.scalar(
            select(Dataset.id).where(
                Dataset.project_id == project.id, Dataset.name == name
            )
        )
        is not None
    )


def _create_table(
    tables: Tables,
    identity: Identity,
    project: Project,
    my_role: Role | None,
    name: str,
    columns: list[dict],
    key_columns: list[str] | None,
    layout: Layout | None = None,
) -> None:
    """Create a dataset's table as the caller, as an HTTP error on failure."""
    try:
        tables.create_table(
            identity.token, project.name, name, columns, key_columns, layout
        )
    except TableForbidden as exc:
        raise _forbidden(project, my_role, Role.WRITE) from exc
    except NamespaceMissing as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Project '{project.name}' has no namespace; re-provision it",
        ) from exc
    except TableExists as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"A table '{name}' already exists in '{project.name}' "
                "but isn't a registered dataset"
            ),
        ) from exc
    except TableError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="The catalog could not create the table",
        ) from exc


def _commit_new(session: Session, *datasets: Dataset) -> None:
    session.add_all(datasets)
    try:
        session.commit()
    except IntegrityError as exc:  # registered concurrently
        session.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Dataset '{datasets[-1].name}' already exists",
        ) from exc


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    operation_id="create_dataset",
    responses=_errors(401, 403, 404, 409, 502, 503),
)
def create_dataset(
    name: ProjectName,
    body: DatasetRegistration,
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
    tables: TablesDep,
) -> DatasetOut:
    """Register a dataset: creates its Iceberg table(s), as the caller.

    `type: timeseries` (warehouse data model v1) builds the columns, key and
    layout from the dataset's metadata, and a `<name>_series` series table unless
    `series_table` names an existing one. Untyped datasets (`type: table`, the
    default) are a test path: only platform admins may register them, unless
    UNTYPED_DATASETS_FOR_MEMBERS is on.
    """
    project, my_role = _authorize_dataset_op(
        session, name, identity, settings, Role.WRITE
    )
    if isinstance(body, TimeseriesCreate):
        return _create_timeseries(
            body, project, my_role, session, identity, _require_tables(tables)
        )
    if isinstance(body, LayerCreate | CrosswalkCreate):
        return _create_layer_or_crosswalk(
            body, project, my_role, session, identity, _require_tables(tables)
        )
    if isinstance(body, DerivedCreate):
        return _create_derived(
            body, project, my_role, session, identity, _require_tables(tables)
        )
    if not (
        settings.untyped_datasets_for_members
        or identity.has_any_role(settings.admin_roles)
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Untyped datasets are limited to platform admins",
        )
    return _create_table_dataset(body, project, my_role, session, identity, tables)


def _create_table_dataset(
    body: DatasetCreate,
    project: Project,
    my_role: Role | None,
    session: Session,
    identity: Identity,
    tables: Tables | None,
) -> DatasetOut:
    tables = _require_tables(tables)
    if _exists(session, project, body.name):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Dataset '{body.name}' already exists",
        )
    columns = [col.model_dump() for col in body.columns]
    _create_table(
        tables, identity, project, my_role, body.name, columns, body.key_columns
    )
    dataset = Dataset(
        project_id=project.id,
        name=body.name,
        kind=TABLE,
        description=body.description,
        columns=columns,
        key_columns=body.key_columns,
        table_identifier=f"{project.name}.{body.name}",
        created_by=identity.subject,
    )
    _commit_new(session, dataset)
    session.refresh(dataset)
    return DatasetOut.model_validate(dataset)


def _timeseries_metadata(
    body: TimeseriesCreate, project: Project, session: Session
) -> tuple[dict, str]:
    """Check the metadata against the project's domains; returns the stored
    metadata and the period's form."""
    variable = lookup(session, Variable, body.variable, project.id)
    if variable is None:
        raise _unprocessable(f"unknown variable '{body.variable}'")
    if body.unit is not None and body.unit != variable.canonical_unit:
        raise _unprocessable(
            f"'{body.variable}' is stored in {variable.canonical_unit}; register "
            "with that unit (writes in other units are converted)"
        )
    period_name = body.period or ("instant" if body.statistic == "inst" else None)
    if period_name is None:
        raise _unprocessable(f"statistic '{body.statistic}' needs a period")
    period = lookup(session, Period, period_name, project.id)
    if period is None:
        raise _unprocessable(f"unknown period '{period_name}'")
    if (period.form == "instant") != (body.statistic == "inst"):
        raise _unprocessable(
            "an instant period goes with statistic inst, and only with it"
        )
    if body.location_layer:
        _layer(session, project, body.location_layer)
    metadata = {
        "role": body.role,
        "variable": variable.name,
        "unit": variable.canonical_unit,
        "statistic": body.statistic,
        "period": period.name,
        "optional_columns": body.optional_columns,
        "series_table": body.series_table or f"{body.name}_series",
        "location_layer": body.location_layer,
        "time_partition": body.time_partition,
        "source": body.source,
    }
    return metadata, period.form


def _create_timeseries(
    body: TimeseriesCreate,
    project: Project,
    my_role: Role | None,
    session: Session,
    identity: Identity,
    tables: Tables,
) -> DatasetOut:
    metadata, period_form = _timeseries_metadata(body, project, session)
    if _exists(session, project, body.name):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Dataset '{body.name}' already exists",
        )
    series_name = metadata["series_table"]
    new_series = None
    if body.series_table:
        shared = session.scalar(
            select(Dataset).where(
                Dataset.project_id == project.id, Dataset.name == series_name
            )
        )
        if shared is None or shared.kind != SERIES:
            raise _unprocessable(f"'{series_name}' is not a series table here")
    else:
        if _exists(session, project, series_name):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Dataset '{series_name}' already exists",
            )
        columns = series_columns([c.model_dump() for c in body.series_attributes])
        _create_table(
            tables,
            identity,
            project,
            my_role,
            series_name,
            columns,
            ["series_key"],
            Layout(sort=("series_key",), properties={"teehr.type": SERIES}),
        )
        new_series = Dataset(
            project_id=project.id,
            name=series_name,
            kind=SERIES,
            description=f"Series of {body.name}",
            columns=columns,
            key_columns=["series_key"],
            table_identifier=f"{project.name}.{series_name}",
            created_by=identity.subject,
        )

    columns, key = timeseries_columns(
        metadata["optional_columns"],
        period_form,
        [c.model_dump() for c in body.extra_columns],
    )
    time_column = (
        "reference_time"
        if "reference_time" in metadata["optional_columns"]
        else "value_time"
    )
    partition = (
        None if body.time_partition == "none" else (time_column, body.time_partition)
    )
    sort = tuple(c for c in ("series_key", "reference_time", "value_time") if c in key)
    properties = {"teehr.type": TIMESERIES} | {
        f"teehr.{k}": ",".join(v) if isinstance(v, list) else str(v)
        for k, v in metadata.items()
        if v not in (None, [])
    }
    try:
        _create_table(
            tables,
            identity,
            project,
            my_role,
            body.name,
            columns,
            key,
            Layout(partition=partition, sort=sort, properties=properties),
        )
    except HTTPException:
        if new_series is not None:  # don't leave an orphan series table
            _drop_quietly(tables, identity, project, series_name)
        raise
    dataset = Dataset(
        project_id=project.id,
        name=body.name,
        kind=TIMESERIES,
        description=body.description,
        columns=columns,
        key_columns=key,
        properties=metadata,
        table_identifier=f"{project.name}.{body.name}",
        created_by=identity.subject,
    )
    _commit_new(session, *(d for d in (new_series, dataset) if d is not None))
    session.refresh(dataset)
    return DatasetOut.model_validate(dataset)


def _layer(session: Session, project: Project, ref: str):
    """The layer a reference names, or 422."""
    found = resolve(session, project, ref)
    if found is None or found.dataset.kind != LAYER:
        raise _unprocessable(f"'{ref}' is not a layer (use <name> or public.<name>)")
    return found


def _create_layer_or_crosswalk(
    body: LayerCreate | CrosswalkCreate,
    project: Project,
    my_role: Role | None,
    session: Session,
    identity: Identity,
    tables: Tables,
) -> DatasetOut:
    """A layer (keyed by location_key) or a crosswalk (keyed by left_key and
    right_key, between two timeseries datasets' series tables)."""
    if isinstance(body, LayerCreate):
        kind, key = LAYER, ["location_key"]
        columns = layer_columns([c.model_dump() for c in body.attributes])
        metadata: dict = {"source": body.source}
    else:
        kind, key = CROSSWALK, ["left_key", "right_key"]
        columns = crosswalk_columns([c.model_dump() for c in body.extra_columns])
        metadata = {"left": body.left, "right": body.right}
        for side in ("left", "right"):
            ref = getattr(body, side)
            found = resolve(session, project, ref)
            if found is None or found.dataset.kind != TIMESERIES:
                raise _unprocessable(
                    f"{side}: '{ref}' is not a timeseries dataset "
                    "(use <name> or public.<name>)"
                )
            series = (found.dataset.properties or {})["series_table"]
            metadata[f"{side}_series_table"] = qualify(found.project, project, series)
        metadata["source"] = body.source
    if _exists(session, project, body.name):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Dataset '{body.name}' already exists",
        )
    properties = {"teehr.type": kind} | {
        f"teehr.{k}": str(v) for k, v in metadata.items() if v is not None
    }
    if kind == LAYER:
        # GeoParquet-style: what the geometry column holds, for readers.
        properties |= {"teehr.geometry_encoding": "WKB", "teehr.crs": "EPSG:4326"}
    _create_table(
        tables,
        identity,
        project,
        my_role,
        body.name,
        columns,
        key,
        Layout(sort=tuple(key), properties=properties),
    )
    dataset = Dataset(
        project_id=project.id,
        name=body.name,
        kind=kind,
        description=body.description,
        columns=columns,
        key_columns=key,
        properties=metadata,
        table_identifier=f"{project.name}.{body.name}",
        created_by=identity.subject,
    )
    _commit_new(session, dataset)
    session.refresh(dataset)
    return DatasetOut.model_validate(dataset)


def _create_derived(
    body: DerivedCreate,
    project: Project,
    my_role: Role | None,
    session: Session,
    identity: Identity,
    tables: Tables,
) -> DatasetOut:
    """A climatology, percentiles or naive_scale dataset of an observed timeseries
    (ADR-0019): its columns from teehr-core, its variable and unit the observed
    dataset's. Empty until a build fills it."""
    found = resolve(session, project, body.observed)
    props = (found.dataset.properties or {}) if found is not None else {}
    if found is None or found.dataset.kind != TIMESERIES:
        raise _unprocessable(
            f"observed: '{body.observed}' is not a timeseries dataset "
            "(use <name> or public.<name>)"
        )
    if props.get("role") != "observed":
        raise _unprocessable(
            f"observed: '{body.observed}' is {props.get('role')!r}; a {body.type} is "
            "built from observations"
        )
    if _exists(session, project, body.name):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Dataset '{body.name}' already exists",
        )
    columns, key = derived_columns(body.type)
    metadata = {
        "observed": qualify(found.project, project, found.dataset.name),
        "variable": props["variable"],
        "unit": props["unit"],
    }
    properties = {"teehr.type": body.type, "teehr.role": "auxiliary"} | {
        f"teehr.{k}": str(v) for k, v in metadata.items()
    }
    _create_table(
        tables, identity, project, my_role, body.name, list(columns), key,
        Layout(sort=("series_key",), properties=properties),
    )  # fmt: skip
    dataset = Dataset(
        project_id=project.id,
        name=body.name,
        kind=body.type,
        description=body.description,
        columns=list(columns),
        key_columns=key,
        properties=metadata,
        table_identifier=f"{project.name}.{body.name}",
        created_by=identity.subject,
    )
    _commit_new(session, dataset)
    session.refresh(dataset)
    return DatasetOut.model_validate(dataset)


def _drop_quietly(tables: Tables, identity: Identity, project: Project, name: str):
    try:
        tables.drop_table(identity.token, project.name, name)
    except TableError:
        pass


@router.get("", operation_id="list_datasets", responses=_errors(401, 404, 503))
def list_datasets(
    name: ProjectName, session: SessionDep, identity: IdentityDep, settings: SettingsDep
) -> list[DatasetOut]:
    project, _ = _authorize(session, name, identity, settings, Role.READ)
    rows = session.scalars(
        select(Dataset).where(Dataset.project_id == project.id).order_by(Dataset.name)
    )
    return [DatasetOut.model_validate(d) for d in rows]


@router.get("/{dataset}", operation_id="get_dataset", responses=_errors(401, 404, 503))
def get_dataset(
    name: ProjectName,
    dataset: DatasetName,
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
) -> DatasetOut:
    project, _ = _authorize(session, name, identity, settings, Role.READ)
    return DatasetOut.model_validate(_dataset(session, project, dataset))


@router.patch(
    "/{dataset}", operation_id="update_dataset", responses=_errors(401, 403, 404, 503)
)
def update_dataset(
    name: ProjectName,
    dataset: DatasetName,
    body: DatasetUpdate,
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
) -> DatasetOut:
    """Change a dataset's description (the record only: the table is untouched)."""
    project, _ = _authorize(session, name, identity, settings, Role.WRITE)
    record = _dataset(session, project, dataset)
    for field_name, value in body.model_dump(exclude_unset=True).items():
        setattr(record, field_name, value)
    session.commit()
    session.refresh(record)
    return DatasetOut.model_validate(record)


@router.delete(
    "/{dataset}",
    status_code=status.HTTP_204_NO_CONTENT,
    operation_id="delete_dataset",
    responses=_errors(401, 403, 404, 409, 502, 503),
)
def delete_dataset(
    name: ProjectName,
    dataset: DatasetName,
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
    tables: TablesDep,
) -> Response:
    """Delete a dataset: drops its table and data files, as the caller.

    Deleting a timeseries also deletes its series table once no other timeseries
    uses it; a series table still in use can't be deleted on its own.
    """
    project, my_role = _authorize_dataset_op(
        session, name, identity, settings, Role.ADMIN
    )
    tables = _require_tables(tables)
    record = _dataset(session, project, dataset)
    used_by = referrers(session, project, record.name)
    if used_by:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"'{record.name}' is referred to by {', '.join(used_by)}; "
                "delete or change those first"
            ),
        )
    users = _series_users(session, project)
    if record.kind == SERIES and users.get(record.name):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"'{record.name}' is the series table of "
                f"{', '.join(sorted(users[record.name]))}; delete those first"
            ),
        )
    doomed = [record]
    if record.kind == TIMESERIES:
        series_name = (record.properties or {}).get("series_table")
        if users.get(series_name) == {record.name}:
            series = session.scalar(
                select(Dataset).where(
                    Dataset.project_id == project.id, Dataset.name == series_name
                )
            )
            if series is not None:
                doomed.append(series)
    for target in doomed:
        try:
            tables.drop_table(identity.token, project.name, target.name)
        except TableForbidden as exc:
            raise _forbidden(project, my_role, Role.ADMIN) from exc
        except TableError as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="The catalog could not drop the table",
            ) from exc
        session.delete(target)
        session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


def _series_users(session: Session, project: Project) -> dict[str, set[str]]:
    """Series table name -> the timeseries datasets that use it."""
    users: dict[str, set[str]] = {}
    for ts in session.scalars(
        select(Dataset).where(
            Dataset.project_id == project.id, Dataset.kind == TIMESERIES
        )
    ):
        series = (ts.properties or {}).get("series_table")
        users.setdefault(series, set()).add(ts.name)
    return users


async def _payload(request: Request, settings: SettingsDep) -> tuple[str, bytes]:
    """The request body and its media type, refusing oversized bodies early."""
    limit = settings.max_write_bytes
    length = request.headers.get("content-length")
    if length and length.isdigit() and int(length) > limit:
        raise _too_large(limit)
    body = await request.body()
    if len(body) > limit:
        raise _too_large(limit)
    media_type = request.headers.get("content-type", "").split(";")[0].strip()
    return media_type, body


def _too_large(limit: int) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_413_CONTENT_TOO_LARGE,
        detail=(
            f"Writes through the API are limited to {limit // (1024 * 1024)} MiB; "
            "load larger data with a staged upload"
        ),
    )


def _unprocessable(detail: str) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=detail
    )


@router.post(
    "/{dataset}/data",
    status_code=status.HTTP_201_CREATED,
    operation_id="write_dataset",
    responses=_errors(401, 403, 404, 409, 413, 415, 502, 503),
    openapi_extra={
        "requestBody": {
            "required": True,
            "description": (
                "The rows: Arrow IPC stream, Parquet, NDJSON, or a JSON array of "
                "objects. JSON is parsed with the declared column types; "
                "timestamptz values need a zone offset (Z or +05:00)."
            ),
            "content": {
                ARROW_STREAM: {"schema": {"type": "string", "format": "binary"}},
                PARQUET: {"schema": {"type": "string", "format": "binary"}},
                NDJSON: {"schema": {"type": "string"}},
                JSON: {"schema": {"type": "array", "items": {"type": "object"}}},
            },
        }
    },
)
def write_dataset(
    name: ProjectName,
    dataset: DatasetName,
    payload: Annotated[tuple[str, bytes], Depends(_payload)],
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
    tables: TablesDep,
    mode: Annotated[
        Literal["append", "overwrite", "upsert"] | None,
        Query(
            description=(
                "upsert (replace rows with the same key, add the rest; the default "
                "for timeseries and series tables; on a series table, columns "
                "left out keep their values), append (the default for untyped "
                "datasets; not allowed on typed ones), or overwrite. Timeseries "
                "writes must send every column (null where there's no value)."
            )
        ),
    ] = None,
    filter: Annotated[
        str | None,
        Query(
            max_length=2000,
            description=(
                "overwrite only: replace only rows matching this filter, e.g. "
                "series_key = 'usgs-01' AND value_time >= '2026-01-01T00:00:00Z' "
                "(comparisons, IN, BETWEEN, IS NULL, AND/OR/NOT only); without it "
                "the whole table is replaced"
            ),
        ),
    ] = None,
    timezone: Annotated[
        str | None,
        Query(
            max_length=64,
            description=(
                "IANA time zone (e.g. America/Denver) that times without one are "
                "in, for timestamptz columns; they're stored as UTC. Without it, "
                "such times are refused."
            ),
        ),
    ] = None,
    unit: Annotated[
        str | None,
        Query(
            max_length=64,
            description=(
                "Timeseries: the unit of `value` (UCUM, e.g. [cft_i]/s); converted "
                "to the dataset's unit. Default: the dataset's unit."
            ),
        ),
    ] = None,
) -> WriteOut:
    """Write rows to a dataset, as the caller. One snapshot per request.

    For a timeseries, values are converted to the dataset's unit, and series keys
    not yet in its series table are added there first (with the dataset's default
    layer; existing series rows are left alone).
    """
    project, my_role = _authorize_dataset_op(
        session, name, identity, settings, Role.WRITE
    )
    tables = _require_tables(tables)
    record = _dataset(session, project, dataset)
    typed = record.kind in TYPED
    # Typed datasets upsert on their key; one without a key (percentiles) appends.
    mode = mode or ("upsert" if typed and record.key_columns else "append")
    if typed and mode == "append":
        raise _unprocessable(
            f"{record.kind} datasets take upsert or overwrite, not append"
        )
    if mode == "upsert" and not record.key_columns:
        raise _unprocessable("upsert needs a dataset with key_columns")
    if filter is not None and mode != "overwrite":
        raise _unprocessable("filter only applies to mode=overwrite")
    if unit is not None and record.kind != TIMESERIES:
        raise _unprocessable("unit only applies to timeseries datasets")
    units = _write_units(session, project, record, unit)
    if timezone is not None:
        try:
            ZoneInfo(timezone)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise _unprocessable(f"unknown time zone '{timezone}'") from exc
    columns = record.columns or []
    try:
        data = (
            _decode_layer(*payload, columns)
            if record.kind == LAYER
            else decode(*payload, columns)
        )
        if data.num_rows == 0:
            raise _unprocessable("No rows to write")
        provided = list(data.column_names)
        if record.kind == LAYER:
            data = prepare_layer(data)
        if record.kind == TIMESERIES:
            # A timeseries row is a whole record from its source: leaving a column
            # out would null it (e.g. wipe a quality code), so it's refused.
            absent = [c["name"] for c in columns if c["name"] not in provided]
            if absent:
                raise DataMismatch(
                    f"send every column of the dataset; missing: {', '.join(absent)}"
                    " (null where there's no value)"
                )
        data = conform(data, columns, timezone)
        expression = overwrite_expression(filter, columns)
        if mode == "upsert" or typed:
            repeated = duplicate_keys(data, record.key_columns or [])
            if repeated:
                raise DataMismatch(
                    f"{repeated} key values ({', '.join(record.key_columns)}) "
                    "occur more than once in this batch"
                )
        if units is not None and units[0].symbol != units[1].symbol:
            index = data.schema.get_field_index("value")
            data = data.set_column(
                index, data.schema.field(index), convert(data["value"], *units)
            )
    except DataMismatch as exc:
        raise _unprocessable(str(exc)) from exc
    except UnsupportedMediaType as exc:
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE, detail=str(exc)
        ) from exc
    notes: list[str] = []
    try:
        if record.kind == CROSSWALK:
            notes = _crosswalk_notes(session, project, record, data, tables, identity)
        elif record.kind == SERIES:
            notes = _location_notes(
                session, project, record, data, provided, tables, identity
            )
        series_added = (
            _add_series(session, project, record, data, tables, identity)
            if record.kind == TIMESERIES
            else None
        )
        if record.kind == SERIES and mode == "upsert":
            # Series rows are attributes filled in a piece at a time: columns
            # left out keep their current values.
            result = tables.merge(
                identity.token,
                project.name,
                record.name,
                data,
                record.key_columns or [],
                provided,
            )
        else:
            result = tables.write(
                identity.token, project.name, record.name, data, mode, expression
            )
    except TableForbidden as exc:
        raise _forbidden(project, my_role, Role.WRITE) from exc
    except TableMissing as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Dataset '{record.name}' has no table in the catalog",
        ) from exc
    except WriteConflict as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Another write committed first; retry",
        ) from exc
    except TableError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="The catalog could not commit the write",
        ) from exc
    months = None
    if record.kind == TIMESERIES and mode != "overwrite":
        stamps = pc.strftime(data["value_time"], format="%Y-%m", locale="C")
        months = sorted(m for m in pc.unique(stamps).to_pylist() if m)
    write = DatasetWrite(
        dataset_id=record.id,
        mode=mode,
        filter=filter,
        rows=data.num_rows,
        months=months,
        unit=units and units[0].symbol,
        series_added=series_added,
        notes=notes or None,
        snapshot_id=result.snapshot_id,
        written_by=identity.subject,
    )
    session.add(write)
    session.commit()
    session.refresh(write)
    return WriteOut.model_validate(write)


def _decode_layer(media_type: str, body: bytes, columns: list[dict]) -> pa.Table:
    """A layer write's rows. JSON carries geometry as GeoJSON geometry objects or
    WKT; the rest of each row is read with the declared types as usual."""
    if media_type not in (JSON, NDJSON):
        return decode(media_type, body, columns)
    try:
        if media_type == JSON:
            rows = json.loads(body)
        else:
            rows = [json.loads(line) for line in body.splitlines() if line.strip()]
    except ValueError as exc:
        raise DataMismatch(f"could not read the {media_type} body: {exc}") from exc
    if not isinstance(rows, list) or not all(isinstance(r, dict) for r in rows):
        raise DataMismatch("JSON body must be an array of objects")
    if not rows:
        return pa.table({})
    rest, wkt = json_geometry(rows)
    plain = [c for c in columns if c["name"] not in ("geometry", *BBOX)]
    data = decode(JSON, json.dumps(rest).encode(), plain)
    if data.num_rows != len(rows):  # every row was empty apart from geometry
        data = pa.table({"__rows": pa.nulls(len(rows))}).drop_columns(["__rows"])
    return data.append_column("geometry", pa.array(wkt, pa.string()))


def _missing(
    tables: Tables,
    identity: Identity,
    namespace: str,
    name: str,
    column: str,
    values: list[str],
) -> list[str]:
    """Which of ``values`` aren't in table ``name``'s ``column`` (sorted)."""
    present = tables.existing_keys(identity.token, namespace, name, column, values)
    return sorted(set(values) - present)


def _crosswalk_notes(
    session: Session,
    project: Project,
    record: Dataset,
    data: pa.Table,
    tables: Tables,
    identity: Identity,
) -> list[str]:
    """Keys not (yet) in their side's series table: reported, not refused, so
    timeseries and their crosswalk can be loaded in either order."""
    notes = []
    for side in ("left", "right"):
        ref = (record.properties or {})[f"{side}_series_table"]
        found = resolve(session, project, ref)
        keys = pc.unique(data[f"{side}_key"]).to_pylist()
        if found is None:
            notes.append(f"{side} series table '{ref}' no longer exists")
            continue
        missing = _missing(
            tables, identity, found.namespace, found.dataset.name, "series_key", keys
        )
        if missing:
            notes.append(missing_note(missing, len(keys), f"{side}_key values", ref))
    return notes


def _default_layers(session: Session, project: Project, series: Dataset) -> set[str]:
    """The default layers of the timeseries that use this series table."""
    return {
        (ts.properties or {}).get("location_layer")
        for ts in session.scalars(
            select(Dataset).where(
                Dataset.project_id == project.id, Dataset.kind == TIMESERIES
            )
        )
        if (ts.properties or {}).get("series_table") == series.name
    } - {None}


def _location_notes(
    session: Session,
    project: Project,
    record: Dataset,
    data: pa.Table,
    provided: list[str],
    tables: Tables,
    identity: Identity,
) -> list[str]:
    """Series rows: a location_layer must name a layer (refused if not); location
    keys not in their layer are reported. Rows that don't say their layer use the
    timeseries' default layer, when there's exactly one."""
    if "location_key" not in provided and "location_layer" not in provided:
        return []
    defaults = _default_layers(session, project, record)
    fallback = next(iter(defaults)) if len(defaults) == 1 else None
    layers = (
        data["location_layer"].to_pylist()
        if "location_layer" in provided
        else [None] * data.num_rows
    )
    keys = (
        data["location_key"].to_pylist()
        if "location_key" in provided
        else [None] * data.num_rows
    )
    by_layer: dict[str, set[str]] = {}
    for layer, key in zip(layers, keys, strict=True):
        layer = layer or fallback
        if layer is not None:
            group = by_layer.setdefault(layer, set())
            if key is not None:
                group.add(key)
    notes = []
    for ref, wanted in sorted(by_layer.items()):
        found = _layer(session, project, ref)
        if not wanted:
            continue
        missing = _missing(
            tables,
            identity,
            found.namespace,
            found.dataset.name,
            "location_key",
            sorted(wanted),
        )
        if missing:
            notes.append(missing_note(missing, len(wanted), "location keys", ref))
    return notes


def _write_units(
    session: Session, project: Project, record: Dataset, unit: str | None
) -> tuple[Unit, Unit] | None:
    """(the unit values arrive in, the dataset's unit), for a timeseries."""
    if record.kind != TIMESERIES:
        return None
    canonical = (record.properties or {})["unit"]
    target = lookup(session, Unit, canonical, project.id)
    source = lookup(session, Unit, unit, project.id) if unit else target
    if source is None or target is None:
        raise _unprocessable(f"unknown unit '{unit or canonical}'")
    if source.dimension != target.dimension:
        raise _unprocessable(
            f"{source.symbol} ({source.dimension}) doesn't convert to "
            f"{target.symbol} ({target.dimension})"
        )
    return source, target


def _add_series(
    session: Session,
    project: Project,
    record: Dataset,
    data,
    tables: Tables,
    identity: Identity,
) -> int:
    """Add the batch's series keys that the series table doesn't have yet."""
    properties = record.properties or {}
    series = session.scalar(
        select(Dataset).where(
            Dataset.project_id == project.id,
            Dataset.name == properties["series_table"],
        )
    )
    if series is None:
        raise TableMissing(f"series table '{properties['series_table']}'")
    keys = pc.unique(data["series_key"])
    rows = {"series_key": keys}
    if properties.get("location_layer"):
        rows["location_layer"] = pa.array(
            [properties["location_layer"]] * len(keys), pa.string()
        )
    new = conform(pa.table(rows), series.columns or [])
    return tables.insert_missing(identity.token, project.name, series.name, new)


@router.get(
    "/{dataset}/writes",
    operation_id="list_dataset_writes",
    responses=_errors(401, 404, 503),
)
def list_dataset_writes(
    name: ProjectName,
    dataset: DatasetName,
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
) -> list[WriteOut]:
    """The dataset's writes through the API, newest first."""
    project, _ = _authorize(session, name, identity, settings, Role.READ)
    record = _dataset(session, project, dataset)
    rows = session.scalars(
        select(DatasetWrite)
        .where(DatasetWrite.dataset_id == record.id)
        .order_by(DatasetWrite.written_at.desc(), DatasetWrite.id)
        .limit(limit)
    )
    return [WriteOut.model_validate(w) for w in rows]


NEXT_CURSOR = "X-Next-Cursor"
TOTAL_ROWS = "X-Total-Rows"
SNAPSHOT_ID = "X-Snapshot-Id"


def _bad_request(detail: str) -> HTTPException:
    return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=detail)


def _columns(raw: str | None, declared: list[dict]) -> tuple[str, ...] | None:
    if not raw:
        return None
    names = tuple(dict.fromkeys(c.strip() for c in raw.split(",") if c.strip()))
    known = {c["name"] for c in declared}
    unknown = [c for c in names if c not in known]
    if unknown:
        raise _unprocessable(f"unknown columns: {', '.join(unknown)}")
    return names or None


@router.get(
    "/{dataset}/data",
    operation_id="read_dataset",
    response_model=DataPageOut,
    responses={
        **_errors(400, 401, 403, 404, 409, 410, 502, 503),
        200: {
            "description": (
                "A page of rows: JSON by default, or Arrow / Parquet when the "
                "Accept header asks. Every page carries X-Snapshot-Id."
            ),
            "content": {
                ARROW_STREAM: {"schema": {"type": "string", "format": "binary"}},
                PARQUET: {"schema": {"type": "string", "format": "binary"}},
                GEOJSON: {"schema": {"type": "object"}},
            },
        },
    },
)
def read_dataset(
    request: Request,
    name: ProjectName,
    dataset: DatasetName,
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
    tables: TablesDep,
    columns: Annotated[
        str | None,
        Query(description="Comma-separated columns to return (default: all)"),
    ] = None,
    filter: Annotated[
        str | None,
        Query(
            max_length=2000,
            description=(
                "Rows to return, in the shared expression language (a SQL subset: "
                "comparisons, AND/OR/NOT, IN, BETWEEN, IS NULL, LIKE, arithmetic, "
                "CASE, and allow-listed functions such as hour(), date_trunc(), "
                "ln(), coalesce()), e.g. series_key = 'usgs-01' AND "
                "hour(value_time) = 0. Numbers are in the column's stored unit."
            ),
        ),
    ] = None,
    limit: Annotated[
        int | None, Query(ge=1, description="Rows per page (default PAGE_SIZE)")
    ] = None,
    cursor: Annotated[
        str | None, Query(description="From the previous page's next_cursor")
    ] = None,
    count: Annotated[
        bool, Query(description="Also count the rows matching the filter")
    ] = False,
    bbox: Annotated[
        str | None,
        Query(
            description=(
                "Layers: minx,miny,maxx,maxy (longitude/latitude); the locations "
                "whose bounds overlap it"
            )
        ),
    ] = None,
) -> Response:
    """Read a dataset's rows, as the caller, a page at a time.

    Every page of one read sees the same snapshot (the cursor pins it), so writes
    landing meanwhile don't shift or duplicate rows. For bulk reads use Arrow or
    Parquet; large exports belong in jobs.
    """
    return _read_dataset(
        request, name, dataset, session, identity, settings, tables,
        columns=columns, filter=filter, limit=limit, cursor=cursor, count=count,
        bbox=bbox,
    )  # fmt: skip


#: Adds a page's places: a ``geometry`` column (WKB) and attribute columns.
Placer = Callable[[pa.Table], pa.Table]


def _read_dataset(
    request: Request,
    name: str,
    dataset: str,
    session: Session,
    identity: Identity,
    settings: Settings,
    tables: Tables | None,
    *,
    columns: str | None,
    filter: str | None,
    limit: int | None,
    cursor: str | None,
    count: bool,
    bbox: str | None,
    place: Placer | None = None,
) -> Response:
    """:func:`read_dataset`, with ``place`` giving each page's rows a geometry
    (a run's results with ``output.geometry``): then JSON rows carry GeoJSON
    geometry, GeoJSON is a feature collection, Arrow and Parquet are geo-tagged."""
    project, my_role = _authorize_dataset_op(
        session, name, identity, settings, Role.READ
    )
    tables = _require_tables(tables)
    record = _dataset(session, project, dataset)
    declared = record.columns or []
    selected = _columns(columns, declared)
    accept = request.headers.get("accept") or ""
    geojson = GEOJSON in accept
    if record.kind != LAYER and bbox is not None:
        raise _unprocessable("bbox applies to layers only")
    if record.kind != LAYER and place is None and geojson:
        raise _unprocessable(
            "GeoJSON applies to layers, and to results with output.geometry"
        )
    if geojson and selected and "geometry" not in selected and place is None:
        selected = (*selected, "geometry")
    try:
        if bbox is not None:
            spatial = bbox_filter(bbox)
            filter = f"({filter}) and {spatial}" if filter else spatial
        compiled = compile_filter(filter, declared) if filter else None
    except DataMismatch as exc:
        raise _unprocessable(str(exc)) from exc
    expression = compiled.pushdown if compiled else None
    # What the scan can't apply is applied to the rows it returns, so the read
    # takes every row the scan matches (and the columns the rest needs).
    residual = compiled is not None and compiled.residual is not None
    read_columns = selected
    if residual and selected:
        read_columns = tuple(dict.fromkeys([*selected, *compiled.residual_columns]))

    media_type = JSON if geojson else negotiate(accept)
    cap = (
        settings.max_page_rows_json
        if media_type == JSON
        else settings.max_page_rows_binary
    )
    page = limit or settings.page_size
    if page > cap:
        raise _bad_request(f"limit must be <= {cap} for {media_type}")
    query = fingerprint(filter, selected)
    snapshot_id, offset = None, 0
    if cursor:
        try:
            snapshot_id, offset = decode_cursor(cursor, query)
        except BadCursor as exc:
            raise _bad_request(str(exc)) from exc

    try:
        result = tables.read(
            identity.token,
            project.name,
            record.name,
            columns=read_columns,
            row_filter=expression,
            snapshot_id=snapshot_id,
            limit=None if residual else offset + page + 1,
        )
        matched = result.rows
        if residual:
            matched = compiled.apply(matched)
            if selected:
                matched = matched.select(list(selected))
        if not count:
            total = None
        elif residual:
            total = matched.num_rows
        else:
            total = tables.count(
                identity.token,
                project.name,
                record.name,
                expression,
                result.snapshot_id,
            )
    except TableForbidden as exc:
        raise _forbidden(project, my_role, Role.READ) from exc
    except TableMissing as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Dataset '{record.name}' has no table in the catalog",
        ) from exc
    except SnapshotGone as exc:
        raise HTTPException(
            status_code=status.HTTP_410_GONE,
            detail="This read's snapshot has expired; start the read again",
        ) from exc
    except DataMismatch as exc:  # the filter's rest, evaluated on the rows
        raise _unprocessable(str(exc)) from exc
    except TableError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="The catalog could not read the table",
        ) from exc

    rows = matched.slice(offset, page)
    if place is not None:
        rows = place(rows)
    more = matched.num_rows > offset + page
    next_cursor = (
        encode_cursor(result.snapshot_id, offset + page, query)
        if more and result.snapshot_id is not None
        else None
    )
    headers = {}
    if result.snapshot_id is not None:
        headers[SNAPSHOT_ID] = str(result.snapshot_id)
    has_geometry = "geometry" in rows.column_names and (
        record.kind == LAYER or place is not None
    )
    if media_type == JSON:
        plain = rows.drop_columns(["geometry"]) if has_geometry else rows
        out = json_rows(plain)
        geometry = as_geojson(rows["geometry"]) if has_geometry else None
        if geojson:
            if next_cursor:
                headers[NEXT_CURSOR] = next_cursor
            if total is not None:
                headers[TOTAL_ROWS] = str(total)
            return JSONResponse(
                feature_collection(out, geometry),
                media_type=GEOJSON,
                headers=headers,
            )
        if has_geometry:
            for row, geom in zip(out, geometry, strict=True):
                row["geometry"] = geom
        body = {"rows": out, "next_cursor": next_cursor}
        if total is not None:
            body["total"] = total
        return JSONResponse(body, headers=headers)
    if next_cursor:
        headers[NEXT_CURSOR] = next_cursor
    if total is not None:
        headers[TOTAL_ROWS] = str(total)
    if has_geometry and media_type == PARQUET:
        rows = with_geoparquet(rows)
    elif has_geometry and media_type == ARROW_STREAM:
        rows = with_geoarrow(rows)
    return Response(
        encode_binary(rows, media_type), media_type=media_type, headers=headers
    )
