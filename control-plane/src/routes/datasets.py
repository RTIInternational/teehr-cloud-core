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

from typing import Annotated, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

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
from src.errors import APIError
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
from src.models import Dataset, DatasetWrite, Project, ProjectStatus, Role
from src.paging import BadCursor, decode_cursor, encode_cursor, fingerprint
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
    DataPageOut,
    DatasetCreate,
    DatasetOut,
    WriteOut,
)
from src.tables import (
    DataMismatch,
    NamespaceMissing,
    SnapshotGone,
    TableError,
    TableExists,
    TableForbidden,
    TableMissing,
    Tables,
    WriteConflict,
    conform,
    get_tables,
    parse_filter,
)

router = APIRouter(prefix="/v1/projects/{name}/datasets", tags=["datasets"])

TablesDep = Annotated[Tables | None, Depends(get_tables)]
DatasetName = Annotated[
    str, Path(pattern=PROJECT_NAME_PATTERN, description="Dataset name")
]

TOKEN_REFRESH = "token_refresh_required"
DATASET_KIND = "table"  # typed datasets (timeseries, series, ...) come later


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


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    operation_id="create_dataset",
    responses=_errors(401, 403, 404, 409, 502, 503),
)
def create_dataset(
    name: ProjectName,
    body: DatasetCreate,
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
    tables: TablesDep,
) -> DatasetOut:
    """Register a dataset: creates its Iceberg table, as the caller.

    Datasets are untyped (`table`) for now, a test path until typed kinds
    (timeseries, ...) exist: only platform admins may register them, unless
    UNTYPED_DATASETS_FOR_MEMBERS is on.
    """
    project, my_role = _authorize_dataset_op(
        session, name, identity, settings, Role.WRITE
    )
    if not (
        settings.untyped_datasets_for_members
        or identity.has_any_role(settings.admin_roles)
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                "Untyped datasets are limited to platform admins until typed "
                "datasets (timeseries, ...) are available"
            ),
        )
    tables = _require_tables(tables)
    if session.scalar(
        select(Dataset.id).where(
            Dataset.project_id == project.id, Dataset.name == body.name
        )
    ):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Dataset '{body.name}' already exists",
        )
    columns = [col.model_dump() for col in body.columns]
    try:
        tables.create_table(
            identity.token, project.name, body.name, columns, body.key_columns
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
                f"A table '{body.name}' already exists in '{project.name}' "
                "but isn't a registered dataset"
            ),
        ) from exc
    except TableError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="The catalog could not create the table",
        ) from exc

    dataset = Dataset(
        project_id=project.id,
        name=body.name,
        kind=DATASET_KIND,
        description=body.description,
        columns=columns,
        key_columns=body.key_columns,
        table_identifier=f"{project.name}.{body.name}",
        created_by=identity.subject,
    )
    session.add(dataset)
    try:
        session.commit()
    except IntegrityError as exc:  # registered concurrently
        session.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Dataset '{body.name}' already exists",
        ) from exc
    session.refresh(dataset)
    return DatasetOut.model_validate(dataset)


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
    """Delete a dataset: drops its table and data files, as the caller."""
    project, my_role = _authorize_dataset_op(
        session, name, identity, settings, Role.ADMIN
    )
    tables = _require_tables(tables)
    record = _dataset(session, project, dataset)
    try:
        tables.drop_table(identity.token, project.name, record.name)
    except TableForbidden as exc:
        raise _forbidden(project, my_role, Role.ADMIN) from exc
    except TableError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="The catalog could not drop the table",
        ) from exc
    session.delete(record)
    session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


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
        Literal["append", "overwrite"],
        Query(description="append (default), or overwrite rows"),
    ] = "append",
    filter: Annotated[
        str | None,
        Query(
            max_length=2000,
            description=(
                "overwrite only: replace only rows matching this filter, e.g. "
                "location_id = 'usgs-01' and value_time >= '2026-01-01T00:00:00+00:00'"
                "; without it the whole table is replaced"
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
) -> WriteOut:
    """Write rows to a dataset, as the caller. One snapshot per request."""
    project, my_role = _authorize_dataset_op(
        session, name, identity, settings, Role.WRITE
    )
    tables = _require_tables(tables)
    record = _dataset(session, project, dataset)
    if filter is not None and mode != "overwrite":
        raise _unprocessable("filter only applies to mode=overwrite")
    if timezone is not None:
        try:
            ZoneInfo(timezone)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise _unprocessable(f"unknown time zone '{timezone}'") from exc
    columns = record.columns or []
    try:
        data = decode(*payload, columns)
        if data.num_rows == 0:
            raise _unprocessable("No rows to write")
        data = conform(data, columns, timezone)
        expression = parse_filter(filter, columns) if filter else None
    except DataMismatch as exc:
        raise _unprocessable(str(exc)) from exc
    except UnsupportedMediaType as exc:
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE, detail=str(exc)
        ) from exc
    try:
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
    write = DatasetWrite(
        dataset_id=record.id,
        mode=mode,
        filter=filter,
        rows=data.num_rows,
        snapshot_id=result.snapshot_id,
        written_by=identity.subject,
    )
    session.add(write)
    session.commit()
    session.refresh(write)
    return WriteOut.model_validate(write)


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
                "Rows to return, e.g. location_id = 'usgs-01' and "
                "value_time >= '2026-01-01T00:00:00+00:00'"
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
) -> Response:
    """Read a dataset's rows, as the caller, a page at a time.

    Every page of one read sees the same snapshot (the cursor pins it), so writes
    landing meanwhile don't shift or duplicate rows. For bulk reads use Arrow or
    Parquet; large exports belong in jobs.
    """
    project, my_role = _authorize_dataset_op(
        session, name, identity, settings, Role.READ
    )
    tables = _require_tables(tables)
    record = _dataset(session, project, dataset)
    declared = record.columns or []
    selected = _columns(columns, declared)
    try:
        expression = parse_filter(filter, declared) if filter else None
    except DataMismatch as exc:
        raise _unprocessable(str(exc)) from exc

    media_type = negotiate(request.headers.get("accept"))
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
            columns=selected,
            row_filter=expression,
            snapshot_id=snapshot_id,
            limit=offset + page + 1,
        )
        total = (
            tables.count(
                identity.token,
                project.name,
                record.name,
                expression,
                result.snapshot_id,
            )
            if count
            else None
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
    except TableError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="The catalog could not read the table",
        ) from exc

    rows = result.rows.slice(offset, page)
    more = result.rows.num_rows > offset + page
    next_cursor = (
        encode_cursor(result.snapshot_id, offset + page, query)
        if more and result.snapshot_id is not None
        else None
    )
    headers = {}
    if result.snapshot_id is not None:
        headers[SNAPSHOT_ID] = str(result.snapshot_id)
    if media_type == JSON:
        body = {"rows": json_rows(rows), "next_cursor": next_cursor}
        if total is not None:
            body["total"] = total
        return JSONResponse(body, headers=headers)
    if next_cursor:
        headers[NEXT_CURSOR] = next_cursor
    if total is not None:
        headers[TOTAL_ROWS] = str(total)
    return Response(
        encode_binary(rows, media_type), media_type=media_type, headers=headers
    )
