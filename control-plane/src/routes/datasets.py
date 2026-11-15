"""Datasets: a project's tables, registered and managed through the API (Phase 2).

Each dataset is one Iceberg table in the project's namespace. The API creates and
drops it **as the caller** (their token against Polaris), so Polaris enforces the
caller's project roles; the control-plane DB records the declared columns that
writes are checked against.

Access: listing and reading need the read role (everyone, for a system project);
registering needs write; deleting needs admin. Platform admins may act anywhere.
"""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, Response, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.auth import Identity
from src.config import Settings
from src.errors import APIError
from src.models import Dataset, Project, ProjectStatus, Role
from src.routes.projects import (
    IdentityDep,
    ProjectName,
    SessionDep,
    SettingsDep,
    _authorize,
    _errors,
)
from src.schemas import PROJECT_NAME_PATTERN, DatasetCreate, DatasetOut
from src.tables import (
    NamespaceMissing,
    TableError,
    TableExists,
    TableForbidden,
    Tables,
    get_tables,
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
    """Register a dataset: creates its Iceberg table, as the caller."""
    project, my_role = _authorize_dataset_op(
        session, name, identity, settings, Role.WRITE
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
