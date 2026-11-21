"""Domains: the units, periods and variables datasets must name (#182).

Public rows are readable by anyone signed in and added by platform admins; a
project's own rows are readable by its readers and added by its writers. Rows
aren't edited or deleted through the API yet: datasets depend on them.
"""

from typing import Annotated

from fastapi import APIRouter, HTTPException, Query, status
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from teehr_core.metrics.catalog import catalog

from src.auth import Identity
from src.config import Settings
from src.domains import key_column, lookup, taken, visible
from src.models import Period, Project, Role, Unit, Variable
from src.routes.projects import (
    IdentityDep,
    SessionDep,
    SettingsDep,
    _authorize,
    _errors,
)
from src.schemas import (
    PROJECT_NAME_PATTERN,
    MetricOut,
    PeriodCreate,
    PeriodOut,
    UnitCreate,
    UnitOut,
    VariableCreate,
    VariableOut,
)

router = APIRouter(prefix="/v1/domains", tags=["domains"])

ProjectQuery = Annotated[
    str | None,
    Query(
        pattern=PROJECT_NAME_PATTERN,
        description="Also include this project's own rows",
    ),
]


def _scope(
    session: Session,
    project: str | None,
    identity: Identity,
    settings: Settings,
    min_role: Role,
) -> Project | None:
    """The project a request is scoped to (None: public rows only)."""
    if project is None:
        if min_role is not Role.READ and not identity.has_any_role(
            settings.admin_roles
        ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Only platform admins add public rows; name a project",
            )
        return None
    found, _ = _authorize(session, project, identity, settings, min_role)
    return found


def _list(session, model, project: Project | None, out):
    names = {project.id: project.name} if project else {}
    rows = session.scalars(
        visible(model, project and project.id).order_by(key_column(model))
    )
    return [
        out.model_validate({**_fields(row), "project": names.get(row.project_id)})
        for row in rows
    ]


def _fields(row) -> dict:
    return {c: getattr(row, c) for c in row.__mapper__.attrs.keys()}


def _add(session: Session, row, project: Project | None, name: str, out):
    model = type(row)
    if taken(session, model, name, row.project_id):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"'{name}' is already defined",
        )
    session.add(row)
    try:
        session.commit()
    except IntegrityError as exc:
        session.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"'{name}' is already defined",
        ) from exc
    session.refresh(row)
    return out.model_validate({**_fields(row), "project": project and project.name})


ADD_ERRORS = _errors(401, 403, 404, 409)


@router.get("/units", operation_id="list_units", responses=_errors(401, 404))
def list_units(
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
    project: ProjectQuery = None,
) -> list[UnitOut]:
    scope = _scope(session, project, identity, settings, Role.READ)
    return _list(session, Unit, scope, UnitOut)


@router.post(
    "/units",
    status_code=status.HTTP_201_CREATED,
    operation_id="add_unit",
    responses=ADD_ERRORS,
)
def add_unit(
    body: UnitCreate, session: SessionDep, identity: IdentityDep, settings: SettingsDep
) -> UnitOut:
    scope = _scope(session, body.project, identity, settings, Role.WRITE)
    row = Unit(
        project_id=scope and scope.id,
        **body.model_dump(exclude={"project"}),
    )
    return _add(session, row, scope, body.symbol, UnitOut)


@router.get("/periods", operation_id="list_periods", responses=_errors(401, 404))
def list_periods(
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
    project: ProjectQuery = None,
) -> list[PeriodOut]:
    scope = _scope(session, project, identity, settings, Role.READ)
    return _list(session, Period, scope, PeriodOut)


@router.post(
    "/periods",
    status_code=status.HTTP_201_CREATED,
    operation_id="add_period",
    responses=ADD_ERRORS,
)
def add_period(
    body: PeriodCreate,
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
) -> PeriodOut:
    scope = _scope(session, body.project, identity, settings, Role.WRITE)
    row = Period(
        project_id=scope and scope.id,
        **body.model_dump(exclude={"project"}),
    )
    return _add(session, row, scope, body.name, PeriodOut)


#: Metrics TEEHR can't run yet, and why (they need derived datasets, #251).
UNAVAILABLE = {
    "mean_absolute_scaled_error": (
        "its scale comes from a naive_scale field, which needs a derived dataset TEEHR "
        "can't make yet (teehr-cloud-core#251)"
    ),
}


@router.get("/metrics", operation_id="list_metrics", responses=_errors(401))
def list_metrics(identity: IdentityDep) -> list[MetricOut]:
    """Every metric a spec can name, from teehr-core's registry: aliases, definition,
    parameters, result unit, whether it merges from state, and whether TEEHR can run
    it today. Any signed-in user."""
    return [
        MetricOut(
            **m, available=m["name"] not in UNAVAILABLE, note=UNAVAILABLE.get(m["name"])
        )
        for m in catalog()
    ]


@router.get("/variables", operation_id="list_variables", responses=_errors(401, 404))
def list_variables(
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
    project: ProjectQuery = None,
) -> list[VariableOut]:
    scope = _scope(session, project, identity, settings, Role.READ)
    return _list(session, Variable, scope, VariableOut)


@router.post(
    "/variables",
    status_code=status.HTTP_201_CREATED,
    operation_id="add_variable",
    responses=ADD_ERRORS,
)
def add_variable(
    body: VariableCreate,
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
) -> VariableOut:
    scope = _scope(session, body.project, identity, settings, Role.WRITE)
    if lookup(session, Unit, body.canonical_unit, scope and scope.id) is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"unknown unit '{body.canonical_unit}'",
        )
    row = Variable(
        project_id=scope and scope.id,
        **body.model_dump(exclude={"project"}),
    )
    return _add(session, row, scope, body.name, VariableOut)
