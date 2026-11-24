"""Evaluation runs (Phase 5).

``POST /v1/projects/{name}/runs/plan`` checks an evaluation spec against the
project's datasets (and ``public.<name>`` ones) and says what a run will do, or
everything that stops it, as 422 ``errors``; nothing runs. Runs of a saved
evaluation start at ``POST .../evaluations/{slug}/runs``; ``GET .../runs/{id}``
follows one, and ``GET .../runs/{id}/results`` reads its results.

A version's results table keeps every run's rows (``run_id`` says which; read as a
dataset, it shows them all); pairs and state hold the latest run's.
"""

import uuid
from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.orm import Session
from teehr_core.metrics.units import result_factors
from teehr_core.model.units import Conversion
from teehr_core.spec import load_spec

from src import job_logs
from src.errors import problem
from src.evaluations import SpecProblems, plan
from src.formats import ARROW_STREAM, PARQUET
from src.jobs import ACTIVE as ACTIVE_JOBS
from src.jobs import cancel, reconcile
from src.kube import Kube, KubeError, get_kube
from src.models import EvaluationVersion, Job, Role, Run
from src.places import placer
from src.routes.datasets import TablesDep, _read_dataset, _require_tables
from src.routes.projects import (
    IdentityDep,
    ProjectName,
    SessionDep,
    SettingsDep,
    _authorize,
    _errors,
)
from src.schemas import LogsOut

router = APIRouter(prefix="/v1/projects/{name}/runs", tags=["runs"])


class ConversionOut(BaseModel):
    scale: float
    offset: float


class PlannedInputOut(BaseModel):
    dataset: str = Field(
        description="As the spec names it (`<name>` or `public.<name>`)"
    )
    side: str = Field(description="primary | secondary")
    role: str
    variable: str | None
    unit: str | None = Field(description="The unit the dataset is stored in")
    period: str | None
    conversion: ConversionOut = Field(
        description="value × scale + offset gives the evaluation unit"
    )


class PlannedRelationOut(BaseModel):
    dataset: str
    reversed: bool = Field(
        description="The crosswalk's left keys are the secondary side's series"
    )


class PlannedStageOut(BaseModel):
    group_by: list[str]
    columns: list[str] = Field(
        description="The stage's result columns, besides group_by"
    )


class PlanOut(BaseModel):
    unit: str | None = Field(
        description="The evaluation unit (output.unit, else the primary's)"
    )
    inputs: list[PlannedInputOut]
    relation: PlannedRelationOut | None
    stages: list[PlannedStageOut]
    warnings: list[str] = Field(
        default_factory=list,
        description=(
            "What the spec does that it may not expect, e.g. that every run is full"
        ),
    )


@router.post(
    "/plan",
    operation_id="plan_run",
    response_model=PlanOut,
    responses=_errors(401, 404),
)
def plan_run(
    name: ProjectName,
    spec: Annotated[
        dict[str, Any],
        Body(description="An evaluation spec (teehr-shared-eval-spec v1), as JSON"),
    ],
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
):
    """Check an evaluation spec against the project's datasets, without running it.

    Checks the spec's shape, that every dataset it names exists (`<name>` here,
    `public.<name>` in the public project), and everything the engine can check
    without the data: roles, units and conversions, periods, the crosswalk linking
    the inputs, fields and metric options. Needs the read role.
    """
    project, _ = _authorize(session, name, identity, settings, Role.READ)
    try:
        summary = plan(session, project, spec)
    except SpecProblems as exc:
        first = exc.errors[0]["msg"] if exc.errors else "Invalid spec"
        return problem(422, first, errors=exc.errors)
    return PlanOut.model_validate(summary, from_attributes=True)


# --- runs ---------------------------------------------------------------------


class RunOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    evaluation: str | None = Field(description="The evaluation's slug")
    version: int | None = Field(description="The evaluation version it ran")
    status: str = Field(description="queued | running | succeeded | failed | cancelled")
    results_dataset: str | None = Field(
        description=(
            "The table its results are in: results_<slug>_v<version>, which keeps every "
            "run's rows (run_id says which); GET .../runs/{id}/results reads this run's"
        )
    )
    result: dict[str, Any] | None = Field(
        description="When it succeeded: rows, each input's snapshot, engine version"
    )
    error: str | None
    created_by: str
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


def run_out(run: Run) -> RunOut:
    task = run.task or {}
    return RunOut(
        id=run.id, evaluation=task.get("evaluation"), version=task.get("version"),
        status=run.status, results_dataset=task.get("results_dataset"),
        result=run.result, error=run.error, created_by=run.created_by,
        created_at=run.created_at, started_at=run.started_at,
        finished_at=run.finished_at,
    )  # fmt: skip


def sync(session: Session, kube: Kube | None, run: Run) -> None:
    """Bring a run up to date with its job (fail it if its pod died unreported)."""
    job = session.get(Job, run.job_id) if run.job_id else None
    if job is None:
        return
    if reconcile(kube, job) or run.status != job.status.value:
        run.status, run.error = job.status.value, job.error or run.error
        run.started_at = run.started_at or job.started_at
        run.finished_at = job.finished_at
        session.commit()


@router.get(
    "/{run_id}",
    operation_id="get_run",
    response_model=RunOut,
    responses=_errors(401, 404),
)
def get_run(
    name: ProjectName,
    run_id: uuid.UUID,
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
    kube: Annotated[Kube | None, Depends(get_kube)],
):
    """A run's status and, once it has succeeded, where its results are. Needs the
    read role."""
    project, _ = _authorize(session, name, identity, settings, Role.READ)
    run = session.get(Run, run_id)
    if run is None or run.project_id != project.id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Run not found")
    sync(session, kube, run)
    return run_out(run)


@router.get(
    "/{run_id}/results",
    operation_id="read_run_results",
    responses={
        **_errors(400, 401, 403, 404, 409, 410, 502, 503),
        200: {
            "description": (
                "A page of the run's results: JSON by default, or Arrow / Parquet when "
                "the Accept header asks (as reading a dataset)."
            ),
            "content": {
                ARROW_STREAM: {"schema": {"type": "string", "format": "binary"}},
                PARQUET: {"schema": {"type": "string", "format": "binary"}},
            },
        },
    },
)
def read_run_results(
    request: Request,
    name: ProjectName,
    run_id: uuid.UUID,
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
    tables: TablesDep,
    kube: Annotated[Kube | None, Depends(get_kube)],
    columns: Annotated[
        str | None,
        Query(description="Comma-separated columns to return (default: all)"),
    ] = None,
    filter: Annotated[
        str | None,
        Query(max_length=2000, description="Rows to return, as when reading a dataset"),
    ] = None,
    limit: Annotated[int | None, Query(ge=1, description="Rows per page")] = None,
    cursor: Annotated[str | None, Query(description="From the previous page")] = None,
    count: Annotated[bool, Query(description="Also count the matching rows")] = False,
    geometry: Annotated[
        bool,
        Query(
            description=(
                "With output.geometry: give each row its place (and output.attributes); "
                "false leaves them out"
            )
        ),
    ] = True,
    unit: Annotated[
        str | None,
        Query(
            description=(
                "Return the results in this unit (UCUM) instead of the evaluation "
                "unit: each metric converts by its kind (errors by scale alone, "
                "squares by its square, ratios not across an offset); the filter's "
                "numbers are in it too. 422 unit_not_convertible names the columns "
                "that can't be. The response's unit is in X-Unit."
            )
        ),
    ] = None,
):
    """A succeeded run's results: its rows of its version's results table (which
    keeps every run's), paged as reading a dataset. With ``output.geometry`` each
    row gets its place: GeoJSON geometry in JSON rows, a feature collection for
    ``Accept: application/geo+json``, GeoParquet / GeoArrow for Parquet and Arrow.
    Needs the read role."""
    project, _ = _authorize(session, name, identity, settings, Role.READ)
    run = session.get(Run, run_id)
    if run is None or run.project_id != project.id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Run not found")
    sync(session, kube, run)
    dataset = (run.task or {}).get("results_dataset")
    if run.status != "succeeded" or not dataset:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            detail=f"The run is {run.status}; its results come once it succeeds",
        )
    mine = f"run_id = '{run.id}'"
    place = None
    version = session.get(EvaluationVersion, run.evaluation_version_id)
    if geometry and version is not None:
        place = placer(
            session, project, identity, _require_tables(tables), load_spec(version.spec)
        )
    units = _result_units(session, project, version) if version is not None else None
    return _read_dataset(
        request, name, dataset, session, identity, settings, tables,
        columns=columns, filter=f"({filter}) AND {mine}" if filter else mine,
        limit=limit, cursor=cursor, count=count, bbox=None, place=place,
        unit=unit, results=units,
    )  # fmt: skip


def _result_units(session: Session, project, version: EvaluationVersion):
    """The results' unit (the evaluation unit: output.unit, else the primary's, as
    the datasets are now) and each column's conversion from it."""
    spec = load_spec(version.spec)
    unit = spec.output.unit
    if unit is None:
        try:
            unit = plan(session, project, version.spec).unit
        except SpecProblems:
            unit = None  # a dataset it read is gone: the unit can't be told

    def factors(scale: float, offset: float):
        return result_factors(spec, Conversion(scale, offset))

    return unit, factors


@router.get(
    "/{run_id}/logs",
    operation_id="read_run_logs",
    response_model=LogsOut,
    responses=_errors(401, 404),
)
def read_run_logs(
    name: ProjectName,
    run_id: uuid.UUID,
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
    kube: Annotated[Kube | None, Depends(get_kube)],
    after: Annotated[
        int | None, Query(description="Only lines after this one (its `id`)")
    ] = None,
    limit: Annotated[int, Query(ge=1, le=10_000)] = 1000,
):
    """A run's log: its plan, each shard (from every pod), its writes and outcome,
    oldest first. Poll with ``after`` (the last line's ``id``) to follow it until
    ``ended``. Needs the read role."""
    project, _ = _authorize(session, name, identity, settings, Role.READ)
    run = session.get(Run, run_id)
    if run is None or run.project_id != project.id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Run not found")
    sync(session, kube, run)
    lines = job_logs.read(session, run.job_id, after, limit) if run.job_id else []
    return LogsOut(lines=lines, ended=run.status not in ("queued", "running"))


@router.post(
    "/{run_id}/cancel",
    operation_id="cancel_run",
    response_model=RunOut,
    responses=_errors(401, 403, 404, 409, 502, 503),
)
def cancel_run(
    name: ProjectName,
    run_id: uuid.UUID,
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
    kube: Annotated[Kube | None, Depends(get_kube)],
):
    """Stop a run that's queued or running. Its tables keep what an earlier run
    wrote (a run replaces them only as it finishes). Needs the write role."""
    project, _ = _authorize(session, name, identity, settings, Role.WRITE)
    run = session.get(Run, run_id)
    if run is None or run.project_id != project.id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Run not found")
    sync(session, kube, run)
    job = session.get(Job, run.job_id) if run.job_id else None
    if job is None or job.status not in ACTIVE_JOBS:
        raise HTTPException(
            status.HTTP_409_CONFLICT, detail=f"Run is {run.status}, not going"
        )
    if kube is None:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Runs (jobs) aren't configured; can't stop the job",
        )
    try:
        cancel(kube, job)
    except KubeError as exc:
        raise HTTPException(
            status.HTTP_502_BAD_GATEWAY, detail="Couldn't stop the job"
        ) from exc
    run.status, run.finished_at = job.status.value, job.finished_at
    session.commit()
    session.refresh(run)
    return run_out(run)
