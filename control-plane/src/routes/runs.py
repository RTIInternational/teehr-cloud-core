"""Evaluation runs (Phase 5).

``POST /v1/projects/{name}/runs/plan`` checks an evaluation spec against the
project's datasets (and ``public.<name>`` ones) and says what a run will do, or
everything that stops it, as 422 ``errors``; nothing runs. Runs of a saved
evaluation start at ``POST .../evaluations/{slug}/runs``; ``GET .../runs/{id}``
follows one.
"""

import uuid
from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Body, Depends, HTTPException, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.orm import Session

from src.errors import problem
from src.evaluations import SpecProblems, plan
from src.jobs import ACTIVE as ACTIVE_JOBS
from src.jobs import cancel, reconcile
from src.kube import Kube, KubeError, get_kube
from src.models import Job, Role, Run
from src.routes.projects import (
    IdentityDep,
    ProjectName,
    SessionDep,
    SettingsDep,
    _authorize,
    _errors,
)

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
        description="Where its results are: results_<slug>_v<version>"
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
