"""Evaluation runs (Phase 5). For now: planning a spec before running it.

``POST /v1/projects/{name}/runs/plan`` checks an evaluation spec against the
project's datasets (and ``public.<name>`` ones) and says what a run will do, or
everything that stops it, as 422 ``errors``. Nothing runs; the run itself comes
next (``POST /v1/projects/{name}/runs``).
"""

from typing import Annotated, Any

from fastapi import APIRouter, Body
from pydantic import BaseModel, Field

from src.errors import problem
from src.evaluations import SpecProblems, plan
from src.models import Role
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
