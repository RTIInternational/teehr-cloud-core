"""Saved evaluations: a spec under a name, versioned (Phase 5).

``PUT /v1/projects/{name}/evaluations/{slug}`` plans the spec against the
project's datasets (as ``POST .../runs/plan`` does) and saves it: as version 1, as
a new version when it differs from the latest, or not at all when it means the
same (aliases, key order and defaults aside). Versions never change; each one's
runs write ``results_<slug>_v<version>`` (warehouse data model, "Evaluation
outputs"). Runs come next.
"""

from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Body, HTTPException, Path, Response, status
from pydantic import BaseModel, Field
from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session

from src.errors import problem
from src.evaluations import SpecProblems, plan, spec_hash
from src.models import Evaluation, EvaluationVersion, Project, Role
from src.routes.projects import (
    IdentityDep,
    ProjectName,
    SessionDep,
    SettingsDep,
    _authorize,
    _errors,
)
from src.routes.runs import PlanOut

router = APIRouter(prefix="/v1/projects/{name}/evaluations", tags=["evaluations"])

# Short enough that results_<slug>_v<version> is a valid dataset name (60).
SLUG_PATTERN = r"^[a-z][a-z0-9_]{0,44}$"
Slug = Annotated[str, Path(pattern=SLUG_PATTERN, description="The evaluation's name")]


class VersionOut(BaseModel):
    version: int
    created_by: str
    created_at: datetime


class EvaluationOut(BaseModel):
    slug: str
    version: int = Field(description="The latest version")
    spec: dict[str, Any] = Field(description="The latest version's spec, as saved")
    versions: list[VersionOut]
    created_by: str
    created_at: datetime


class SavedOut(EvaluationOut):
    changed: bool = Field(description="Whether this request saved a new version")
    plan: PlanOut


class EvaluationSummaryOut(BaseModel):
    slug: str
    version: int
    updated_at: datetime = Field(description="When the latest version was saved")
    created_by: str


def _evaluation(session: Session, project: Project, slug: str) -> Evaluation | None:
    return session.scalar(
        select(Evaluation).where(
            Evaluation.project_id == project.id, Evaluation.slug == slug
        )
    )


def _versions(session: Session, evaluation: Evaluation) -> list[EvaluationVersion]:
    return list(
        session.scalars(
            select(EvaluationVersion)
            .where(EvaluationVersion.evaluation_id == evaluation.id)
            .order_by(EvaluationVersion.version)
        )
    )


def _not_found() -> HTTPException:
    return HTTPException(status.HTTP_404_NOT_FOUND, detail="Evaluation not found")


def _out(evaluation: Evaluation, versions: list[EvaluationVersion]) -> dict[str, Any]:
    latest = versions[-1]
    return {
        "slug": evaluation.slug,
        "version": latest.version,
        "spec": latest.spec,
        "versions": [
            VersionOut.model_validate(v, from_attributes=True) for v in versions
        ],
        "created_by": evaluation.created_by,
        "created_at": evaluation.created_at,
    }


@router.put(
    "/{slug}",
    operation_id="save_evaluation",
    response_model=SavedOut,
    responses={
        201: {"model": SavedOut, "description": "Saved as a new evaluation"},
        **_errors(401, 403, 404),
    },  # fmt: skip
)
def save_evaluation(
    name: ProjectName,
    slug: Slug,
    spec: Annotated[
        dict[str, Any],
        Body(description="An evaluation spec (teehr-shared-eval-spec v1), as JSON"),
    ],
    response: Response,
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
):
    """Save an evaluation spec under ``slug``, after planning it against the
    project's datasets (422 with every problem found, as ``runs/plan``).

    A spec that means the same as the latest version saves nothing (``changed``
    false); a different one saves the next version. Needs the write role.
    """
    project, _ = _authorize(session, name, identity, settings, Role.WRITE)
    try:
        planned = plan(session, project, spec)
    except SpecProblems as exc:
        first = exc.errors[0]["msg"] if exc.errors else "Invalid spec"
        return problem(422, first, errors=exc.errors)
    fingerprint = spec_hash(planned.spec)
    evaluation = _evaluation(session, project, slug)
    created = evaluation is None
    if evaluation is None:
        evaluation = Evaluation(
            project_id=project.id, slug=slug, created_by=identity.subject
        )
        session.add(evaluation)
        session.flush()
    versions = _versions(session, evaluation)
    changed = not versions or versions[-1].spec_hash != fingerprint
    if changed:
        version = EvaluationVersion(
            evaluation_id=evaluation.id,
            version=(versions[-1].version + 1) if versions else 1,
            spec=spec,
            spec_hash=fingerprint,
            created_by=identity.subject,
        )
        session.add(version)
        versions.append(version)
    session.commit()
    session.refresh(evaluation)
    for v in versions:
        session.refresh(v)
    if created:
        response.status_code = status.HTTP_201_CREATED
    return {**_out(evaluation, versions), "changed": changed,
            "plan": PlanOut.model_validate(planned, from_attributes=True)}  # fmt: skip


@router.get(
    "",
    operation_id="list_evaluations",
    response_model=list[EvaluationSummaryOut],
    responses=_errors(401, 404),
)
def list_evaluations(
    name: ProjectName, session: SessionDep, identity: IdentityDep, settings: SettingsDep
):
    """The project's saved evaluations, with their latest versions. Needs the read
    role."""
    project, _ = _authorize(session, name, identity, settings, Role.READ)
    latest = (
        select(
            EvaluationVersion.evaluation_id,
            func.max(EvaluationVersion.version).label("version"),
            func.max(EvaluationVersion.created_at).label("updated_at"),
        )
        .group_by(EvaluationVersion.evaluation_id)
        .subquery()
    )
    rows = session.execute(
        select(Evaluation, latest.c.version, latest.c.updated_at)
        .join(latest, latest.c.evaluation_id == Evaluation.id)
        .where(Evaluation.project_id == project.id)
        .order_by(Evaluation.slug)
    )
    return [
        EvaluationSummaryOut(
            slug=e.slug, version=v, updated_at=u, created_by=e.created_by
        )
        for e, v, u in rows
    ]


@router.get(
    "/{slug}",
    operation_id="get_evaluation",
    response_model=EvaluationOut,
    responses=_errors(401, 404),
)
def get_evaluation(
    name: ProjectName,
    slug: Slug,
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
):
    """An evaluation: its latest spec and every version. Needs the read role."""
    project, _ = _authorize(session, name, identity, settings, Role.READ)
    evaluation = _evaluation(session, project, slug)
    if evaluation is None:
        raise _not_found()
    return _out(evaluation, _versions(session, evaluation))


@router.get(
    "/{slug}/versions/{version}",
    operation_id="get_evaluation_version",
    response_model=dict[str, Any],
    responses=_errors(401, 404),
)
def get_evaluation_version(
    name: ProjectName,
    slug: Slug,
    version: int,
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
):
    """One version's spec, as saved. Needs the read role."""
    project, _ = _authorize(session, name, identity, settings, Role.READ)
    evaluation = _evaluation(session, project, slug)
    found = None
    if evaluation is not None:
        found = session.scalar(
            select(EvaluationVersion).where(
                EvaluationVersion.evaluation_id == evaluation.id,
                EvaluationVersion.version == version,
            )
        )
    if found is None:
        raise _not_found()
    return found.spec


@router.delete(
    "/{slug}",
    operation_id="delete_evaluation",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=_errors(401, 403, 404),
)
def delete_evaluation(
    name: ProjectName,
    slug: Slug,
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
):
    """Delete an evaluation and its versions. Needs the admin role."""
    project, _ = _authorize(session, name, identity, settings, Role.ADMIN)
    evaluation = _evaluation(session, project, slug)
    if evaluation is None:
        raise _not_found()
    session.execute(
        delete(EvaluationVersion).where(
            EvaluationVersion.evaluation_id == evaluation.id
        )
    )
    session.delete(evaluation)
    session.commit()
