"""Saved evaluations: a spec under a name, versioned (Phase 5).

``PUT /v1/projects/{name}/evaluations/{slug}`` plans the spec against the
project's datasets (as ``POST .../runs/plan`` does) and saves it: as version 1, as
a new version when it differs from the latest, or not at all when it means the
same (aliases, key order and defaults aside). Versions never change; each one's
runs write ``results_<slug>_v<version>`` (warehouse data model, "Evaluation
outputs"). Runs come next.
"""

import contextlib
from datetime import datetime
from typing import Annotated, Any

from fastapi import (
    APIRouter,
    Body,
    Depends,
    HTTPException,
    Path,
    Query,
    Response,
    status,
)
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session

from src.access import project_roles
from src.delegation import (
    Delegation,
    DelegationError,
    Sealer,
    get_delegation,
    get_sealer,
)
from src.errors import APIError, problem
from src.evaluations import SpecProblems, plan, spec_hash
from src.incremental import decide
from src.jobs import JobRefused, start_job
from src.jobs.run import keeps_state
from src.kube import Kube, KubeError, get_kube
from src.models import (
    Dataset,
    Evaluation,
    EvaluationVersion,
    JobStatus,
    Project,
    Role,
    Run,
)
from src.routes.projects import (
    IdentityDep,
    ProjectName,
    SessionDep,
    SettingsDep,
    _authorize,
    _errors,
)
from src.routes.runs import PlanOut, RunOut, run_out, sync
from src.tables import TableError, TableForbidden, TableMissing, Tables, get_tables

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
    responses=_errors(401, 403, 404, 409, 502, 503),
)
def delete_evaluation(
    name: ProjectName,
    slug: Slug,
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
    tables: Annotated[Tables | None, Depends(get_tables)],
    kube: Annotated[Kube | None, Depends(get_kube)],
    keep_results: Annotated[
        bool,
        Query(description="Keep the runs' tables (as datasets of their own)"),
    ] = False,
):
    """Delete an evaluation with its versions, runs and their tables
    (``results_``, ``pairs_``, ``state_<slug>_v<n>``, dropped as you), unless
    ``keep_results``. Refused
    while one of its runs is going. Needs the admin role."""
    project, my_role = _authorize(session, name, identity, settings, Role.ADMIN)
    evaluation = _evaluation(session, project, slug)
    if evaluation is None:
        raise _not_found()
    versions = _versions(session, evaluation)
    runs = session.scalars(
        select(Run).where(Run.evaluation_version_id.in_([v.id for v in versions]))
    ).all()
    for run in runs:
        sync(session, kube, run)
    going = [
        r for r in runs if r.status in (JobStatus.QUEUED.value, JobStatus.RUNNING.value)
    ]
    if going:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"'{slug}' has {len(going)} run(s) going; cancel them "
            "(POST .../runs/{id}/cancel) or wait for them to finish",
        )
    if not keep_results:
        names = [n for v in versions for n in run_tables(slug, v.version)]
        results = session.scalars(
            select(Dataset).where(
                Dataset.project_id == project.id, Dataset.name.in_(names)
            )
        ).all()
        if results and tables is None:
            raise HTTPException(
                status.HTTP_503_SERVICE_UNAVAILABLE, "Table storage is not configured"
            )
        for dataset in results:
            try:
                tables.drop_table(identity.token, project.name, dataset.name)
            except TableMissing:
                pass  # gone from the catalog already; drop the record
            except TableForbidden as exc:
                raise HTTPException(
                    status.HTTP_403_FORBIDDEN,
                    f"The catalog refused dropping '{dataset.name}'"
                    + (
                        "; your token may predate your role, get a new one"
                        if my_role
                        else ""
                    ),
                ) from exc
            except TableError as exc:
                raise HTTPException(
                    status.HTTP_502_BAD_GATEWAY,
                    f"The catalog could not drop '{dataset.name}'",
                ) from exc
            session.delete(dataset)
            session.commit()
    session.execute(
        delete(EvaluationVersion).where(
            EvaluationVersion.evaluation_id == evaluation.id
        )
    )
    session.delete(evaluation)
    session.commit()


# --- runs ---------------------------------------------------------------------


class RunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: int | None = Field(default=None, ge=1, description="Default: the latest")
    full: bool = Field(
        default=False,
        description="Recompute everything, even where only some months changed",
    )


#: The tables a version's runs write (warehouse data model, "Evaluation outputs").
RUN_TABLE_KINDS = ("results", "pairs", "state")


def results_dataset(slug: str, version: int) -> str:
    return f"results_{slug}_v{version}"


def run_tables(slug: str, version: int) -> list[str]:
    return [f"{kind}_{slug}_v{version}" for kind in RUN_TABLE_KINDS]


@router.post(
    "/{slug}/runs",
    operation_id="start_run",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=RunOut,
    responses=_errors(401, 403, 404, 429, 502, 503),
)
def start_run(
    name: ProjectName,
    slug: Slug,
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
    kube: Annotated[Kube | None, Depends(get_kube)],
    delegation: Annotated[Delegation | None, Depends(get_delegation)],
    sealer: Annotated[Sealer | None, Depends(get_sealer)],
    body: Annotated[RunRequest | None, Body()] = None,
):
    """Run a version of the evaluation (default the latest) as a job acting as
    you: it reads the inputs, runs the spec and writes results_<slug>_v<version>
    (replacing an earlier run's). The spec is planned again first, against the
    datasets as they are now (422 if it no longer plans).

    A run is incremental when it can be: the version keeps state and only some
    months of its inputs changed since its last run (timeseries writes record the
    months they touch); it then recomputes only those. ``full`` recomputes all;
    the run's ``result.mode`` says which it did, and why when full. Needs the write
    role."""
    project, my_role = _authorize(session, name, identity, settings, Role.WRITE)
    if (
        my_role is not None
        and not identity.has_any_role(settings.admin_roles)
        and project_roles(project.name)[my_role] not in identity.roles
    ):
        # The job acts with this token's roles (token exchange can't add any).
        raise APIError(
            status.HTTP_403_FORBIDDEN,
            f"Your access token doesn't carry your {my_role.value} role in "
            f"'{project.name}', so the run would go without it. Get a new token "
            "and retry.",
            code="token_refresh_required",
        )
    if kube is None or delegation is None or sealer is None:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, detail="Runs (jobs) aren't configured"
        )
    evaluation = _evaluation(session, project, slug)
    versions = _versions(session, evaluation) if evaluation else []
    wanted = (
        body.version
        if body and body.version
        else (versions[-1].version if versions else 0)
    )
    version = next((v for v in versions if v.version == wanted), None)
    if evaluation is None or version is None:
        raise _not_found()
    try:
        planned = plan(session, project, version.spec)
    except SpecProblems as exc:
        first = exc.errors[0]["msg"] if exc.errors else "Invalid spec"
        return problem(422, first, errors=exc.errors)
    if planned.manifest["location_fields"]:
        fields = ", ".join(planned.manifest["location_fields"])
        return problem(
            422,
            f"fields {fields} label series by their location layer, which runs "
            "don't do yet",
            errors=[{"loc": ["body", "fields"], "type": "unsupported",
                     "msg": f"location fields aren't run yet: {fields}"}],
        )  # fmt: skip
    incremental = decide(
        session, project, version, planned.spec, planned.manifest,
        keeps_state=keeps_state(planned.spec), full=bool(body and body.full),
    )  # fmt: skip
    try:
        job = start_job(
            session, settings, kube, delegation, sealer,
            kind="run", project=project,
            caller=identity.subject, caller_token=identity.token,
        )  # fmt: skip
    except JobRefused as exc:
        session.rollback()
        raise HTTPException(status.HTTP_429_TOO_MANY_REQUESTS, detail=str(exc)) from exc
    except DelegationError as exc:
        session.rollback()
        raise HTTPException(
            status.HTTP_502_BAD_GATEWAY,
            detail="Couldn't get a token for the job to act as you; sign in again and retry",
        ) from exc
    except KubeError as exc:
        session.rollback()
        raise HTTPException(
            status.HTTP_502_BAD_GATEWAY, detail="Couldn't start the run's job"
        ) from exc
    run = Run(
        project_id=project.id,
        evaluation_version_id=version.id,
        job_id=job.id,
        status=job.status.value,
        created_by=identity.subject,
        task={
            "evaluation": slug,
            "version": version.version,
            "spec": version.spec,
            "results_dataset": results_dataset(slug, version.version),
            # Pairs are kept when the spec asks (output.pairs: all); default none.
            "pairs": planned.spec.output.pairs == "all",
            "incremental": incremental,
            "manifest": planned.manifest,
        },
    )
    session.add(run)
    try:
        session.commit()
    except Exception:
        session.rollback()
        with contextlib.suppress(KubeError):
            kube.delete_job(job.k8s_name)
        raise
    session.refresh(run)
    return run_out(run)


@router.get(
    "/{slug}/runs",
    operation_id="list_runs",
    response_model=list[RunOut],
    responses=_errors(401, 404),
)
def list_runs(
    name: ProjectName,
    slug: Slug,
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
    kube: Annotated[Kube | None, Depends(get_kube)],
):
    """The evaluation's runs, newest first. Needs the read role."""
    project, _ = _authorize(session, name, identity, settings, Role.READ)
    evaluation = _evaluation(session, project, slug)
    if evaluation is None:
        raise _not_found()
    runs = session.scalars(
        select(Run)
        .join(EvaluationVersion, EvaluationVersion.id == Run.evaluation_version_id)
        .where(EvaluationVersion.evaluation_id == evaluation.id)
        .order_by(Run.created_at.desc())
    ).all()
    for run in runs:
        sync(session, kube, run)
    return [run_out(r) for r in runs]
