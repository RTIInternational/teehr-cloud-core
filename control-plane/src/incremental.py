"""Whether a run can be incremental, and which months it recomputes (Phase 5).

A run keeps its first stage's state per group and month of value_time (when the
spec allows: ``jobs.run.keeps_state``). The next run of the same version can then
recompute only the months whose inputs changed and merge them with the rest:

- what changed: the writes to the run's inputs since the last run with state
  started (write lineage, ``dataset_writes``). A timeseries write records the
  months of value_time it touched; every other write (an overwrite, which may
  remove rows anywhere, or a series table, crosswalk or layer) changes too much
  to tell, so the run is a full one;
- what a month's pairs depend on: that month's rows; with a persistence input also
  the observations up to its reach before (``max_age`` plus the longest lead), and
  with a Diebold-Mariano test the lag partners up to ``step × lags`` before. The
  months a change reaches are recomputed with it (the job widens them by the
  reaches the last full run recorded). A field that looks across a whole series
  (exceedance, events, change) or a climatology or blend input (built from the whole
  record) makes every run full.

Writes that land while a run reads count as new for the next one: at worst a
month is recomputed twice.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session
from teehr_core.spec import EvaluationSpec
from teehr_core.spec.models import (
    AttributeField,
    CalendarField,
    ExprField,
    GeneratedInput,
    LeadTimeBinsField,
    WithinField,
)

from src.dataset_types import TIMESERIES
from src.models import Dataset, DatasetWrite, EvaluationVersion, Project, Run


def blockers(spec: EvaluationSpec) -> list[str]:
    """Why a spec's months can't be recomputed on their own (empty: they can)."""
    out = []
    for p in spec.inputs.secondary:
        if isinstance(p, GeneratedInput) and p.generate != "persistence":
            out.append(f"a {p.generate} input is built from the whole record")
    for f in spec.fields:
        if isinstance(f, ExprField | CalendarField | LeadTimeBinsField):
            continue
        if isinstance(f, AttributeField | WithinField):
            continue  # one label per series (a change to its layer makes runs full)
        out.append(f"field '{f.name}' looks beyond each pair's month")
    return out


def _full(why: str) -> dict[str, Any]:
    return {"mode": "full", "why": why}


def _previous(session: Session, version: EvaluationVersion) -> Run | None:
    """The version's latest run that kept state."""
    for run in session.scalars(
        select(Run)
        .where(Run.evaluation_version_id == version.id, Run.status == "succeeded")
        .order_by(Run.started_at.desc())
    ):
        if (run.result or {}).get("state") and run.started_at is not None:
            return run
    return None


def _aware(t: datetime) -> datetime:
    return t if t.tzinfo else t.replace(tzinfo=UTC)


def _dataset(session: Session, project: str, name: str) -> Dataset | None:
    return session.scalar(
        select(Dataset)
        .join(Project, Project.id == Dataset.project_id)
        .where(Project.name == project, Dataset.name == name)
    )


def decide(
    session: Session,
    project: Project,
    version: EvaluationVersion,
    spec: EvaluationSpec,
    manifest: dict[str, Any],
    keeps_state: bool,
    full: bool = False,
) -> dict[str, Any]:
    """``{"mode": "incremental", "months": [...], ...}`` (the months changed, those
    of observations, and the stored reaches) or ``{"mode": "full", "why": ...}`` for
    a run about to start."""
    if full:
        return _full("a full run was asked for")
    if not keeps_state:
        return _full("the spec's metrics don't merge from state")
    why = blockers(spec)
    if why:
        return _full("; ".join(why))
    previous = _previous(session, version)
    if previous is None:
        return _full("no earlier run kept state")
    if (previous.task or {}).get("manifest", {}).get("datasets") != manifest[
        "datasets"
    ]:
        return _full("the datasets it reads changed")
    before = previous.result or {}
    skill = spec.aggregate[0].skill
    if skill is not None and skill.test is not None and not before.get("dm"):
        return _full("the stored state has no Diebold-Mariano settings")
    persistence = any(
        isinstance(p, GeneratedInput) and p.generate == "persistence"
        for p in spec.inputs.secondary
    )
    if persistence and before.get("persistence_reach") is None:
        return _full("the stored state has no persistence reach")
    since = _aware(previous.started_at)
    months: set[str] = set()
    obs_months: set[str] = set()
    for ref, d in manifest["datasets"].items():
        targets = [(d["project"], d["name"], d["kind"])]
        if d.get("series"):
            targets.append((d["series"]["project"], d["series"]["name"], "series"))
        for owner, name, kind in targets:
            where = f"{owner}.{name}"
            record = _dataset(session, owner, name)
            if record is None:
                return _full(f"'{where}' is gone")
            if _aware(record.created_at) > since:
                return _full(f"'{where}' is new since the last run")
            writes = session.scalars(
                select(DatasetWrite).where(
                    DatasetWrite.dataset_id == record.id,
                    DatasetWrite.written_at > since,
                )
            ).all()
            if not writes:
                continue
            if kind != TIMESERIES:
                return _full(f"'{where}' changed")
            for w in writes:
                if w.months is None:
                    return _full(
                        f"'{where}' was overwritten (rows anywhere may be gone)"
                    )
                months.update(w.months)
                if ref == spec.inputs.primary.dataset and kind == TIMESERIES:
                    obs_months.update(w.months)
    for layer in manifest.get("location_layers", []):
        where = f"{layer['project']}.{layer['name']}"
        record = _dataset(session, layer["project"], layer["name"])
        if record is None:
            return _full(f"'{where}' is gone")
        changed = session.scalar(
            select(DatasetWrite.id).where(
                DatasetWrite.dataset_id == record.id, DatasetWrite.written_at > since
            )
        )
        if changed is not None or _aware(record.created_at) > since:
            return _full(f"'{where}' changed (series' locations)")
    return {
        "mode": "incremental",
        "months": sorted(months),
        # Observations reach forward through persistence forecasts made from them.
        "obs_months": sorted(obs_months),
        "dm": before.get("dm"),
        "persistence_reach": before.get("persistence_reach"),
        "since_run": str(previous.id),
    }
