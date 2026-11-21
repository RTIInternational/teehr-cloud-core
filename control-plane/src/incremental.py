"""Whether a run can be incremental, and which months it recomputes (Phase 5).

A run keeps its first stage's state per group and month of value_time (when the
spec allows: ``jobs.run.keeps_state``). The next run of the same version can then
recompute only the months whose inputs changed and merge them with the rest:

- what changed: the run job compares each input's live files, at the snapshot it
  pins, with the state the last run with state recorded (``result.input_states``:
  ADR-0019, ``teehr_core.warehouse.changes``), whoever wrote them, however much
  history was expired since. A timeseries
  gives the months of value_time its changed files span; a series table that only
  gained series changes nothing by itself; any other change (to a series table,
  crosswalk or layer), or a state that can't be told, makes the run a full one.
  Here, the API decides what doesn't need the warehouse and passes those states;
- what a month's pairs depend on: that month's rows; with a persistence input also
  the observations up to its reach before (``max_age`` plus the longest lead), and
  with a Diebold-Mariano test the lag partners up to ``step × lags`` before. The
  months a change reaches are recomputed with it (the job widens them by the
  reaches the last full run recorded). A field that looks across a whole series
  (exceedance, events, change) or a climatology or blend input (built from the whole
  record) makes every run full.

Writes that land while a run reads are after its pinned snapshots: the next run
sees them once.
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

from src.models import Dataset, EvaluationVersion, Project, Run


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
    """``{"mode": "incremental", "since": {...}, ...}`` (the inputs' states the last
    run recorded, and the stored reaches) or ``{"mode": "full", "why": ...}`` for a run
    about to start."""
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
    states = before.get("input_states")
    if not states:
        return _full("the last run didn't record its inputs' states")
    tables = [(d["project"], d["name"]) for d in manifest["datasets"].values()]
    tables += [(x["project"], x["name"]) for x in manifest.get("location_layers", [])]
    for owner, name in tables:
        record = _dataset(session, owner, name)
        if record is None:
            return _full(f"'{owner}.{name}' is gone")
        if _aware(record.created_at) > since:
            return _full(f"'{owner}.{name}' is new since the last run")
    return {
        "mode": "incremental",
        # The inputs' states the last run read: the job finds what changed since.
        "since": states,
        "dm": before.get("dm"),
        "persistence_reach": before.get("persistence_reach"),
        "since_run": str(previous.id),
        # The state that run left: a later run that failed may have committed other.
        "state_snapshot": (before.get("state") or {}).get("snapshot_id"),
    }
