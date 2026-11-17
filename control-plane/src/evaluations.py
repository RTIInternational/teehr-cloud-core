"""Evaluation specs against a project's datasets (Phase 5): resolve, check, plan.

A spec (teehr-shared-eval-spec, ``spec/``) names datasets as ``<name>`` (this
project) or ``public.<name>``. Planning resolves every name to its dataset, builds
teehr-core's engine catalog from the datasets' metadata (role, variable, unit,
statistic, period; tables with their declared columns and no rows), and runs the
engine's planner, which checks everything that doesn't need the data: roles, units
and their conversions, periods, the pairing relation, fields and metrics.

A run plans again with the data (per-series periods, field labels); this is the
check before one starts.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa
from pydantic import ValidationError
from sqlalchemy.orm import Session
from teehr_core.engine import Catalog, PlanError, build_plan
from teehr_core.engine import Dataset as EngineDataset
from teehr_core.engine.plan import labels_name
from teehr_core.model.dataset_types import CROSSWALK, LAYER, TIMESERIES, arrow_schema
from teehr_core.model.periods import PeriodRegistry
from teehr_core.model.units import UnitRegistry
from teehr_core.spec import EvaluationSpec, load_spec
from teehr_core.spec.models import AttributeField, GeneratedInput, WithinField

from src import refs
from src.domains import visible
from src.models import Period, Project, Unit, Variable


@dataclass
class SpecProblems(Exception):
    """What's wrong with a spec, as shared ``errors`` entries ``{loc, msg, type}``."""

    errors: list[dict[str, Any]] = field(default_factory=list)

    def __str__(self) -> str:
        return "; ".join(e["msg"] for e in self.errors)


def _problem(msg: str, kind: str, *loc: str | int) -> SpecProblems:
    return SpecProblems([{"loc": ["body", *loc], "msg": msg, "type": kind}])


def parse(body: Any) -> EvaluationSpec:
    """Validate a spec's shape (raises :class:`SpecProblems`)."""
    try:
        return load_spec(body)
    except ValidationError as exc:
        raise SpecProblems(
            [
                {"loc": ["body", *e["loc"]], "msg": e["msg"], "type": e["type"]}
                for e in exc.errors(include_url=False)
            ]
        ) from None
    except (ValueError, TypeError) as exc:
        raise _problem(str(exc), "value_error") from None


def references(spec: EvaluationSpec) -> list[str]:
    """Every dataset the spec names, in order of first use."""
    names = [spec.inputs.primary.dataset]
    for p in spec.inputs.secondary:
        if isinstance(p, GeneratedInput):
            names += [n for n in (p.like, p.from_) if n]
        else:
            names.append(p.dataset)
    if spec.pair.relation:
        names.append(spec.pair.relation)
    for f in spec.fields:
        names += [
            n for n in (getattr(f, "from_", None), getattr(f, "layer", None)) if n
        ]
    return list(dict.fromkeys(names))


def _empty(columns: list[dict] | None) -> pa.Table:
    return arrow_schema(columns or []).empty_table()


def _qualified(owner: Project, name: str) -> tuple[str, str]:
    return owner.name, name


@dataclass
class Resolved:
    """A spec's datasets: the catalog the engine plans with, and the records."""

    catalog: Catalog
    records: dict[str, refs.Resolved]
    #: For the relation: whether its left side is the spec's secondary side.
    relation_reversed: bool = False


def registries(
    session: Session, project: Project
) -> tuple[UnitRegistry, PeriodRegistry, dict[str, str]]:
    """The units, periods and variables visible in ``project`` (public + its own)."""
    units = UnitRegistry.from_rows(
        {"symbol": u.symbol, "dimension": u.dimension, "to_si": u.to_si,
         "offset": u.offset, "description": u.description}
        for u in session.scalars(visible(Unit, project.id))
    )  # fmt: skip
    periods = PeriodRegistry.from_rows(
        {"name": p.name, "form": p.form, "length": p.length,
         "value_time_at": p.value_time_at, "start": p.start, "end": p.end,
         "description": p.description}
        for p in session.scalars(visible(Period, project.id))
    )  # fmt: skip
    variables = {
        v.name: v.canonical_unit for v in session.scalars(visible(Variable, project.id))
    }
    return units, periods, variables


def resolve(session: Session, project: Project, spec: EvaluationSpec) -> Resolved:
    """Resolve the spec's datasets into an engine catalog (raises SpecProblems)."""
    units, periods, variables = registries(session, project)
    records: dict[str, refs.Resolved] = {}
    problems = []
    for name in references(spec):
        found = refs.resolve(session, project, name)
        if found is None:
            where = "the public project" if name.startswith("public.") else project.name
            problems.append(
                {
                    "loc": ["body"],
                    "msg": f"no dataset '{name}' in {where}",
                    "type": "not_found",
                }
            )
        else:
            records[name] = found
    if problems:
        raise SpecProblems(problems)

    datasets = []
    for name, found in records.items():
        record, props = found.dataset, found.dataset.properties or {}
        if record.kind == TIMESERIES:
            series = refs.resolve(session, found.project, props["series_table"])
            datasets.append(
                EngineDataset(
                    name=name,
                    role=props["role"],
                    table=_empty(record.columns),
                    series=_empty(series.dataset.columns) if series else None,
                    variable=props.get("variable"),
                    unit=props.get("unit"),
                    statistic=props.get("statistic"),
                    period=props.get("period"),
                )
            )
        elif record.kind == CROSSWALK:
            datasets.append(
                EngineDataset(
                    name, "auxiliary", _empty(record.columns), kind="crosswalk"
                )
            )
        else:  # layers (within fields), series and plain tables
            datasets.append(
                EngineDataset(
                    name, "auxiliary", _empty(record.columns), kind=record.kind
                )
            )

    reversed_ = False
    if spec.pair.relation:
        reversed_ = _check_relation(session, project, spec, records)
    for f in spec.fields:
        if isinstance(f, WithinField | AttributeField):
            labels = _check_labels(session, project, spec, records, f)
            if labels is not None:
                datasets.append(labels)
    try:
        catalog = Catalog(datasets, units, periods, variables)
    except Exception as exc:  # a dataset that doesn't meet the contract
        raise _problem(str(exc), "contract") from None
    return Resolved(catalog, records, reversed_)


def _series_of(session: Session, found: refs.Resolved) -> tuple[str, str] | None:
    props = found.dataset.properties or {}
    if found.dataset.kind != TIMESERIES or not props.get("series_table"):
        return None
    return _qualified(found.project, props["series_table"])


def _check_relation(
    session: Session, project: Project, spec: EvaluationSpec, records: dict
) -> bool:
    """The crosswalk must link the primary's series to each secondary's; returns
    whether it does so right to left (its left side holds the secondary keys)."""
    xw = records[spec.pair.relation]
    if xw.dataset.kind != CROSSWALK:
        raise _problem(
            f"relation '{spec.pair.relation}' is a {xw.dataset.kind}, not a crosswalk",
            "relation", "pair", "relation",
        )  # fmt: skip
    props = xw.dataset.properties or {}

    def table(ref: str) -> tuple[str, str]:  # as the crosswalk's project names it
        owner, name = refs.split(ref)
        return owner or xw.project.name, name

    left, right = table(props["left_series_table"]), table(props["right_series_table"])
    primary = _series_of(session, records[spec.inputs.primary.dataset])
    secondaries = [
        _series_of(session, records[p.dataset])
        for p in spec.inputs.secondary
        if not isinstance(p, GeneratedInput)
    ]
    if primary == left and all(s == right for s in secondaries):
        return False
    if primary == right and all(s == left for s in secondaries) and left != right:
        return True
    raise _problem(
        f"relation '{spec.pair.relation}' links {left[0]}.{left[1]} to "
        f"{right[0]}.{right[1]}, not the primary input's series to every secondary's",
        "relation", "pair", "relation",
    )  # fmt: skip


def _check_labels(
    session: Session, project: Project, spec: EvaluationSpec, records: dict, f: Any
) -> EngineDataset | None:
    """A field that labels series by their location's layer: check the layer, and
    stand in for the labels a run computes."""
    if isinstance(f, AttributeField) and f.of == "series":
        return None  # straight from the series table
    if isinstance(f, WithinField):
        layer = records[f.layer]
        if layer.dataset.kind != LAYER:
            raise _problem(
                f"field '{f.name}': '{f.layer}' is not a layer", "field", "fields"
            )
    else:
        observed = records[spec.inputs.primary.dataset]
        layer_ref = (observed.dataset.properties or {}).get("location_layer")
        layer = (
            refs.resolve(session, observed.project, layer_ref) if layer_ref else None
        )
        if layer is None:
            raise _problem(
                f"field '{f.name}': '{spec.inputs.primary.dataset}' names no location "
                "layer to look attributes up in",
                "field", "fields",
            )  # fmt: skip
        names = {c["name"] for c in layer.dataset.columns or []}
        if f.attribute not in names:
            raise _problem(
                f"field '{f.name}': layer '{layer.dataset.name}' has no attribute "
                f"'{f.attribute}'",
                "field", "fields",
            )  # fmt: skip
    table = pa.table(
        {"series_key": pa.array([], pa.string()), "value": pa.array([], pa.string())}
    )
    return EngineDataset(labels_name(f.name), "auxiliary", table, kind="labels")


@dataclass
class PlanSummary:
    """What a planned spec will do."""

    unit: str | None
    inputs: list[dict[str, Any]]
    relation: dict[str, Any] | None
    stages: list[dict[str, Any]]


def plan(session: Session, project: Project, body: Any) -> PlanSummary:
    """Parse, resolve and plan a spec (raises :class:`SpecProblems`)."""
    spec = parse(body)
    resolved = resolve(session, project, spec)
    try:
        planned = build_plan(spec, resolved.catalog)
    except (PlanError, KeyError, ValueError) as exc:
        message = exc.args[0] if isinstance(exc, KeyError) and exc.args else str(exc)
        raise _problem(str(message), "plan") from None

    def side(ds: EngineDataset, conversion: Any, role: str) -> dict[str, Any]:
        return {
            "dataset": ds.name, "side": role, "role": ds.role, "variable": ds.variable,
            "unit": ds.unit, "period": ds.period,
            "conversion": {"scale": conversion.scale, "offset": conversion.offset},
        }  # fmt: skip

    inputs = [side(planned.observed, planned.observed_conversion, "primary")]
    inputs += [side(p.dataset, p.conversion, "secondary") for p in planned.predicted]
    relation = None
    if planned.relation is not None:
        relation = {
            "dataset": planned.relation.name,
            "reversed": resolved.relation_reversed,
        }
    stages = [
        {
            "group_by": list(stage.group_by),
            "columns": [m.output_name() for m in stage.metrics],
        }
        for stage in planned.spec.aggregate
    ]
    return PlanSummary(planned.unit, inputs, relation, stages)
