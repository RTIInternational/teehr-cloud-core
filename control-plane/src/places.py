"""Places for a run's results (``output.geometry`` and ``output.attributes``).

The final stage's ``output.geometry`` column says where each result row is:

- ``primary_series_key``: the observed series' location (its series table's
  ``location_key``, in the series' ``location_layer`` or the dataset's default);
- a ``within`` field: the polygon of its layer the label names (the field's
  ``attribute``, by default the polygon's key).

:func:`placer` returns a function that gives a page of result rows a ``geometry``
column (WKB, null where there's none) and the ``output.attributes`` of the same
place, read from the layers as the caller.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable

import pyarrow as pa
from fastapi import HTTPException, status
from pyiceberg.expressions import In
from sqlalchemy.orm import Session
from teehr_core.spec import EvaluationSpec
from teehr_core.spec.models import WithinField

from src import refs
from src.auth import Identity
from src.models import Project
from src.tables import TableError, Tables

Placer = Callable[[pa.Table], pa.Table]


def _unprocessable(detail: str) -> HTTPException:
    return HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, detail=detail)


def _resolve(session: Session, project: Project, ref: str) -> refs.Resolved:
    found = refs.resolve(session, project, ref)
    if found is None:
        raise _unprocessable(f"output.geometry: no dataset '{ref}'")
    return found


def placer(
    session: Session,
    project: Project,
    identity: Identity,
    tables: Tables,
    spec: EvaluationSpec,
) -> Placer | None:
    """The function that places a page of the spec's results, or None if the spec
    doesn't ask for geometry."""
    column = spec.output.geometry
    if column is None:
        return None
    attributes = list(spec.output.attributes)

    def read(found: refs.Resolved, key: str, values: list[str], wanted: list[str]):
        names = {c["name"] for c in found.dataset.columns or []}
        missing = [c for c in wanted if c not in names]
        if missing:
            raise _unprocessable(
                f"output.attributes: layer '{found.dataset.name}' has no "
                f"{', '.join(missing)}"
            )
        try:
            return tables.read(
                identity.token, found.namespace, found.dataset.name,
                columns=tuple(dict.fromkeys([key, *wanted])),
                row_filter=In(key, values), snapshot_id=None, limit=None,
            ).rows  # fmt: skip
        except TableError as exc:
            raise HTTPException(
                status.HTTP_502_BAD_GATEWAY,
                f"Couldn't read layer '{found.dataset.name}' for output.geometry",
            ) from exc

    within = next(
        (f for f in spec.fields if isinstance(f, WithinField) and f.name == column),
        None,
    )
    if within is not None:
        layer = _resolve(session, project, within.layer)

        def by_label(labels: list[str]) -> dict[str, dict]:
            rows = read(layer, within.attribute, labels, ["geometry", *attributes])
            return {str(r[within.attribute]): r for r in rows.to_pylist()}

        return _attach(column, attributes, by_label)

    observed = _resolve(session, project, spec.inputs.primary.dataset)
    props = observed.dataset.properties or {}
    series = _resolve(session, observed.project, props["series_table"])
    default_layer = props.get("location_layer")

    def by_series(keys: list[str]) -> dict[str, dict]:
        located = read(series, "series_key", keys, ["location_key", "location_layer"])
        wanted: dict[str, dict[str, str]] = defaultdict(dict)  # layer -> loc -> series
        for r in located.to_pylist():
            layer = r.get("location_layer") or default_layer
            if layer and r.get("location_key"):
                wanted[layer][r["location_key"]] = r["series_key"]
        out: dict[str, dict] = {}
        for ref, by_location in wanted.items():
            layer = _resolve(session, observed.project, ref)
            rows = read(
                layer, "location_key", list(by_location), ["geometry", *attributes]
            )
            for r in rows.to_pylist():
                out[by_location[r["location_key"]]] = r
        return out

    return _attach(column, attributes, by_series)


def _attach(
    column: str, attributes: list[str], lookup: Callable[[list[str]], dict[str, dict]]
) -> Placer:
    def place(rows: pa.Table) -> pa.Table:
        if column not in rows.column_names:
            return rows  # not read (``columns`` left it out): nothing to place by
        keys = [None if k is None else str(k) for k in rows[column].to_pylist()]
        found = (
            lookup(sorted({k for k in keys if k is not None})) if rows.num_rows else {}
        )
        places = [found.get(k) if k is not None else None for k in keys]
        out = rows
        for name in attributes:
            if name in out.column_names:
                continue  # a result column of the same name wins
            out = out.append_column(name, pa.array([p and p.get(name) for p in places]))
        geometry = pa.array([p and p.get("geometry") for p in places], pa.binary())
        return out.append_column("geometry", geometry)

    return place
