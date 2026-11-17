"""Running an evaluation version, in the job's pod, as the caller (Phase 5).

1. Read every dataset the run's manifest names (and each timeseries' series table)
   through the control plane's read API as the caller, in Arrow pages pinned to
   one snapshot each. A timeseries input's spec ``filter`` goes with the read, so
   only the rows it keeps leave the warehouse (the engine applies it again).
2. Build teehr-core's engine catalog from the manifest and the rows
   (``evaluations.catalog_from``) and run the spec.
3. Add lineage to the results (``run_id``, ``spec_version``, ``engine_version``),
   have the results table created (``results_<slug>_v<version>``, job grant) and
   write the rows through the write API as the caller: the first chunk replaces
   an earlier run's rows, the rest append. When the spec keeps pairs
   (``output.pairs: all``), the paired rows go to ``pairs_<slug>_v<version>`` the
   same way.

State: when the first stage can be computed from mergeable state (teehr-core's
engine says which can), the run computes it per group and month of value_time,
keeps it in ``state_<slug>_v<version>``, and computes the results from it, so a
full run and an incremental one (which recomputes only changed months) give the
same results. State column names that datasets can't hold (``+crps|fair|sum``,
``~obs``) are stored hex-encoded, and quantile sketches as JSON text.

The inputs' snapshot ids are the run's result, with the rows written, so a run
says exactly what it read.
"""

import json
import re
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.compute as pc
from teehr_core import __version__ as ENGINE_VERSION
from teehr_core.engine import ContractError, DuckDBBackend, PlanError, build_plan
from teehr_core.metrics.state import SKETCH_TYPE
from teehr_core.spec import EvaluationSpec, load_spec

from src.evaluations import SpecProblems, catalog_from
from src.jobs.load import ARROW, GrantClient, LoadFailed, _problem, arrow_bytes, chunks

PAGE_ROWS = 1_000_000  # the API's largest binary page
#: State is kept per month of value_time (a rerun recomputes changed months only).
STATE_BUCKET = "month"
LINEAGE = ("run_id", "spec_version", "engine_version")
_COLUMN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
_ENCODED = "xs__"


def read_all(
    client: GrantClient, project: str, name: str, row_filter: str | None = None
) -> tuple[pa.Table, int | None]:
    """A dataset's rows (those ``row_filter`` keeps), page by page from one
    snapshot; and that snapshot's id."""
    parts, cursor, snapshot = [], None, None
    while True:
        params: dict[str, Any] = {"limit": PAGE_ROWS}
        if row_filter:
            params["filter"] = row_filter
        if cursor:
            params["cursor"] = cursor
        resp = client.api(
            "GET", f"/projects/{project}/datasets/{name}/data",
            params=params, headers={"Accept": ARROW},
        )  # fmt: skip
        if resp.status_code != 200:
            raise LoadFailed([f"reading {project}.{name}: {_problem(resp)}"])
        parts.append(pa.ipc.open_stream(resp.content).read_all())
        snapshot = snapshot or resp.headers.get("x-snapshot-id")
        cursor = resp.headers.get("x-next-cursor")
        if not cursor:
            break
    table = pa.concat_tables(parts) if len(parts) > 1 else parts[0]
    return table, int(snapshot) if snapshot else None


def _input_filters(spec: dict) -> dict[str, str]:
    """Each timeseries input's filter, by the name the spec gives the dataset."""
    inputs = spec["inputs"]
    secondary = inputs["secondary"]
    sides = [
        inputs["primary"],
        *(secondary if isinstance(secondary, list) else [secondary]),
    ]
    return {
        s["dataset"]: s["filter"] for s in sides if s.get("dataset") and s.get("filter")
    }


def months_filter(months: list[str], column: str = "value_time") -> str:
    """An expression keeping rows whose ``column`` is in one of ``months`` (YYYY-MM)."""
    parts = []
    for month in months:
        year, number = (int(x) for x in month.split("-"))
        end = (year + 1, 1) if number == 12 else (year, number + 1)
        parts.append(
            f"({column} >= '{year:04d}-{number:02d}-01T00:00:00Z' AND "
            f"{column} < '{end[0]:04d}-{end[1]:02d}-01T00:00:00Z')"
        )
    return " OR ".join(parts)


def _both(a: str | None, b: str | None) -> str | None:
    return f"({a}) AND ({b})" if a and b else a or b


def read_inputs(
    client: GrantClient, manifest: dict, spec: dict, months: list[str] | None = None
) -> tuple[dict[str, pa.Table], dict[str, pa.Table], dict[str, int | None]]:
    """The rows of every dataset in the manifest, their series tables', and the
    snapshot each was read at (keyed ``<project>.<name>``). With ``months`` (an
    incremental run), a timeseries' rows are only those months'."""
    filters = _input_filters(spec)
    rows, series, snapshots = {}, {}, {}
    for ref, d in manifest["datasets"].items():
        row_filter = filters.get(ref)
        if months is not None and d["kind"] == "timeseries":
            if not months:
                continue  # nothing changed: no rows to read
            row_filter = _both(row_filter, months_filter(months))
        rows[ref], snapshots[f"{d['project']}.{d['name']}"] = read_all(
            client, d["project"], d["name"], row_filter
        )
        if d.get("series"):
            s = d["series"]
            series[ref], snapshots[f"{s['project']}.{s['name']}"] = read_all(
                client, s["project"], s["name"]
            )
    return rows, series, snapshots


def _column_type(t: pa.DataType) -> tuple[str, pa.DataType]:
    """The dataset column type for an Arrow type, and the Arrow type to cast to."""
    if pa.types.is_dictionary(t):
        t = t.value_type
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return "string", pa.string()
    if pa.types.is_boolean(t):
        return "boolean", pa.bool_()
    if pa.types.is_integer(t):
        small = t.bit_width < 32 or (
            t.bit_width == 32 and pa.types.is_signed_integer(t)
        )
        return ("int32", pa.int32()) if small else ("int64", pa.int64())
    if pa.types.is_float16(t) or pa.types.is_float32(t):
        return "float32", pa.float32()
    if pa.types.is_floating(t) or pa.types.is_null(t) or pa.types.is_decimal(t):
        return "float64", pa.float64()
    if pa.types.is_timestamp(t):
        if t.tz:
            return "timestamptz", pa.timestamp("us", tz="UTC")
        return "timestamp", pa.timestamp("us")
    if pa.types.is_date(t):
        return "date", pa.date32()
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        return "binary", pa.binary()
    raise LoadFailed([f"results have a column of type {t}, which datasets can't hold"])


def typed(table: pa.Table, run: dict) -> tuple[pa.Table, list[dict]]:
    """A run's table with lineage, typed as dataset columns, and those columns."""
    n = table.num_rows
    table = table.append_column("run_id", pa.array([run["id"]] * n, pa.string()))
    table = table.append_column(
        "spec_version", pa.array([run["version"]] * n, pa.int32())
    )
    table = table.append_column(
        "engine_version", pa.array([f"teehr-core {ENGINE_VERSION}"] * n, pa.string())
    )
    columns, arrays = [], []
    for field in table.schema:
        name, arrow = _column_type(field.type)
        columns.append({"name": field.name, "type": name, "required": False})
        arrays.append(pc.cast(table[field.name], arrow))
    return pa.Table.from_arrays(arrays, names=table.column_names), columns


def write_table(
    client: GrantClient,
    project: str,
    name: str,
    table: pa.Table,
    overwrite_filter: str | None = None,
) -> int | None:
    """Replace a run table's rows (those ``overwrite_filter`` keeps, or all) with
    ``table`` (chunked); the last snapshot."""
    path = f"/projects/{project}/datasets/{name}/data"
    snapshot = None
    pieces = list(chunks(table)) if table.num_rows else [table]
    for i, chunk in enumerate(pieces):
        if chunk.num_rows == 0:
            break  # nothing to write; an earlier run's rows stay
        params = {"mode": "overwrite" if i == 0 else "append"}
        if i == 0 and overwrite_filter:
            params["filter"] = overwrite_filter
        resp = client.api(
            "POST", path, params=params, content=arrow_bytes(chunk),
            headers={"Content-Type": ARROW},
        )  # fmt: skip
        if resp.status_code != 201:
            raise LoadFailed([f"writing {name}: {_problem(resp)}"])
        snapshot = resp.json().get("snapshot_id")
    return snapshot


def store(
    client: GrantClient,
    project: str,
    kind: str,
    table: pa.Table,
    run: dict,
    overwrite_filter: str | None = None,
) -> dict:
    """Write one of the run's tables (results, pairs) with its lineage."""
    rows, columns = typed(table, run)
    try:
        resp = client._grant(
            "POST", "run-table", json={"kind": kind, "columns": columns}
        )
    except httpx.HTTPStatusError as exc:
        raise LoadFailed([f"the {kind} table: {_problem(exc.response)}"]) from None
    dataset = resp.json()["dataset"]
    snapshot = write_table(client, project, dataset, rows, overwrite_filter)
    return {"dataset": dataset, "rows": rows.num_rows, "snapshot_id": snapshot}


def keeps_state(spec: EvaluationSpec) -> bool:
    """Whether runs of ``spec`` keep state (and compute results from it)."""
    stage = spec.aggregate[0]
    return (
        not DuckDBBackend.state_blockers(spec)
        and not stage.grouping_sets  # coarser sets are built from state: later
        and not (stage.skill is not None and stage.skill.test is not None)  # DM: later
    )


def encode_state(state: pa.Table) -> pa.Table:
    """State rows as dataset columns: names a dataset can't hold hex-encoded,
    sketches (lists of {b, w}) as JSON text."""
    names, arrays = [], []
    for field in state.schema:
        column = state[field.name]
        if pa.types.is_list(field.type):
            column = pa.array(
                [None if v is None else json.dumps([[e["b"], e["w"]] for e in v])
                 for v in column.to_pylist()],
                pa.string(),
            )  # fmt: skip
        ok = _COLUMN.match(field.name) and not field.name.startswith(_ENCODED)
        names.append(field.name if ok else _ENCODED + field.name.encode().hex())
        arrays.append(column)
    return pa.Table.from_arrays(arrays, names=names)


def decode_state(table: pa.Table) -> pa.Table:
    """The engine's state rows from a stored state table (lineage dropped)."""
    names, arrays = [], []
    for name in table.column_names:
        if name in LINEAGE:
            continue
        column = table[name]
        if name.startswith(_ENCODED):
            name = bytes.fromhex(name.removeprefix(_ENCODED)).decode()
        if name.startswith("~"):  # a quantile sketch
            column = pa.array(
                [None if v is None else [{"b": b, "w": w} for b, w in json.loads(v)]
                 for v in column.to_pylist()],
                SKETCH_TYPE,
            )  # fmt: skip
        names.append(name)
        arrays.append(column)
    return pa.Table.from_arrays(arrays, names=names)


def _stored_state(client: GrantClient, project: str, run: dict) -> pa.Table | None:
    """The version's state as the last run left it (None if it can't be read)."""
    name = f"state_{run['evaluation']}_v{run['version']}"
    try:
        table, _ = read_all(client, project, name)
    except LoadFailed:
        return None
    return decode_state(table)


def _months_of(bucket: pa.ChunkedArray) -> pa.ChunkedArray:
    return pc.strftime(bucket, format="%Y-%m", locale="C")


def run_evaluation(client: GrantClient, task: dict) -> dict:
    """Do a run's task; returns its result (raises LoadFailed with what went wrong)."""
    run, project = task["run"], task["project"]
    manifest, spec = run["manifest"], run["spec"]
    how = run.get("incremental") or {"mode": "full", "why": "not decided"}
    stored = None
    if how["mode"] == "incremental":
        stored = _stored_state(client, project, run)
        if stored is None:
            how = {"mode": "full", "why": "the stored state couldn't be read"}
    months = how["months"] if how["mode"] == "incremental" else None
    rows, series, snapshots = read_inputs(client, manifest, spec, months)
    backend = DuckDBBackend()
    pairs = None
    try:
        parsed = load_spec(spec)
        if months != []:
            catalog = catalog_from(manifest, rows, series)
            pairs = backend.pairs(build_plan(parsed, catalog))
        state = None
        if keeps_state(parsed):
            fresh = None
            if pairs is not None and pairs.num_rows:
                fresh = backend.first_stage_state(parsed, pairs, STATE_BUCKET)
            if months is None:
                state = fresh
            else:  # the stored state, its changed months replaced
                kept = stored.filter(
                    pc.invert(
                        pc.is_in(
                            _months_of(stored["__bucket"]),
                            pa.array(months, pa.string()),
                        )
                    )
                )
                parts = [t for t in (kept, fresh) if t is not None and t.num_rows]
                state = (
                    pa.concat_tables(parts, promote_options="permissive")
                    if parts
                    else kept
                )
            if state is None:
                raise PlanError("the evaluation produced no pairs")
            stages = backend.summarize(
                parsed, backend.first_stage_from_state(parsed, state)
            )
        else:
            stages = backend.aggregate(parsed, pairs)
    except (SpecProblems, PlanError, ContractError, ValueError) as exc:
        raise LoadFailed([str(exc)]) from None
    out = store(client, project, "results", stages[-1], run)
    if run.get("pairs") and pairs is not None and pairs.num_rows:
        replaced = None if months is None else months_filter(months)
        out["pairs"] = store(client, project, "pairs", pairs, run, replaced)
    if state is not None:
        out["state"] = {
            **store(client, project, "state", encode_state(state), run),
            "bucket": STATE_BUCKET,
        }
    return {
        **out,
        **how,
        "inputs": snapshots,
        "engine_version": f"teehr-core {ENGINE_VERSION}",
    }
