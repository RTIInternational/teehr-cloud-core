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
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.compute as pc
from teehr_core import __version__ as ENGINE_VERSION
from teehr_core.durations import parse_duration
from teehr_core.engine import ContractError, DuckDBBackend, PlanError, build_plan
from teehr_core.engine.connect import arrow_table, memory_connection, register
from teehr_core.engine.duckdb_backend import DmSettings, state_rollup_sql
from teehr_core.metrics.significance import default_lag
from teehr_core.metrics.state import SKETCH_TYPE
from teehr_core.spec import EvaluationSpec, GeneratedInput, load_spec

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


def _start(month: str) -> datetime:
    year, number = (int(x) for x in month.split("-"))
    return datetime(year, number, 1, tzinfo=UTC)


def _next(t: datetime) -> datetime:
    return datetime(t.year + (t.month == 12), t.month % 12 + 1, 1, tzinfo=UTC)


def spread(months: Iterable[str], reach: timedelta) -> set[str]:
    """``months`` and the months up to ``reach`` after each."""
    out = set()
    for month in months:
        t = _start(month)
        end = _next(t) + reach
        while t < end:
            out.add(f"{t.year:04d}-{t.month:02d}")
            t = _next(t)
    return out


def _windows(months: Iterable[str], lookback: timedelta, form: str) -> str:
    def at(t: datetime) -> str:
        return form.format(t.strftime("%Y-%m-%dT%H:%M:%SZ"))

    return " OR ".join(
        f"(value_time >= {at(_start(m) - lookback)} AND value_time < {at(_next(_start(m)))})"
        for m in sorted(months)
    )


def months_filter(months: Iterable[str], lookback: timedelta = timedelta(0)) -> str:
    """A filter (the shared expression language) keeping rows whose value_time is in
    one of ``months`` (YYYY-MM), or up to ``lookback`` before one."""
    return _windows(months, lookback, "'{}'")


def months_sql(months: Iterable[str], lookback: timedelta = timedelta(0)) -> str:
    """The same, as DuckDB SQL (for the engine's pairs)."""
    return _windows(months, lookback, "TIMESTAMPTZ '{}'")


def _both(a: str | None, b: str | None) -> str | None:
    return f"({a}) AND ({b})" if a and b else a or b


def read_inputs(
    client: GrantClient,
    manifest: dict,
    spec: dict,
    windows: dict[str, str] | None = None,
) -> tuple[dict[str, pa.Table], dict[str, pa.Table], dict[str, int | None]]:
    """The rows of every dataset in the manifest, their series tables', and the
    snapshot each was read at (keyed ``<project>.<name>``). With ``windows`` (an
    incremental run), a timeseries' rows are only those its window keeps (none
    without one)."""
    filters = _input_filters(spec)
    rows, series, snapshots = {}, {}, {}
    for ref, d in manifest["datasets"].items():
        row_filter = filters.get(ref)
        if windows is not None and d["kind"] == "timeseries":
            if not windows.get(ref):
                continue  # nothing of it to read
            row_filter = _both(row_filter, windows[ref])
        rows[ref], snapshots[f"{d['project']}.{d['name']}"] = read_all(
            client, d["project"], d["name"], row_filter
        )
        if d.get("series"):
            s = d["series"]
            series[ref], snapshots[f"{s['project']}.{s['name']}"] = read_all(
                client, s["project"], s["name"]
            )
    return rows, series, snapshots


def count_rows(client: GrantClient, project: str, name: str) -> int:
    resp = client.api(
        "GET", f"/projects/{project}/datasets/{name}/data",
        params={"limit": 1, "count": "true"}, headers={"Accept": ARROW},
    )  # fmt: skip
    if resp.status_code != 200:
        raise LoadFailed([f"counting {project}.{name}: {_problem(resp)}"])
    return int(resp.headers.get("x-total-rows") or 0)


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
    return not DuckDBBackend.state_blockers(spec)


def first_stage_from_state(
    backend: DuckDBBackend, spec: EvaluationSpec, state: pa.Table
) -> pa.Table:
    """The first stage from state rows. With grouping sets (rollups) state is kept
    for the finest set only; the coarser sets' rows come from it (state merges)."""
    con = memory_connection()
    try:
        register(con, "s", state)
        source = "s"
        if spec.aggregate[0].grouping_sets:
            source = state_rollup_sql(spec.aggregate[0], "s")
        merged = arrow_table(con, DuckDBBackend.merge_state_sql(spec, source))
        dm_sql = DuckDBBackend.merge_dm_sql(spec, source)
        dm = arrow_table(con, dm_sql) if dm_sql else None
        boot_sql = DuckDBBackend.bootstrap_state_sql(spec, source)
        units = arrow_table(con, boot_sql) if boot_sql else None
    finally:
        con.close()
    return backend.first_stage_from_merged(spec, merged, dm, units)


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


class _Full(Exception):
    """An incremental run can't be done after all; a full one is (the reason)."""


def _sql(table: pa.Table, query: str) -> pa.Table:
    con = memory_connection()
    try:
        con.execute("SET TimeZone = 'UTC'")
        register(con, "t", table)
        return arrow_table(con, query)
    finally:
        con.close()


def _time_step(tables: list[pa.Table]) -> int | None:
    """The smallest spacing (seconds) of value times within a series' run."""
    steps = []
    for table in tables:
        if table.num_rows < 2:
            continue
        run = (
            "series_key, reference_time"
            if "reference_time" in table.column_names
            else "series_key"
        )
        row = _sql(table, (
            "SELECT min(d) AS d FROM (SELECT epoch(value_time) - lag(epoch(value_time)) "
            f"OVER (PARTITION BY {run} ORDER BY value_time) AS d FROM t) WHERE d > 0"
        )).to_pylist()[0]  # fmt: skip
        if row["d"]:
            steps.append(int(row["d"]))
    return min(steps) if steps else None


def _max_lead(table: pa.Table) -> timedelta:
    """The longest lead time of a forecast's latest run in ``table``."""
    if table.num_rows == 0 or "reference_time" not in table.column_names:
        return timedelta(0)
    row = _sql(table, (
        "SELECT max(epoch(value_time) - epoch(reference_time)) AS s FROM t "
        "WHERE reference_time = (SELECT max(reference_time) FROM t)"
    )).to_pylist()[0]  # fmt: skip
    return timedelta(seconds=row["s"] or 0)


def _persistence(spec: EvaluationSpec) -> list[GeneratedInput]:
    return [
        p for p in spec.inputs.secondary
        if isinstance(p, GeneratedInput) and p.generate == "persistence"
    ]  # fmt: skip


def _predicted(spec: EvaluationSpec) -> list[str]:
    """The datasets of the predicted side: inputs, and persistence's forecasts."""
    refs = [
        p.dataset for p in spec.inputs.secondary if not isinstance(p, GeneratedInput)
    ]
    return refs + [p.like for p in _persistence(spec) if p.like]


def _reach(spec: EvaluationSpec, rows: dict[str, pa.Table]) -> timedelta:
    """How far after an observation a persistence forecast made from it can be valid."""
    out = timedelta(0)
    for p in _persistence(spec):
        age = parse_duration(p.max_age) if p.max_age else timedelta(0)
        out = max(out, age + _max_lead(rows.get(p.like, pa.table({}))))
    return out


def _evaluate(client: GrantClient, task: dict, how: dict) -> dict:
    run, project = task["run"], task["project"]
    manifest, spec = run["manifest"], run["spec"]
    parsed = load_spec(spec)
    stateful = keeps_state(parsed)
    skill = parsed.aggregate[0].skill
    dm_test = skill is not None and skill.test is not None
    incremental = how["mode"] == "incremental"

    stored, windows, dirty = None, None, None
    dm: DmSettings | None = None
    p_reach = timedelta(0)
    if incremental:
        stored = _stored_state(client, project, run)
        if stored is None:
            raise _Full("the stored state couldn't be read")
        if how.get("dm"):
            dm = DmSettings(**how["dm"])
            if dm_test and skill.lag is None:
                total = sum(
                    count_rows(client, manifest["datasets"][r]["project"],
                               manifest["datasets"][r]["name"])
                    for r in _predicted(parsed) if r in manifest["datasets"]
                )  # fmt: skip
                needed = default_lag(max(total, 1))
                if needed > (dm.max_lag or 0):
                    raise _Full(
                        f"the test now needs {needed} lags; the state keeps {dm.max_lag}"
                    )
        p_reach = timedelta(seconds=how.get("persistence_reach") or 0)
        lag_reach = (
            timedelta(seconds=(dm.step or 0) * (dm.max_lag or 0))
            if dm
            else timedelta(0)
        )
        dirty = (
            set(how["months"])
            | spread(how.get("obs_months", []), p_reach + lag_reach)
            | spread(how["months"], lag_reach)
        )
        observed = parsed.inputs.primary.dataset
        windows = {
            ref: months_filter(
                dirty, (p_reach if ref == observed else timedelta(0)) + lag_reach
            )
            for ref, d in manifest["datasets"].items()
            if d["kind"] == "timeseries" and dirty
        }
    rows, series, snapshots = read_inputs(client, manifest, spec, windows)

    if stateful and not incremental:
        if dm_test:
            predicted = [rows[r] for r in _predicted(parsed) if r in rows]
            n = sum(t.num_rows for t in predicted)
            lags = skill.lag if skill.lag is not None else default_lag(4 * max(n, 1))
            dm = DmSettings(_time_step(predicted) or 3600, lags)
        p_reach = _reach(parsed, rows)
    elif incremental and _persistence(parsed) and _reach(parsed, rows) > p_reach:
        raise _Full(
            "persistence forecasts now reach further than the stored state allows"
        )

    backend = DuckDBBackend(dm=dm, finest_set_only=stateful)
    pairs, state = None, None
    if dirty != set():
        pairs = backend.pairs(build_plan(parsed, catalog_from(manifest, rows, series)))
    if stateful:
        fresh = None
        if pairs is not None and pairs.num_rows:
            counted = months_sql(dirty) if incremental else None
            fresh = backend.first_stage_state(parsed, pairs, STATE_BUCKET, counted)
            if incremental:  # the pairs this run is for (the rest were lag partners)
                pairs = _sql(pairs, f"SELECT * FROM t WHERE {counted}")
        if not incremental:
            state = fresh
        else:  # the stored state, its changed months replaced
            kept = stored.filter(
                pc.invert(
                    pc.is_in(
                        _months_of(stored["__bucket"]),
                        pa.array(sorted(dirty), pa.string()),
                    )
                )
            )
            parts = [t for t in (kept, fresh) if t is not None and t.num_rows]
            state = (
                pa.concat_tables(parts, promote_options="permissive") if parts else kept
            )
        if state is None:
            raise PlanError("the evaluation produced no pairs")
        stages = backend.summarize(
            parsed, first_stage_from_state(backend, parsed, state)
        )
    else:
        stages = backend.aggregate(parsed, pairs)

    out = store(client, project, "results", stages[-1], run)
    if run.get("pairs") and pairs is not None and pairs.num_rows:
        replaced = months_filter(dirty) if incremental else None
        out["pairs"] = store(client, project, "pairs", pairs, run, replaced)
    if state is not None:
        out["state"] = {
            **store(client, project, "state", encode_state(state), run),
            "bucket": STATE_BUCKET,
        }
        if dm is not None and dm_test:
            out["dm"] = {"step": dm.step, "max_lag": dm.max_lag}
        if _persistence(parsed):
            out["persistence_reach"] = int(p_reach.total_seconds())
    out["inputs"] = snapshots
    if incremental:
        out["changed"] = how["months"]
        how = {**how, "months": sorted(dirty)}
    return {**out, **{k: how[k] for k in ("mode", "why", "months") if k in how}}


def run_evaluation(client: GrantClient, task: dict) -> dict:
    """Do a run's task; returns its result (raises LoadFailed with what went wrong).

    An incremental run that finds it can't be one after all runs in full."""
    how = task["run"].get("incremental") or {"mode": "full", "why": "not decided"}
    try:
        try:
            result = _evaluate(client, task, how)
        except _Full as exc:
            result = _evaluate(client, task, {"mode": "full", "why": str(exc)})
    except (SpecProblems, PlanError, ContractError, ValueError) as exc:
        raise LoadFailed([str(exc)]) from None
    return {
        **result,
        "inputs": result.pop("inputs", None) or {},
        "engine_version": f"teehr-core {ENGINE_VERSION}",
    }
