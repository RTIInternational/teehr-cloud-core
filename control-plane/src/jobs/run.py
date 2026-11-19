"""Running an evaluation version, in the job's pod, as the caller (Phase 5).

1. Read every dataset the run's manifest names (and each timeseries' series table)
   straight from the warehouse as the caller, each pinned to one snapshot
   (``reads.Warehouse``: PyIceberg plans, DuckDB reads; #223). A timeseries input's
   spec ``filter`` goes with the read, so only the rows it keeps are read (the
   engine applies it again).
2. Build teehr-core's engine catalog from the manifest and the rows
   (``evaluations.catalog_from``) and run the spec. A run that keeps state goes in
   shards (VeriRace's design): contiguous ranges of observed series keys, each read
   with its linked predicted series only and sized to the pod's memory; each
   shard's state is kept and the parts merged per month.
3. Add lineage to the results (``run_id``, ``spec_version``, ``engine_version``),
   have the results table created (``results_<slug>_v<version>``, job grant) and
   write the rows as the caller, in the job, replacing an earlier run's rows; the
   control plane records each commit (ADR-0016: run data doesn't pass through it). When the spec keeps pairs
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
import logging
import os
import re
import resource
import sys
import time
from collections.abc import Callable, Iterable, Sequence
from datetime import UTC, datetime, timedelta

import httpx
import pyarrow as pa
import pyarrow.compute as pc
from teehr_core import __version__ as ENGINE_VERSION
from teehr_core.durations import parse_duration
from teehr_core.engine import ContractError, DuckDBBackend, PlanError, build_plan
from teehr_core.engine.connect import arrow_table, memory_connection, register
from teehr_core.engine.duckdb_backend import DmSettings, state_rollup_sql
from teehr_core.engine.shards import key_ranges, shard_rows
from teehr_core.metrics.significance import default_lag
from teehr_core.metrics.state import SKETCH_TYPE
from teehr_core.spec import EvaluationSpec, GeneratedInput, load_spec
from teehr_core.spec.models import AttributeField, WithinField

from src.evaluations import SpecProblems, catalog_from
from src.expressions import overwrite_expression
from src.jobs.load import GrantClient, LoadFailed, _problem
from src.jobs.reads import CatalogAccess, Read, Warehouse
from src.tables import (
    DataMismatch,
    PolarisTables,
    TableError,
    TableForbidden,
    Tables,
    catalog_properties,
    conform,
)

log = logging.getLogger("src.jobs")

#: State is kept per month of value_time (a rerun recomputes changed months only).
STATE_BUCKET = "month"
LINEAGE = ("run_id", "spec_version", "engine_version")
_COLUMN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
_ENCODED = "xs__"


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


def rows_per_shard() -> int:
    """Predicted rows a shard holds: teehr-core's ``shard_rows`` for the pod's memory
    (``RUN_ROWS_PER_SHARD`` overrides)."""
    explicit = os.environ.get("RUN_ROWS_PER_SHARD")
    return int(explicit) if explicit else shard_rows()


def peak_memory_mb() -> float:
    """The pod's peak memory so far (this process's, in MB)."""
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak / 1e6 if sys.platform == "darwin" else peak / 1e3  # bytes / KiB


def open_warehouse(client: GrantClient, task: dict) -> Warehouse:
    """The warehouse, read as the caller (tests replace this)."""
    if not task.get("catalog"):
        raise LoadFailed(["the control plane didn't say where the warehouse is"])
    threads = int(os.environ.get("JOB_THREADS") or 0) or None
    return Warehouse(
        CatalogAccess.from_task(task["catalog"]), client.token, threads=threads
    )


def _observed_keyed(spec: EvaluationSpec) -> set[str]:
    """Datasets keyed by the observed series: the observed input, climatologies, and
    the datasets fields look values up in."""
    refs = {spec.inputs.primary.dataset}
    refs |= {p.from_ for p in spec.inputs.secondary
             if isinstance(p, GeneratedInput) and p.from_}  # fmt: skip
    refs |= {f.from_ for f in spec.fields if getattr(f, "from_", None)}
    return refs


class Inputs:
    """A run's datasets, read from the warehouse: all of them, or one shard's.

    A shard is a range of observed series keys. It reads the datasets keyed by the
    observed series for those keys, the predicted datasets for the series linked to
    them (by the relation, or by the series tables' shared attribute), and the
    relation's rows for them. Series tables are read once, for the run's keys, and
    each shard takes its own rows from them. Anything else (layers, other tables) is
    read whole, once.
    """

    def __init__(
        self,
        warehouse: Warehouse,
        manifest: dict,
        spec: EvaluationSpec,
        raw_spec: dict,
        windows: dict[str, str] | None,
    ):
        self.wh, self.manifest, self.spec, self.windows = (
            warehouse, manifest, spec, windows,
        )  # fmt: skip
        self.filters = _input_filters(raw_spec)
        self.obs_keyed = _observed_keyed(spec)
        self.predicted = set(_predicted(spec))
        self.relation = spec.pair.relation
        reversed_ = manifest.get("relation_reversed")
        self.obs_col, self.pred_col = (
            ("right_key", "left_key") if reversed_ else ("left_key", "right_key")
        )
        self._whole: dict[tuple[str, str], pa.Table] = {}
        self._keys: list[str] | None = None
        self._keys_known = False
        self._run_series: dict[str, pa.Table] = {}
        self._predicted_rows: int | None = None

    def _read(
        self, ref: str, where: str | None, keys: Sequence[str] | None
    ) -> pa.Table:
        d = self.manifest["datasets"][ref]
        return self.wh.read(
            Read(d["project"], d["name"], d["columns"], where, keys=keys)
        )

    def _where(self, ref: str) -> str | None:
        d = self.manifest["datasets"][ref]
        where = self.filters.get(ref)
        if self.windows is not None and d["kind"] == "timeseries":
            where = _both(where, self.windows.get(ref))
        return where

    def whole(self, project: str, name: str, columns: list | None = None) -> pa.Table:
        """A table read whole (once per run)."""
        key = (project, name)
        if key not in self._whole:
            self._whole[key] = self.wh.read(Read(project, name, columns or []))
        return self._whole[key]

    def _dataset_whole(self, ref: str) -> pa.Table:
        d = self.manifest["datasets"][ref]
        return self.whole(d["project"], d["name"], d["columns"])

    def _series_whole(self, ref: str) -> pa.Table | None:
        s = self.manifest["datasets"][ref].get("series")
        return self.whole(s["project"], s["name"], s["columns"]) if s else None

    def shard_keys(self) -> list[str] | None:
        """The observed series keys a run splits into shards (None: it can't)."""
        if not self._keys_known:
            self._keys, self._keys_known = self._find_keys(), True
        return self._keys

    def _find_keys(self) -> list[str] | None:
        if self.relation:
            rel = self._dataset_whole(self.relation)
            return sorted({str(k) for k in rel[self.obs_col].to_pylist()})
        if self.spec.pair.on_attribute:
            obs = self._series_whole(self.spec.inputs.primary.dataset)
            return (
                sorted({str(k) for k in obs["series_key"].to_pylist()}) if obs else None
            )
        return None

    def predicted_rows(self) -> int:
        """Rows of the predicted datasets the run reads: its window's, of the series
        linked to its keys (counted once)."""
        if self._predicted_rows is None:
            keys = self.shard_keys()
            linked = self._linked(keys) if keys else None
            total = 0
            for ref in self.predicted & set(self.manifest["datasets"]):
                d = self.manifest["datasets"][ref]
                total += self.wh.count(Read(d["project"], d["name"], d["columns"],
                                            self._where(ref), keys=linked))  # fmt: skip
            self._predicted_rows = total
        return self._predicted_rows

    def _shard_series(self, ref: str, keys: Sequence[str]) -> pa.Table:
        """``ref``'s series rows for ``keys``: the run's are read once (for every key
        of its side), and each shard's taken from them."""
        if ref not in self._run_series:
            s = self.manifest["datasets"][ref]["series"]
            all_obs = self.shard_keys()
            run_keys = (
                None if all_obs is None
                else all_obs if ref in self.obs_keyed
                else self._linked(all_obs)
            )  # fmt: skip
            self._run_series[ref] = self.wh.read(
                Read(s["project"], s["name"], s["columns"], keys=run_keys)
            )
        rows = self._run_series[ref]
        return rows.filter(
            pc.is_in(rows["series_key"], pa.array(list(keys), pa.string()))
        )

    def _linked(self, obs_keys: Sequence[str]) -> list[str]:
        """The predicted series linked to ``obs_keys``."""
        if self.relation:
            rel = self._dataset_whole(self.relation)
            mask = pc.is_in(rel[self.obs_col], pa.array(list(obs_keys), pa.string()))
            return sorted({str(k) for k in rel.filter(mask)[self.pred_col].to_pylist()})
        attr = self.spec.pair.on_attribute
        obs = self._series_whole(self.spec.inputs.primary.dataset)
        wanted = {
            v for k, v in zip(obs["series_key"].to_pylist(), obs[attr].to_pylist(),
                              strict=True)
            if k in set(obs_keys) and v is not None
        }  # fmt: skip
        out: set[str] = set()
        for ref in self.predicted & set(self.manifest["datasets"]):
            series = self._series_whole(ref)
            if series is not None and attr in series.column_names:
                out |= {str(k) for k, v in zip(series["series_key"].to_pylist(),
                                                series[attr].to_pylist(), strict=True)
                        if v in wanted}  # fmt: skip
        return sorted(out)

    def load(
        self, obs_keys: Sequence[str] | None = None
    ) -> tuple[dict[str, pa.Table], dict[str, pa.Table]]:
        """Every dataset's rows and series tables: all of them, or ``obs_keys``'
        shard's."""
        rows, series = {}, {}
        pred_keys = self._linked(obs_keys) if obs_keys is not None else None
        for ref, d in self.manifest["datasets"].items():
            if self.windows is not None and d["kind"] == "timeseries":
                if not self.windows.get(ref):
                    continue  # nothing of it to read
            keys = None
            if obs_keys is not None:
                if ref in self.obs_keyed:
                    keys = obs_keys
                elif ref in self.predicted:
                    keys = pred_keys
            if ref == self.relation and obs_keys is not None:
                rel = self._dataset_whole(ref)
                mask = pc.is_in(
                    rel[self.obs_col], pa.array(list(obs_keys), pa.string())
                )
                rows[ref] = rel.filter(mask)
            elif keys is None and d["kind"] != "timeseries":
                rows[ref] = self._dataset_whole(ref)
            else:
                rows[ref] = self._read(ref, self._where(ref), keys)
            if d.get("series"):
                s = d["series"]
                series[ref] = (
                    self.whole(s["project"], s["name"], s["columns"]) if keys is None
                    else self._shard_series(ref, keys)
                )  # fmt: skip
        return rows, series

    def snapshots(self) -> dict[str, int | None]:
        """The snapshot each input was read at (keyed ``<project>.<name>``)."""
        refs = []
        for d in self.manifest["datasets"].values():
            refs.append(f"{d['project']}.{d['name']}")
            if d.get("series"):
                refs.append(f"{d['series']['project']}.{d['series']['name']}")
        return {r: self.wh.snapshots[r] for r in refs if r in self.wh.snapshots}


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


def open_tables(task: dict) -> Tables:
    """Table writes as the caller, at the run's catalog (tests replace this)."""
    if not task.get("catalog"):
        raise LoadFailed(["the control plane didn't say where the warehouse is"])
    c = CatalogAccess.from_task(task["catalog"])
    return PolarisTables.at(
        catalog_properties(c.uri, c.warehouse, c.realm, c.region, c.endpoint,
                           c.path_style)
    )  # fmt: skip


def write_table(
    client: GrantClient,
    tables: Tables,
    project: str,
    name: str,
    columns: list[dict],
    table: pa.Table,
    overwrite_filter: str | None = None,
    append: bool = False,
) -> int | None:
    """Replace a run table's rows (those ``overwrite_filter`` keeps, or all) with
    ``table``, or add ``table`` to them (``append``), in one snapshot; returns it.

    The job commits the rows itself, as the caller, with the write gateway's own
    code (conform, filter, PyIceberg write), then reports the commit, which the
    control plane checks and records as the dataset's lineage (ADR-0016)."""
    if table.num_rows == 0:
        return None  # nothing to write; an earlier run's rows stay
    mode = "append" if append else "overwrite"
    started = time.monotonic()
    try:
        data = conform(table, columns, None)
        expression = (
            overwrite_expression(overwrite_filter, columns) if not append else None
        )
    except DataMismatch as exc:
        raise LoadFailed([f"writing {name}: {exc}"]) from None
    try:
        try:
            result = tables.write(client.token(), project, name, data, mode, expression)
        except TableForbidden:  # the token may have gone stale: once more, fresh
            result = tables.write(
                client.token(fresh=True), project, name, data, mode, expression
            )
    except TableForbidden:
        raise LoadFailed([f"writing {name}: the caller may not write it"]) from None
    except TableError as exc:
        raise LoadFailed([f"writing {name}: {exc}"]) from None
    if result.snapshot_id is not None:
        report = {
            "dataset": name, "mode": mode, "filter": overwrite_filter if not append
            else None, "rows": data.num_rows, "snapshot_id": result.snapshot_id,
        }  # fmt: skip
        try:
            client._grant("POST", "writes", json=report)
        except httpx.HTTPStatusError as exc:
            raise LoadFailed(
                [f"recording the write to {name}: {_problem(exc.response)}"]
            ) from None
    log.info(
        "wrote %s: %s %d rows, snapshot %s (%.1fs)", name, mode, data.num_rows,
        result.snapshot_id, time.monotonic() - started,
    )  # fmt: skip
    return result.snapshot_id


def store(
    client: GrantClient,
    tables: Tables,
    project: str,
    kind: str,
    table: pa.Table,
    run: dict,
    overwrite_filter: str | None = None,
    append: bool = False,
) -> dict:
    """Write one of the run's tables (results, pairs, state) with its lineage."""
    rows, columns = typed(table, run)
    try:
        resp = client._grant(
            "POST", "run-table", json={"kind": kind, "columns": columns}
        )
    except httpx.HTTPStatusError as exc:
        raise LoadFailed([f"the {kind} table: {_problem(exc.response)}"]) from None
    dataset = resp.json()["dataset"]
    snapshot = write_table(
        client, tables, project, dataset, columns, rows, overwrite_filter, append
    )
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
        if spec.aggregate[0].finest_set() is not None:
            # State was kept for the finest set only: the coarser sets come from it.
            # Without one (sets that don't nest), every set's rows were kept.
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


def _stored_state(
    wh: Warehouse, project: str, run: dict, snapshot: int | None = None
) -> pa.Table | None:
    """The version's state as the last succeeded run left it, at its ``snapshot``
    (a run that failed after committing state mustn't be built on; VeriRace does
    the same), or the current one when that run didn't say. None if it can't be
    read (an expired snapshot included)."""
    name = f"state_{run['evaluation']}_v{run['version']}"
    if snapshot is not None:
        wh.snapshots[f"{project}.{name}"] = int(snapshot)
    try:
        table = wh.read(Read(project, name, []))
    except Exception:  # noqa: BLE001 - missing, unreadable: the run goes full
        return None
    return decode_state(table)


def merge_shards(spec: EvaluationSpec, parts: list[pa.Table]) -> pa.Table | None:
    """The shards' state parts as one state: a row per group, transform and month
    (groups spanning series, e.g. by lead time, had a part in every shard)."""
    parts = [t for t in parts if t is not None and t.num_rows]
    if not parts:
        return None
    state = pa.concat_tables(parts, promote_options="permissive")
    return (
        state if len(parts) == 1 else DuckDBBackend.merge_state_by_bucket(spec, state)
    )


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


def _layer_key(ref: str | None, owner: str) -> tuple[str, str] | None:
    """A layer reference (``<name>`` in ``owner``'s project, or ``public.<name>``)."""
    if not ref:
        return None
    if ref.startswith("public."):
        return "public", ref.removeprefix("public.")
    return owner, ref


def location_labels(
    layer_rows: Callable[[str, str], pa.Table],
    manifest: dict,
    spec: EvaluationSpec,
    rows: dict[str, pa.Table],
    series_rows: dict[str, pa.Table],
) -> dict[str, pa.Table]:
    """Each observed series' label for the spec's location fields (by field name).

    A series' location is its ``location_key`` in its ``location_layer`` (the
    dataset's default when unset). An attribute field takes the location's
    attribute; a within field the attribute of the polygon of its layer that
    contains the location (by centroid for lines and polygons; boundaries
    included; where polygons overlap, the one with the smallest key).
    """
    import shapely

    fields = {
        f.name: f for f in spec.fields
        if isinstance(f, WithinField) or (isinstance(f, AttributeField) and f.of == "location")
    }  # fmt: skip
    if not fields:
        return {}
    observed = manifest["datasets"][spec.inputs.primary.dataset]
    owner = observed["project"]
    default = observed["properties"].get("location_layer")
    series = series_rows.get(spec.inputs.primary.dataset)
    keys = series["series_key"].to_pylist() if series is not None else []
    located = (
        list(zip(
            series["location_layer"].to_pylist() if "location_layer" in series.column_names
            else [None] * len(keys),
            series["location_key"].to_pylist(), strict=True,
        )) if series is not None else []
    )  # fmt: skip

    layers: dict[tuple[str, str], dict[str, dict]] = {}

    def layer(key: tuple[str, str]) -> dict[str, dict]:
        if key not in layers:
            table = layer_rows(key[0], key[1])
            layers[key] = {r["location_key"]: r for r in table.to_pylist()}
        return layers[key]

    places = []
    for layer_ref, location_key in located:
        key = _layer_key(layer_ref or default, owner)
        places.append(layer(key).get(location_key) if key and location_key else None)

    out = {}
    for name, f in fields.items():
        if isinstance(f, AttributeField):
            values = [p.get(f.attribute) if p else None for p in places]
        else:
            polygons = rows[f.layer].to_pylist()
            shapes = shapely.from_wkb([p["geometry"] for p in polygons])
            tree = shapely.STRtree(shapes)
            points = [
                shapely.centroid(shapely.from_wkb(p["geometry"]))
                if p and p.get("geometry") else None
                for p in places
            ]  # fmt: skip
            values = []
            for point in points:
                if point is None:
                    values.append(None)
                    continue
                hits = tree.query(point, predicate="intersects")
                best = min(
                    hits, key=lambda i: polygons[i]["location_key"], default=None
                )
                values.append(None if best is None else polygons[best].get(f.attribute))
        array = pa.array(values)
        if pa.types.is_null(array.type):
            array = array.cast(pa.string())
        out[name] = pa.table(
            {"series_key": pa.array(keys, pa.string()), "value": array}
        )
    return out


def _evaluate(
    client: GrantClient, tables: Tables, task: dict, how: dict, wh: Warehouse
) -> dict:
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
        stored = _stored_state(wh, project, run, how.get("state_snapshot"))
        if stored is None:
            raise _Full("the stored state couldn't be read")
        if how.get("dm"):
            dm = DmSettings(**how["dm"])
            if dm_test and skill.lag is None:
                total = sum(
                    wh.rows(Read(manifest["datasets"][r]["project"],
                                 manifest["datasets"][r]["name"], []))
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
    inputs = Inputs(wh, manifest, parsed, spec, windows)

    # Shards: ranges of observed series keys, when the run keeps state (each shard's
    # state merges); otherwise one read of everything.
    shards: list[Sequence[str] | None] = [None]
    keys = inputs.shard_keys() if stateful else None
    if keys:
        predicted_rows = inputs.predicted_rows()
        shards = list(key_ranges(keys, predicted_rows / len(keys), rows_per_shard()))
        log.info(
            "%d observed series, %d predicted rows: %d shards of up to %d rows",
            len(keys), predicted_rows, len(shards), rows_per_shard(),
        )  # fmt: skip
    if dirty == set():
        shards = []  # nothing changed: the stored state is the state

    backend = DuckDBBackend(dm=dm, finest_set_only=stateful)
    parts: list[pa.Table] = []
    all_pairs: pa.Table | None = None
    pairs_out: dict | None = None
    counted = months_sql(dirty) if incremental else None
    for i, shard in enumerate(shards):
        started = time.monotonic()
        rows, series = inputs.load(shard)
        rows_read = sum(t.num_rows for t in rows.values())
        if i == 0 and stateful and not incremental and dm_test:
            predicted = [rows[r] for r in _predicted(parsed) if r in rows]
            n = (
                inputs.predicted_rows()
                if shard is not None
                else sum(t.num_rows for t in predicted)
            )
            lags = skill.lag if skill.lag is not None else default_lag(4 * max(n, 1))
            dm = DmSettings(_time_step(predicted) or 3600, lags)
            backend = DuckDBBackend(dm=dm, finest_set_only=stateful)
        if stateful and not incremental:
            p_reach = max(p_reach, _reach(parsed, rows))
        elif incremental and _persistence(parsed) and _reach(parsed, rows) > p_reach:
            raise _Full(
                "persistence forecasts now reach further than the stored state allows"
            )
        labels = location_labels(inputs.whole, manifest, parsed, rows, series)
        catalog = catalog_from(manifest, rows, series, labels)
        del rows, series
        pairs = backend.pairs(build_plan(parsed, catalog))
        del catalog
        log.info(
            "shard %d/%d: %s observed series, %d rows read, %d pairs, %.1fs, "
            "peak memory %.0f MB", i + 1, len(shards),
            "all" if shard is None else len(shard), rows_read, pairs.num_rows,
            time.monotonic() - started, peak_memory_mb(),
        )  # fmt: skip
        if not stateful:
            all_pairs = pairs
            break
        if pairs.num_rows:
            parts.append(
                backend.first_stage_state(parsed, pairs, STATE_BUCKET, counted)
            )
            if incremental:  # the pairs this run is for (the rest were lag partners)
                pairs = _sql(pairs, f"SELECT * FROM t WHERE {counted}")
        if run.get("pairs") and pairs.num_rows:
            first = pairs_out is None
            replaced = months_filter(dirty) if incremental and first else None
            written = store(client, tables, project, "pairs", pairs, run, replaced,
                            append=not first)  # fmt: skip
            pairs_out = (
                written if first
                else {**written, "rows": pairs_out["rows"] + written["rows"]}
            )  # fmt: skip
        del pairs

    state = None
    if stateful:
        fresh = merge_shards(parsed, parts)
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
            pieces = [t for t in (kept, fresh) if t is not None and t.num_rows]
            state = (
                pa.concat_tables(pieces, promote_options="permissive")
                if pieces
                else kept
            )
        if state is None:
            raise PlanError("the evaluation produced no pairs")
        stages = backend.summarize(
            parsed, first_stage_from_state(backend, parsed, state)
        )
    else:
        stages = backend.aggregate(parsed, all_pairs)
        if run.get("pairs") and all_pairs is not None and all_pairs.num_rows:
            pairs_out = store(client, tables, project, "pairs", all_pairs, run)

    out = store(client, tables, project, "results", stages[-1], run)
    if pairs_out is not None:
        out["pairs"] = pairs_out
    if state is not None:
        out["state"] = {
            **store(client, tables, project, "state", encode_state(state), run),
            "bucket": STATE_BUCKET,
        }
        if dm is not None and dm_test:
            out["dm"] = {"step": dm.step, "max_lag": dm.max_lag}
        if _persistence(parsed):
            out["persistence_reach"] = int(p_reach.total_seconds())
    out["inputs"] = inputs.snapshots()
    if keys:
        out["shards"] = len(shards)
    out["reads"] = wh.read_stats()
    if incremental:
        out["changed"] = how["months"]
        how = {**how, "months": sorted(dirty)}
    return {**out, **{k: how[k] for k in ("mode", "why", "months") if k in how}}


def run_evaluation(client: GrantClient, task: dict) -> dict:
    """Do a run's task; returns its result (raises LoadFailed with what went wrong).

    An incremental run that finds it can't be one after all runs in full."""
    how = task["run"].get("incremental") or {"mode": "full", "why": "not decided"}
    wh = open_warehouse(client, task)
    tables = open_tables(task)
    try:
        try:
            result = _evaluate(client, tables, task, how, wh)
        except _Full as exc:
            full = {"mode": "full", "why": str(exc)}
            result = _evaluate(client, tables, task, full, wh)
    except (SpecProblems, PlanError, ContractError, ValueError) as exc:
        raise LoadFailed([str(exc)]) from None
    finally:
        wh.close()
    return {
        **result,
        "inputs": result.pop("inputs", None) or {},
        "engine_version": f"teehr-core {ENGINE_VERSION}",
    }
