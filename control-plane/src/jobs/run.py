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
   write the rows as the caller, in the job, beside earlier runs' (only this run's
   rows are replaced: the table keeps every run's, by ``run_id``); the
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
import threading
import time
from collections.abc import Callable, Iterable, Sequence
from datetime import UTC, datetime, timedelta

import httpx
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
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
from src.jobs.grant import GrantClient, LoadFailed, _problem
from src.jobs.reads import CatalogAccess, Read, Warehouse, count_delta
from src.jobs.writes import as_caller, open_tables, report
from src.tables import DataMismatch, Tables, conform

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

    def pin(self) -> None:
        """Pin every input (datasets, their series, location layers) to its current
        snapshot, so every pod of the run reads the same data (ADR-0017)."""
        for project, name, _, _ in input_tables(self.manifest):
            self.wh.pin(project, name)

    def snapshots(self) -> dict[str, int | None]:
        """The snapshot each input was read at (keyed ``<project>.<name>``): the
        next run finds what changed since (ADR-0019)."""
        refs = [f"{p}.{n}" for p, n, _, _ in input_tables(self.manifest)]
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
    result = as_caller(
        client, name,
        lambda token: tables.write(token, project, name, data, mode, expression),
    )  # fmt: skip
    if result.snapshot_id is not None:
        where = overwrite_filter if not append else None
        report(client, name, mode, where, data.num_rows, result.snapshot_id)
    log.info(
        "wrote %s: %s %d rows, snapshot %s (%.1fs)", name, mode, data.num_rows,
        result.snapshot_id, time.monotonic() - started,
    )  # fmt: skip
    return result.snapshot_id


def run_table(client: GrantClient, kind: str, columns: list[dict]) -> str:
    """Have one of the run's tables created (or confirmed); its dataset name."""
    try:
        resp = client._grant(
            "POST", "run-table", json={"kind": kind, "columns": columns}
        )
    except httpx.HTTPStatusError as exc:
        raise LoadFailed([f"the {kind} table: {_problem(exc.response)}"]) from None
    return resp.json()["dataset"]


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
    dataset = run_table(client, kind, columns)
    snapshot = write_table(
        client, tables, project, dataset, columns, rows, overwrite_filter, append
    )
    return {"dataset": dataset, "rows": rows.num_rows, "snapshot_id": snapshot}


def store_results(
    client: GrantClient, tables: Tables, project: str, table: pa.Table, run: dict
) -> dict:
    """Write the run's results beside earlier runs' (a version's results table keeps
    every run's rows, by ``run_id``, as VeriRace's does): only this run's rows are
    replaced, so a repeated write is safe. Pairs and state are replaced instead."""
    return store(client, tables, project, "results", table, run,
                 overwrite_filter=f"run_id = '{run['id']}'")  # fmt: skip


def _parquet_bytes(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd")
    return sink.getvalue().to_pybytes()


class ShardOutputs:
    r"""What a run's shards make, kept outside the pod that made it (ADR-0017):

    - **pairs**, as Parquet files in the pairs table's location, not yet part of it;
      :meth:`commit_pairs` adds them all in one commit (replacing what the run
      replaces), so a run that fails leaves the pairs table as it was;
    - **state parts**, as work files in the staging bucket (the control plane gives
      URLs to put and get them); :meth:`state_parts` reads them back to merge.

    Files are named by run, shard and attempt, so a retried shard's files never mix
    with an abandoned try's. Any pod writes them; pod 0 :meth:`adopt`\ s what the
    queue says the shards wrote, then commits. :meth:`discard` deletes what wasn't
    committed.
    """

    def __init__(self, client: GrantClient, tables: Tables, project: str, run: dict):
        self.client, self.tables, self.project, self.run = client, tables, project, run
        self.pairs_dataset: str | None = None
        self.pairs_columns: list[dict] = []
        self.pair_files: list[str] = []
        self.pair_rows = 0
        self.state_names: list[str] = []

    @staticmethod
    def _name(index: int, attempt: int) -> str:
        return f"shard-{index:05d}-a{attempt}.parquet"

    def add_pairs(self, index: int, attempt: int, pairs: pa.Table) -> dict:
        """Write a shard's pairs as a file; what it wrote (for the queue)."""
        if pairs.num_rows == 0:
            return {}
        rows, columns = typed(pairs, self.run)
        if self.pairs_dataset is None:
            self.pairs_dataset = run_table(self.client, "pairs", columns)
            self.pairs_columns = columns
        name = self.pairs_dataset
        try:
            data = conform(rows, columns, None)
        except DataMismatch as exc:
            raise LoadFailed([f"writing {name}: {exc}"]) from None
        file_name = f"run-{self.run['id']}/{self._name(index, attempt)}"
        path = as_caller(
            self.client, name,
            lambda token: self.tables.write_file(token, self.project, name, data,
                                                 file_name),
        )  # fmt: skip
        self.pair_files.append(path)
        return {"pairs": path, "pair_rows": data.num_rows, "pairs_dataset": name,
                "pairs_columns": columns}  # fmt: skip

    def add_state(self, index: int, attempt: int, part: pa.Table) -> str:
        """Put a shard's state part in the run's work files; its name."""
        name = f"state/{self._name(index, attempt)}"
        url = self._urls(put=[name])["put"][name]
        self.client.put(url, _parquet_bytes(part))
        return name

    def drop(self, out: dict) -> None:
        """Delete the pairs file of an attempt that lost its shard."""
        path = out.get("pairs")
        if path and out.get("pairs_dataset"):
            try:
                self.tables.delete_files(self.client.token(), self.project,
                                         out["pairs_dataset"], [path])  # fmt: skip
            except Exception:  # noqa: BLE001 - orphan cleanup catches it
                log.warning("couldn't delete %s", path)
        if path in self.pair_files:
            self.pair_files.remove(path)

    def adopt(self, outs: list[dict]) -> None:
        """Take over what the shards wrote (pod 0, before committing)."""
        with_pairs = [o for o in outs if o.get("pairs")]
        self.pair_files = [o["pairs"] for o in with_pairs]
        self.pair_rows = sum(o["pair_rows"] for o in with_pairs)
        if with_pairs:
            self.pairs_dataset = with_pairs[0]["pairs_dataset"]
            self.pairs_columns = with_pairs[0]["pairs_columns"]
        self.state_names = [o["state"] for o in outs if o.get("state")]

    def state_parts(self) -> list[pa.Table]:
        if not self.state_names:
            return []
        urls = self._urls(get=self.state_names)["get"]
        return [pq.read_table(pa.BufferReader(self.client.get(urls[n])))
                for n in self.state_names]  # fmt: skip

    def _urls(self, put=(), get=()) -> dict:
        try:
            body = {"put": list(put), "get": list(get)}
            return self.client._grant("POST", "work", json=body).json()
        except httpx.HTTPStatusError as exc:
            raise LoadFailed(
                [f"the run's work files: {_problem(exc.response)}"]
            ) from None

    def commit_pairs(self, overwrite_filter: str | None) -> dict | None:
        """Add the shards' pairs files to the pairs table in one commit, replacing
        the rows ``overwrite_filter`` keeps (all, when None)."""
        if not self.pair_files:
            return None  # no pairs: an earlier run's stay
        name, started = self.pairs_dataset, time.monotonic()
        try:
            expression = overwrite_expression(overwrite_filter, self.pairs_columns)
        except DataMismatch as exc:
            raise LoadFailed([f"writing {name}: {exc}"]) from None
        result = as_caller(
            self.client, name,
            lambda token: self.tables.commit_files(token, self.project, name,
                                                   self.pair_files, expression),
        )  # fmt: skip
        files, self.pair_files = len(self.pair_files), []  # the table's now
        if result.snapshot_id is not None:
            report(self.client, name, "overwrite", overwrite_filter, self.pair_rows,
                    result.snapshot_id)  # fmt: skip
        log.info(
            "committed %s: %d rows in %d files, snapshot %s (%.1fs)", name,
            self.pair_rows, files, result.snapshot_id,
            time.monotonic() - started,
        )  # fmt: skip
        return {
            "dataset": name,
            "rows": self.pair_rows,
            "snapshot_id": result.snapshot_id,
        }

    def discard(self) -> None:
        """Delete files written but not committed, and the work files (best effort:
        the control plane deletes work files when the job finishes too)."""
        if self.pair_files and self.pairs_dataset:
            try:
                self.tables.delete_files(self.client.token(), self.project,
                                         self.pairs_dataset, self.pair_files)  # fmt: skip
            except Exception:  # noqa: BLE001
                log.warning("couldn't delete %d uncommitted pairs files",
                            len(self.pair_files))  # fmt: skip
            self.pair_files = []
        if self.state_names:
            try:
                self.client._grant("DELETE", "work")
            except Exception:  # noqa: BLE001
                pass
            self.state_names = []


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


def input_tables(manifest: dict) -> list[tuple[str, str, str, str | None]]:
    """Every table a run reads, once: (project, name, what, dataset ref), where
    ``what`` is the dataset's kind, ``series`` or ``layer``."""
    out: dict[tuple[str, str], tuple[str, str, str, str | None]] = {}
    for ref, d in manifest["datasets"].items():
        out.setdefault(
            (d["project"], d["name"]), (d["project"], d["name"], d["kind"], ref)
        )
        if d.get("series"):
            s = d["series"]
            out.setdefault(
                (s["project"], s["name"]), (s["project"], s["name"], "series", None)
            )
    for x in manifest.get("location_layers", []):
        out.setdefault(
            (x["project"], x["name"]), (x["project"], x["name"], "layer", None)
        )
    return list(out.values())


def changed_months(
    wh: Warehouse, manifest: dict, spec: EvaluationSpec, since: dict[str, int | None]
) -> dict[str, list[str]]:
    """The months of value_time changed in the run's inputs since the snapshots the
    last run read, up to the ones this run pins, and those of the observations
    (ADR-0019). Raises ``_Full`` when more than months changed."""
    months: set[str] = set()
    observed: set[str] = set()
    for project, name, what, ref in input_tables(manifest):
        where = f"{project}.{name}"
        if where not in since:
            raise _Full(f"the last run didn't record '{where}'")
        found = wh.changes(project, name, since[where])
        if found.unknown:
            raise _Full(f"'{where}': {found.unknown}")
        if not found.changed:
            continue
        if what == "timeseries":
            months.update(found.months)
            if ref == spec.inputs.primary.dataset:
                observed.update(found.months)
        elif what == "series" and found.added_only:
            continue  # new series: their rows arrive as timeseries changes
        elif what == "layer":
            raise _Full(f"'{where}' changed (series' locations)")
        else:
            raise _Full(f"'{where}' changed")
    log.info("changed since the last run: %s", ", ".join(sorted(months)) or "nothing")
    return {"months": sorted(months), "obs_months": sorted(observed)}


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
    client: GrantClient,
    tables: Tables,
    task: dict,
    how: dict,
    wh: Warehouse,
    outputs: ShardOutputs,
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
        if "since" in how:
            how = {**how, **changed_months(wh, manifest, parsed, how["since"])}
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

    if not stateful:  # one read of everything, no shards
        rows, series = inputs.load(None)
        labels = location_labels(inputs.whole, manifest, parsed, rows, series)
        pairs = DuckDBBackend(dm=dm).pairs(
            build_plan(parsed, catalog_from(manifest, rows, series, labels))
        )
        del rows, series
        stages = DuckDBBackend(dm=dm).aggregate(parsed, pairs)
        out = {}
        if run.get("pairs") and pairs.num_rows:
            out["pairs"] = store(client, tables, project, "pairs", pairs, run)
        out = {**store_results(client, tables, project, stages[-1], run), **out}
        return _finished(out, how, inputs, wh, dirty)

    # Shards: ranges of observed series keys (each shard's state merges).
    keys = inputs.shard_keys()
    shards: list[Sequence[str] | None] = [None]
    if keys:
        predicted_rows = inputs.predicted_rows()
        shards = list(key_ranges(keys, predicted_rows / len(keys), rows_per_shard()))
        log.info(
            "%d observed series, %d predicted rows: %d shards of up to %d rows",
            len(keys), predicted_rows, len(shards), rows_per_shard(),
        )  # fmt: skip
    if dirty == set():
        shards = []  # nothing changed: the stored state is the state
    preloaded: dict[int, tuple] = {}
    if shards and not incremental and dm_test:
        # The test's settings come from the predicted rows' time step: the first
        # shard's, kept for this pod to run it (it claims shard 0 first).
        rows, series = inputs.load(shards[0])
        preloaded[0] = (rows, series)
        predicted = [rows[r] for r in _predicted(parsed) if r in rows]
        n = (
            inputs.predicted_rows()
            if shards[0] is not None
            else sum(t.num_rows for t in predicted)
        )
        lags = skill.lag if skill.lag is not None else default_lag(4 * max(n, 1))
        dm = DmSettings(_time_step(predicted) or 3600, lags)
        del rows, series, predicted

    inputs.pin()  # every pod reads what pod 0 planned against
    plan = {
        "mode": how["mode"],
        "dirty": sorted(dirty) if dirty is not None else None,
        "windows": windows,
        "dm": {"step": dm.step, "max_lag": dm.max_lag} if dm else None,
        "p_reach": int(p_reach.total_seconds()),
        "snapshots": dict(wh.snapshots),
        "shards": len(shards),
    }
    queue = ShardQueue(client)
    queue.put(plan, shards)
    work = ShardWork(client, tables, task, plan, wh, outputs, inputs, preloaded)
    queue.work(work)

    # Every shard done: merge their state, then commit.
    outs = queue.outputs()
    outputs.adopt(outs)
    if not incremental:
        reaches = [o.get("reach") or 0 for o in outs]
        p_reach = max([p_reach, *(timedelta(seconds=r) for r in reaches)])
    fresh = merge_shards(parsed, outputs.state_parts())
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
            pa.concat_tables(pieces, promote_options="permissive") if pieces else kept
        )
    if state is None:
        raise PlanError("the evaluation produced no pairs")
    backend = DuckDBBackend(dm=dm, finest_set_only=True)
    stages = backend.summarize(parsed, first_stage_from_state(backend, parsed, state))

    # Computed: now the shards' pairs, in one commit, then results and state.
    pairs_out = outputs.commit_pairs(months_filter(dirty) if incremental else None)
    out = store_results(client, tables, project, stages[-1], run)
    if pairs_out is not None:
        out["pairs"] = pairs_out
    out["state"] = {
        **store(client, tables, project, "state", encode_state(state), run),
        "bucket": STATE_BUCKET,
    }
    if dm is not None and dm_test:
        out["dm"] = {"step": dm.step, "max_lag": dm.max_lag}
    if _persistence(parsed):
        out["persistence_reach"] = int(p_reach.total_seconds())
    if keys:
        out["shards"] = len(shards)
    others = [o.get("reads") for o in outs if o.get("pod") != work.pod]
    return _finished(out, how, inputs, wh, dirty, others)


def _finished(
    out: dict,
    how: dict,
    inputs: Inputs,
    wh: Warehouse,
    dirty: set[str] | None,
    others: Iterable[dict] = (),
) -> dict:
    """The run's result: what it wrote, read, and how it went (full, incremental)."""
    out["inputs"] = inputs.snapshots()
    out["reads"] = wh.read_stats(others)  # this pod's reads and the others' shards
    if how["mode"] == "incremental":
        out["changed"] = how["months"]
        how = {**how, "months": sorted(dirty)}
    return {**out, **{k: how[k] for k in ("mode", "why", "months") if k in how}}


class ShardWork:
    """Runs one shard of a run, in any of its pods, from the task and pod 0's plan
    (ADR-0017): reads the shard's series at the plan's snapshots, pairs them, and
    writes its state part and pairs (``ShardOutputs``); returns what it wrote."""

    def __init__(
        self, client: GrantClient, tables: Tables, task: dict, plan: dict,
        wh: Warehouse, outputs: ShardOutputs, inputs: Inputs | None = None,
        preloaded: dict[int, tuple] | None = None,
    ):  # fmt: skip
        self.run = task["run"]
        self.manifest, spec = self.run["manifest"], self.run["spec"]
        self.parsed = load_spec(spec)
        self.incremental = plan["mode"] == "incremental"
        dirty = set(plan["dirty"]) if plan.get("dirty") is not None else None
        self.counted = months_sql(dirty) if self.incremental else None
        self.p_reach = timedelta(seconds=plan.get("p_reach") or 0)
        wh.snapshots.update(plan.get("snapshots") or {})
        self.inputs = inputs or Inputs(wh, self.manifest, self.parsed, spec,
                                       plan.get("windows"))  # fmt: skip
        dm = DmSettings(**plan["dm"]) if plan.get("dm") else None
        self.backend = DuckDBBackend(dm=dm, finest_set_only=True)
        self.outputs = outputs
        self.preloaded = preloaded or {}  # shards' rows read in planning
        self.wh = wh
        self.pod = os.environ.get("HOSTNAME") or "pod-0"
        self.total = plan.get("shards") or 0

    def __call__(self, index: int, attempt: int, keys: Sequence[str] | None) -> dict:
        parsed, started = self.parsed, time.monotonic()
        counted_before = self.wh.counts()
        rows, series = self.preloaded.pop(index, None) or self.inputs.load(keys)
        self.preloaded.clear()  # another shard: what planning read isn't needed
        rows_read = sum(t.num_rows for t in rows.values())
        reach = _reach(parsed, rows) if _persistence(parsed) else timedelta(0)
        if self.incremental and reach > self.p_reach:
            raise _Full(
                "persistence forecasts now reach further than the stored state allows"
            )
        labels = location_labels(self.inputs.whole, self.manifest, parsed, rows, series)
        catalog = catalog_from(self.manifest, rows, series, labels)
        del rows, series
        pairs = self.backend.pairs(build_plan(parsed, catalog))
        del catalog
        log.info(
            "shard %d/%d (try %d): %s observed series, %d rows read, %d pairs, %.1fs, "
            "peak memory %.0f MB", index + 1, self.total, attempt,
            "all" if keys is None else len(keys), rows_read, pairs.num_rows,
            time.monotonic() - started, peak_memory_mb(),
        )  # fmt: skip
        out: dict = {"reach": int(reach.total_seconds())}
        if pairs.num_rows:
            part = self.backend.first_stage_state(
                parsed, pairs, STATE_BUCKET, self.counted
            )
            out["state"] = self.outputs.add_state(index, attempt, part)
            if self.incremental:  # the pairs the run is for (the rest: lag partners)
                pairs = _sql(pairs, f"SELECT * FROM t WHERE {self.counted}")
        if self.run.get("pairs"):
            out |= self.outputs.add_pairs(index, attempt, pairs)
        out["pod"] = self.pod
        out["reads"] = count_delta(counted_before, self.wh.counts())
        return out


#: Seconds between a shard's heartbeats, and between claims while other pods hold
#: the last shards.
HEARTBEAT_SECONDS = float(os.environ.get("SHARD_HEARTBEAT_SECONDS") or 30)
POLL_SECONDS = 5.0


class _Heartbeat:
    """Tells the control plane a shard's attempt is alive, every few seconds, on a
    thread; ``lost`` once it says another pod has the shard."""

    def __init__(self, client: GrantClient, index: int, attempt: int):
        self.client, self.index, self.attempt = client, index, attempt
        self.lost = False
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._beat, daemon=True)

    def _beat(self) -> None:
        while not self._stop.wait(HEARTBEAT_SECONDS):
            try:
                self.client._grant("POST", f"shards/{self.index}/heartbeat",
                                   json={"attempt": self.attempt})  # fmt: skip
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 409:
                    self.lost = True
                    return
            except Exception:  # noqa: BLE001 - the next beat tries again
                pass

    def __enter__(self) -> "_Heartbeat":
        self._thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self._stop.set()
        self._thread.join()


class ShardQueue:
    """The run's shard queue, through the job grant (the control plane keeps it)."""

    def __init__(self, client: GrantClient, worker: str | None = None):
        self.client = client
        self.worker = worker or os.environ.get("HOSTNAME") or "pod-0"

    def put(self, plan: dict, shards: list) -> None:
        body = {"plan": plan, "shards": [list(s) if s is not None else None
                                         for s in shards]}  # fmt: skip
        self.client._grant("PUT", "shards", json=body)

    def work(self, work: ShardWork) -> None:
        """Claim and run shards until none are left (others may still be running)."""
        while True:
            got = self.client._grant(
                "POST", "shards/claim", json={"worker": self.worker}
            ).json()
            shard = got["shard"]
            if shard is None:
                if got["settled"]:
                    return
                time.sleep(POLL_SECONDS)
                continue
            index, attempt = shard["index"], shard["attempt"]
            beat = _Heartbeat(self.client, index, attempt)
            try:
                with beat:
                    out = work(index, attempt, shard["keys"])
            except _Full as exc:  # the run can't be incremental: pod 0 goes full
                self._fail(index, attempt, f"{FULL_PREFIX}{exc}", retry=False)
                continue
            except Exception as exc:  # noqa: BLE001 - tried again, up to its limit
                log.warning("shard %d (try %d) failed: %s", index, attempt, exc)
                self._fail(index, attempt, _brief(exc), retry=True)
                continue
            if beat.lost or not self._done(index, attempt, out):
                work.outputs.drop(out)  # another pod has the shard now

    def _done(self, index: int, attempt: int, out: dict) -> bool:
        try:
            self.client._grant("POST", f"shards/{index}/done",
                               json={"attempt": attempt, "outputs": out})  # fmt: skip
            return True
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 409:
                return False
            raise

    def _fail(self, index: int, attempt: int, error: str, retry: bool) -> None:
        try:
            self.client._grant("POST", f"shards/{index}/fail",
                               json={"attempt": attempt, "error": error,
                                     "retry": retry})  # fmt: skip
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code != 409:
                raise

    def outputs(self) -> list[dict]:
        """What every shard wrote, in order (pod 0, once they're all settled)."""
        summary = self.client._grant("GET", "shards").json()
        if summary["failed"]:
            first = summary["failed"][0]
            error = first["error"] or "it failed"
            if error.startswith(FULL_PREFIX):
                raise _Full(error.removeprefix(FULL_PREFIX))
            raise LoadFailed([f"shard {first['index']}: {error}"])
        return [d["outputs"] or {} for d in summary["done"]]


FULL_PREFIX = "needs a full run: "


def _brief(exc: Exception) -> str:
    if isinstance(exc, LoadFailed):
        return "; ".join(exc.problems)[:10_000]
    return f"{type(exc).__name__}: {exc}"[:10_000]


def work_shards(client: GrantClient, task: dict) -> None:
    """A run's other pods (ADR-0017): wait for pod 0's plan, then claim and run
    shards until the queue is settled. Pod 0 merges, commits and reports."""
    while True:
        try:
            plan = client._grant("GET", "shards/plan").json()["plan"]
            break
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code != 404:
                raise
            time.sleep(POLL_SECONDS)  # not planned yet
    wh = open_warehouse(client, task)
    tables = open_tables(task)
    outputs = ShardOutputs(client, tables, task["project"], task["run"])
    try:
        ShardQueue(client).work(ShardWork(client, tables, task, plan, wh, outputs))
    finally:
        wh.close()


def run_evaluation(client: GrantClient, task: dict) -> dict:
    """Do a run's task; returns its result (raises LoadFailed with what went wrong).

    An incremental run that finds it can't be one after all runs in full."""
    how = task["run"].get("incremental") or {"mode": "full", "why": "not decided"}
    wh = open_warehouse(client, task)
    tables = open_tables(task)
    project, run = task["project"], task["run"]
    outputs = ShardOutputs(client, tables, project, run)
    try:
        try:
            result = _evaluate(client, tables, task, how, wh, outputs)
        except _Full as exc:
            outputs.discard()
            outputs = ShardOutputs(client, tables, project, run)
            full = {"mode": "full", "why": str(exc)}
            result = _evaluate(client, tables, task, full, wh, outputs)
    except (SpecProblems, PlanError, ContractError, ValueError) as exc:
        raise LoadFailed([str(exc)]) from None
    finally:
        outputs.discard()  # what wasn't committed, and the work files
        wh.close()
    return {
        **result,
        "inputs": result.pop("inputs", None) or {},
        "engine_version": f"teehr-core {ENGINE_VERSION}",
    }
