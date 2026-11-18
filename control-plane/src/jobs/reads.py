"""Reading a run's inputs from the warehouse, in the job's pod, as the caller (#223).

Run jobs used to read their inputs through ``/data``, which materializes every page
in the control plane and re-reads from the start for each one: a day of a big
forecast took the control plane down. Here the job reads the tables itself:

- **PyIceberg plans, once per table and filter.** It loads each table from Polaris as
  the caller (ADR-0004), with storage credentials Polaris vends for that table, and
  lists the data files the filter can match at the snapshot the run is pinned to:
  partitions and files whose column statistics rule it out are skipped. A shard's
  read then takes, from that plan, the files whose key bounds can hold one of its
  keys (#228): no catalog round trip per shard.
- **DuckDB reads** those Parquet files, applying the filter and the key list as it
  goes, so only the rows a read keeps reach Arrow.

A table with delete files (merge-on-read) is read by PyIceberg instead, which applies
them; the control plane's own writes don't make any.
"""

from __future__ import annotations

import bisect
import hashlib
import logging
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa
from pyiceberg import expressions as ice
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.table import Table
from teehr_core.engine.connect import arrow_table, memory_connection
from teehr_core.engine.memory import RUN_SHARE, default_memory_limit
from teehr_core.engine.shards import in_list
from teehr_core.spec.expressions import to_duckdb
from teehr_core.sql import lit, q

from src.expressions import compile_filter
from src.tables import DataMismatch

__all__ = ["CatalogAccess", "Read", "Warehouse"]

log = logging.getLogger("src.jobs")

#: How long a loaded table (and the storage credentials vended with it) is reused
#: before it's loaded again; vended credentials last an hour or more.
TABLE_TTL_SECONDS = 15 * 60


def _may_hold(data_file: Any, field_id: int, wanted: list[bytes]) -> bool:
    """Whether a data file's bounds for a string column can hold one of ``wanted``
    (sorted, UTF-8). Bounds may be truncated, but stay bounds (a lower one is a
    prefix, an upper one is rounded up); a file without them may hold anything."""
    lower = (data_file.lower_bounds or {}).get(field_id)
    upper = (data_file.upper_bounds or {}).get(field_id)
    i = bisect.bisect_left(wanted, lower) if lower is not None else 0
    return i < len(wanted) and (upper is None or wanted[i] <= upper)


def _overlaps(wanted: list[str], low: str | None, high: str | None) -> bool:
    """Whether sorted ``wanted`` has a key in [low, high] (a missing bound: open)."""
    i = bisect.bisect_left(wanted, low) if low is not None else 0
    return i < len(wanted) and (high is None or wanted[i] <= high)


@dataclass(frozen=True)
class CatalogAccess:
    """Where the warehouse catalog is and how its storage is reached (the run's task
    carries it; nothing in it is secret)."""

    uri: str  # the Iceberg REST endpoint (Polaris's /api/catalog)
    warehouse: str  # the catalog's name
    realm: str
    region: str = "us-east-1"
    endpoint: str | None = None  # object storage, when not AWS S3 (KinD's RustFS)
    path_style: bool = False

    @classmethod
    def from_task(cls, data: dict[str, Any]) -> CatalogAccess:
        return cls(**data)


@dataclass(frozen=True)
class Read:
    """Which rows of a table to read: those whose ``key`` column is one of ``keys``
    (all, when None) and that ``where`` keeps (a filter in the shared expression
    language), at the table's pinned snapshot."""

    project: str
    name: str
    columns: list[dict]  # the dataset's columns (to check ``where`` against)
    where: str | None = None
    key: str = "series_key"
    keys: Sequence[str] | None = None


@dataclass
class Warehouse:
    """Reads tables as the caller, each pinned to the snapshot it had when first read."""

    access: CatalogAccess
    token: Callable[[], str]  # the caller's access token, fresh
    memory_limit: str | None = None
    threads: int | None = None  # DuckDB's (S3 requests run on them); None: its default
    snapshots: dict[str, int | None] = field(default_factory=dict)
    # Per table: the reads, the files and row groups they read (each, and distinct)
    # and the bytes.
    _planned: dict[str, dict[str, Any]] = field(default_factory=dict, repr=False)
    # Per file: its row groups' key bounds and compressed bytes (from its footer).
    _groups: dict[str, list[tuple[str | None, str | None, int]]] = field(
        default_factory=dict, repr=False
    )
    # Loaded tables (with when), and each table's files per filter (at its snapshot).
    _tables: dict[str, tuple[Table, float]] = field(default_factory=dict, repr=False)
    _plans: dict[tuple[str, str | None], list] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        self._con = memory_connection()
        limit = self.memory_limit or default_memory_limit(RUN_SHARE)
        self._con.execute(f"SET TimeZone = 'UTC'; SET memory_limit = '{limit}'")
        # Footers read for the row-group stats are reused by the read that follows.
        self._con.execute("SET parquet_metadata_cache = true")
        if self.threads:
            self._con.execute(f"SET threads = {int(self.threads)}")
        self._con.install_extension("httpfs")
        self._con.load_extension("httpfs")
        self._catalog: Catalog | None = None
        self._catalog_token = ""

    # -- catalog ---------------------------------------------------------------------

    def catalog(self) -> Catalog:
        token = self.token()
        if self._catalog is None or token != self._catalog_token:
            props = {
                "type": "rest",
                "uri": self.access.uri,
                "warehouse": self.access.warehouse,
                "token": token,
                "header.X-Polaris-Realm": self.access.realm,
                # Storage credentials scoped to each table, from Polaris.
                "header.X-Iceberg-Access-Delegation": "vended-credentials",
                "s3.region": self.access.region,
            }
            if self.access.endpoint:
                props["s3.endpoint"] = self.access.endpoint
            if self.access.path_style:
                props["s3.path-style-access"] = "true"
            self._catalog = load_catalog("polaris", **props)
            self._catalog_token = token
        return self._catalog

    def table(self, project: str, name: str) -> tuple[Table, int | None]:
        """The table and its pinned snapshot (its current one, the first time it's
        read). Loaded once, and again when its vended credentials may be stale."""
        ref = f"{project}.{name}"
        cached = self._tables.get(ref)
        if cached is None or time.monotonic() - cached[1] > TABLE_TTL_SECONDS:
            table = self.catalog().load_table((project, name))
            self._tables[ref] = (table, time.monotonic())
            self._storage(table)
        table = self._tables[ref][0]
        if ref not in self.snapshots:
            current = table.current_snapshot()
            self.snapshots[ref] = current.snapshot_id if current else None
        return table, self.snapshots[ref]

    def _plan(self, read: Read, table: Table, snapshot: int) -> list:
        """The files ``read``'s filter can match (keys aside), planned once."""
        ref = f"{read.project}.{read.name}"
        key = (ref, read.where)
        if key not in self._plans:
            started = time.monotonic()
            scan = table.scan(
                row_filter=self._pushdown(read, keys=False), snapshot_id=snapshot
            )
            self._plans[key] = list(scan.plan_files())
            tasks = self._plans[key]
            log.info(
                "planned %s: %d files, %.1f MB, %d rows (%.1fs)", ref, len(tasks),
                sum(t.file.file_size_in_bytes for t in tasks) / 1e6,
                sum(t.file.record_count for t in tasks), time.monotonic() - started,
            )  # fmt: skip
        return self._plans[key]

    def _files(self, read: Read) -> tuple[Table, list | None]:
        """The table, and the files ``read`` can match (None: it has no snapshot):
        the plan's, narrowed to those whose ``key`` bounds can hold one of its keys."""
        table, snapshot = self.table(read.project, read.name)
        if snapshot is None:
            return table, None
        tasks = self._plan(read, table, snapshot)
        if read.keys is None:
            return table, tasks
        wanted = sorted({k.encode() for k in read.keys})
        field_id = table.schema().find_field(read.key).field_id
        return table, [t for t in tasks if _may_hold(t.file, field_id, wanted)]

    def rows(self, read: Read) -> int:
        """The rows the files ``read`` can match hold (from the catalog: none are read).
        An upper bound: files are counted whole."""
        _, tasks = self._files(read)
        return sum(task.file.record_count for task in tasks or [])

    def count(self, read: Read) -> int:
        """The rows ``read`` keeps: counted in its files (only the columns the filter
        and keys need are read), or their catalog row count when it has deletes."""
        table, tasks = self._files(read)
        if not tasks:
            return 0
        if any(task.delete_files for task in tasks):
            return sum(task.file.record_count for task in tasks)
        sql = f"SELECT count(*) FROM {self._source(tasks)}{self._where(read)}"
        return int(self._con.execute(sql).fetchone()[0])

    # -- reading ---------------------------------------------------------------------

    def read(self, read: Read) -> pa.Table:
        """The rows ``read`` keeps, as Arrow."""
        table, tasks = self._files(read)
        schema = table.schema().as_arrow()
        if tasks is None:
            return schema.empty_table()
        ref = f"{read.project}.{read.name}"
        if not tasks:
            self._tally(ref, tasks)
            return schema.empty_table()
        if any(task.delete_files for task in tasks):
            self._tally(ref, tasks)
            _, snapshot = self.table(read.project, read.name)
            scan = table.scan(row_filter=self._pushdown(read), snapshot_id=snapshot)
            return self._read_with_deletes(read, scan.to_arrow())
        self._tally(ref, tasks, self._groups_read(tasks, read))
        sql = f"SELECT {self._columns(schema)} FROM {self._source(tasks)}{self._where(read)}"
        return arrow_table(self._con, sql).cast(schema)

    @staticmethod
    def _source(tasks: list) -> str:
        files = ", ".join(lit(t.file.file_path) for t in tasks)
        return f"read_parquet([{files}], union_by_name = true)"

    def _groups_read(self, tasks: list, read: Read) -> list[tuple[str, int, int]]:
        """The row groups of ``tasks``' files a read of ``read``'s keys can't skip
        (by their key bounds, as DuckDB skips them): (file, row group, bytes)."""
        paths = [t.file.file_path for t in tasks]
        missing = [p for p in paths if p not in self._groups]
        if missing:
            files = ", ".join(lit(p) for p in missing)
            rows = self._con.execute(
                "SELECT file_name, row_group_id, any_value(stats_min_value) FILTER "
                f"(path_in_schema = {lit(read.key)}), any_value(stats_max_value) FILTER "
                f"(path_in_schema = {lit(read.key)}), sum(total_compressed_size) "
                f"FROM parquet_metadata([{files}]) GROUP BY ALL ORDER BY ALL"
            ).fetchall()
            # DuckDB names a local file without its file:// (tests).
            named = {p.removeprefix("file://"): p for p in missing}
            for path in missing:
                self._groups[path] = []
            for name, _, low, high, size in rows:
                path = named.get(name, name)
                self._groups.setdefault(path, []).append((low, high, int(size or 0)))
        wanted = sorted(set(read.keys)) if read.keys is not None else None
        out = []
        for path in paths:
            for i, (low, high, size) in enumerate(self._groups.get(path, [])):
                if wanted is None or _overlaps(wanted, low, high):
                    out.append((path, i, size))
        return out

    def _tally(
        self, ref: str, tasks: list, groups: list[tuple[str, int, int]] | None = None
    ) -> None:
        t = self._planned.setdefault(ref, {
            "reads": 0, "file_reads": 0, "file_bytes": 0, "paths": set(),
            "group_reads": 0, "groups": set(), "bytes": 0,
        })  # fmt: skip
        t["reads"] += 1
        t["file_reads"] += len(tasks)
        t["file_bytes"] += sum(task.file.file_size_in_bytes for task in tasks)
        t["paths"].update(task.file.file_path for task in tasks)
        if groups is None:  # not known (deletes): the files, whole
            t["bytes"] += sum(task.file.file_size_in_bytes for task in tasks)
            return
        t["group_reads"] += len(groups)
        t["groups"].update((path, i) for path, i, _ in groups)
        t["bytes"] += sum(size for _, _, size in groups)

    def read_stats(self) -> dict[str, dict[str, int]]:
        """Per table read: the reads; the distinct files and row groups they read,
        and how many file and row-group reads that took; the bytes of the files and
        of the row groups read. A read reads the row groups of its files that its
        keys can't rule out, so ``row_group_reads`` near ``row_groups`` means the
        shards' keys sit in their own row groups; near ``row_groups`` x ``reads``,
        every shard read every one. ``bytes_read`` is what they hold (compressed)."""
        return {
            ref: {"reads": t["reads"], "files": len(t["paths"]),
                  "file_reads": t["file_reads"], "row_groups": len(t["groups"]),
                  "row_group_reads": t["group_reads"], "file_bytes": t["file_bytes"],
                  "bytes_read": t["bytes"]}
            for ref, t in self._planned.items()
        }  # fmt: skip

    def _pushdown(self, read: Read, keys: bool = True) -> ice.BooleanExpression:
        parts: list[ice.BooleanExpression] = []
        if keys and read.keys is not None:
            keys = sorted(set(read.keys))
            parts.append(ice.In(read.key, keys) if keys else ice.AlwaysFalse())
        if read.where:
            try:
                pushed = compile_filter(read.where, read.columns).pushdown
            except DataMismatch:
                pushed = None  # the engine applies the input filter (and says why not)
            if pushed is not None:
                parts.append(pushed)
        out: ice.BooleanExpression = ice.AlwaysTrue()
        for part in parts:
            out = ice.And(out, part)
        return out

    def _where(self, read: Read) -> str:
        conditions = []
        if read.keys is not None:
            conditions.append(f"{q(read.key)} {in_list(sorted(set(read.keys)))}")
        if read.where:
            conditions.append(f"({to_duckdb(read.where)})")
        return f" WHERE {' AND '.join(conditions)}" if conditions else ""

    @staticmethod
    def _columns(schema: pa.Schema) -> str:
        return ", ".join(q(name) for name in schema.names)

    def _read_with_deletes(self, read: Read, rows: pa.Table) -> pa.Table:
        """Rows PyIceberg read (deletes applied, pushdown only): the rest of the
        filter, and the keys, in DuckDB."""
        self._con.register("__rows", rows)
        try:
            sql = f"SELECT * FROM __rows{self._where(read)}"
            return arrow_table(self._con, sql).cast(rows.schema)
        finally:
            self._con.unregister("__rows")

    def _storage(self, table: Table) -> None:
        """A DuckDB S3 secret for ``table``'s files, from the credentials Polaris
        vended for it (scoped to its location, so tables don't share one)."""
        p = table.io.properties
        options = [
            "TYPE s3",
            f"REGION {lit(p.get('s3.region') or p.get('client.region') or self.access.region)}",
            f"SCOPE {lit(table.metadata.location)}",
        ]
        if p.get("s3.access-key-id"):
            options += [
                f"KEY_ID {lit(p['s3.access-key-id'])}",
                f"SECRET {lit(p['s3.secret-access-key'])}",
            ]
            if p.get("s3.session-token"):
                options.append(f"SESSION_TOKEN {lit(p['s3.session-token'])}")
        endpoint = p.get("s3.endpoint") or self.access.endpoint
        if endpoint:
            host = endpoint.split("://", 1)[-1].rstrip("/")
            options += [
                f"ENDPOINT {lit(host)}",
                f"USE_SSL {'true' if endpoint.startswith('https') else 'false'}",
            ]
        if self.access.path_style or p.get("s3.path-style-access") == "true":
            options.append("URL_STYLE 'path'")
        name = (
            "table_" + hashlib.sha256(table.metadata.location.encode()).hexdigest()[:16]
        )
        self._con.execute(f"CREATE OR REPLACE SECRET {name} ({', '.join(options)})")

    def close(self) -> None:
        self._con.close()
