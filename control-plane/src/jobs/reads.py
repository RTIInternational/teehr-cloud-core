"""Reading a run's inputs from the warehouse, in the job's pod, as the caller (#223).

Run jobs used to read their inputs through ``/data``, which materializes every page
in the control plane and re-reads from the start for each one: a day of a big
forecast took the control plane down. Here the job reads the tables itself:

- **PyIceberg plans.** It loads each table from Polaris as the caller (ADR-0004), with
  storage credentials Polaris vends for that table, and lists the data files a read
  can match at the snapshot the run is pinned to: partitions and files whose column
  statistics rule the filter out are skipped.
- **DuckDB reads** those Parquet files, applying the filter and the key list as it
  goes, so only the rows a read keeps reach Arrow.

A table with delete files (merge-on-read) is read by PyIceberg instead, which applies
them; the control plane's own writes don't make any.
"""

from __future__ import annotations

import hashlib
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
    snapshots: dict[str, int | None] = field(default_factory=dict)
    # Per table: the reads, the files they planned (each, and distinct) and the bytes.
    _planned: dict[str, dict[str, Any]] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        self._con = memory_connection()
        limit = self.memory_limit or default_memory_limit(RUN_SHARE)
        self._con.execute(f"SET TimeZone = 'UTC'; SET memory_limit = '{limit}'")
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
        """The table (loaded now: its vended credentials are fresh) and its pinned
        snapshot (its current one, the first time it's read)."""
        table = self.catalog().load_table((project, name))
        ref = f"{project}.{name}"
        if ref not in self.snapshots:
            current = table.current_snapshot()
            self.snapshots[ref] = current.snapshot_id if current else None
        return table, self.snapshots[ref]

    def rows(self, read: Read) -> int:
        """The rows the files ``read`` can match hold (from the catalog: none are read).
        An upper bound: files are counted whole."""
        table, snapshot = self.table(read.project, read.name)
        if snapshot is None:
            return 0
        scan = table.scan(row_filter=self._pushdown(read), snapshot_id=snapshot)
        return sum(task.file.record_count for task in scan.plan_files())

    # -- reading ---------------------------------------------------------------------

    def read(self, read: Read) -> pa.Table:
        """The rows ``read`` keeps, as Arrow."""
        table, snapshot = self.table(read.project, read.name)
        schema = table.schema().as_arrow()
        if snapshot is None:
            return schema.empty_table()
        scan = table.scan(row_filter=self._pushdown(read), snapshot_id=snapshot)
        tasks = list(scan.plan_files())
        self._tally(f"{read.project}.{read.name}", tasks)
        if not tasks:
            return schema.empty_table()
        if any(task.delete_files for task in tasks):
            return self._read_with_deletes(read, scan.to_arrow())
        self._storage(table)
        files = ", ".join(lit(t.file.file_path) for t in tasks)
        sql = (
            f"SELECT {self._columns(schema)} FROM read_parquet([{files}], "
            f"union_by_name = true){self._where(read)}"
        )
        return arrow_table(self._con, sql).cast(schema)

    def _tally(self, ref: str, tasks: list) -> None:
        t = self._planned.setdefault(ref, {"reads": 0, "file_reads": 0, "bytes": 0,
                                           "paths": set()})  # fmt: skip
        t["reads"] += 1
        t["file_reads"] += len(tasks)
        t["bytes"] += sum(task.file.file_size_in_bytes for task in tasks)
        t["paths"].update(task.file.file_path for task in tasks)

    def read_stats(self) -> dict[str, dict[str, int]]:
        """Per table read: the reads, the distinct files they read, how many file
        reads that took, and the bytes. ``file_reads`` near ``files`` means the
        shards' keys sit in their own files (the table is sorted by them); near
        ``files`` x ``reads``, every shard read every file."""
        return {
            ref: {"reads": t["reads"], "files": len(t["paths"]),
                  "file_reads": t["file_reads"], "bytes_read": t["bytes"]}
            for ref, t in self._planned.items()
        }  # fmt: skip

    def _pushdown(self, read: Read) -> ice.BooleanExpression:
        parts: list[ice.BooleanExpression] = []
        if read.keys is not None:
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
