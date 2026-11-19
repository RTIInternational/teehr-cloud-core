"""Iceberg table I/O as the caller (Phase 2).

Every call uses the caller's own Keycloak token against Polaris, so Polaris
enforces the caller's project roles and vends storage credentials scoped to the
table. The API never reads or writes project data with its own identity.

PyIceberg does the table work (catalog calls, Parquet I/O, commits); there is no
query engine here. Routes depend on :func:`get_tables`, which tests replace.
"""

import warnings
from collections.abc import Iterable
from dataclasses import dataclass
from functools import lru_cache
from typing import Literal, Protocol

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import (
    CommitFailedException,
    ForbiddenError,
    NoSuchNamespaceError,
    NoSuchTableError,
    TableAlreadyExistsError,
    UnauthorizedError,
)
from pyiceberg.expressions import (
    AlwaysTrue,
    And,
    BooleanExpression,
    GreaterThanOrEqual,
    In,
    LessThanOrEqual,
)
from pyiceberg.partitioning import (
    UNPARTITIONED_PARTITION_SPEC,
    PartitionField,
    PartitionSpec,
)
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER, SortField, SortOrder
from pyiceberg.table.upsert_util import create_match_filter
from pyiceberg.transforms import IdentityTransform, MonthTransform, YearTransform
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IcebergType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
    TimestamptzType,
)

from src.config import Settings, get_settings

# The column types a dataset may declare, by API name.
COLUMN_TYPES: dict[str, IcebergType] = {
    "string": StringType(),
    "int32": IntegerType(),
    "int64": LongType(),
    "float32": FloatType(),
    "float64": DoubleType(),
    "boolean": BooleanType(),
    "date": DateType(),
    "timestamp": TimestampType(),  # no time zone
    "timestamptz": TimestamptzType(),  # UTC
    "binary": BinaryType(),
    "decimal(5,4)": DecimalType(5, 4),  # quantiles (4 decimal places)
}


class TableError(Exception):
    """A table operation failed."""


class TableForbidden(TableError):
    """Polaris refused the caller (no role, or a token from before they got one)."""


class NamespaceMissing(TableError):
    """The project's namespace doesn't exist (not provisioned)."""


class TableExists(TableError):
    """A table of that name already exists in the namespace."""


class TableMissing(TableError):
    """The dataset's table isn't in the catalog."""


class WriteConflict(TableError):
    """Another write committed first; retrying is safe."""


class DataMismatch(ValueError):
    """Submitted data (or a filter) doesn't fit the dataset's declared columns."""


# Above this many keys, existing_keys reads the whole column instead.
_IN_FILTER_MAX = 5000

WriteMode = Literal["append", "overwrite", "upsert"]
PartitionTransform = Literal["month", "year"]


#: How every new table's files are laid out, unless its layout says otherwise:
#: files of about 64 MB, in row groups of a few series each, so a read of a range of
#: series keys skips most of a file (shards are key ranges; VeriRace measured 150k-row
#: row groups). Spark (sorted rewrites) sizes row groups in encoded bytes, PyIceberg
#: (the API's and jobs' writes) in rows. Encoded series data is small: 8 MB made
#: ~6M-row groups of nwm30_medium_range, so a 1.7M-row shard read ~6 of them; 1 MB
#: makes them under 1M rows. Starting points, not tuned.
LAYOUT_DEFAULTS = {
    "write.target-file-size-bytes": str(64 * 1024**2),
    "write.parquet.row-group-size-bytes": str(1024**2),
    "write.parquet.row-group-limit": "150000",
}
# PyIceberg warns that it doesn't size row groups in bytes; the row limit applies.
warnings.filterwarnings(
    "ignore", message=r".*write\.parquet\.row-group-size-bytes.*not implemented"
)


def table_properties(layout: "Layout") -> dict[str, str]:
    """A new table's properties: the layout defaults, then the layout's own."""
    return {**LAYOUT_DEFAULTS, **(layout.properties or {})}


@dataclass(frozen=True)
class Layout:
    """How a new table is laid out: partitioned by a time transform of one
    column, sorted by others, with table properties (metadata mirrored from the
    registry)."""

    partition: tuple[str, PartitionTransform] | None = None
    sort: tuple[str, ...] = ()
    properties: dict[str, str] | None = None


@dataclass(frozen=True)
class WriteResult:
    snapshot_id: int | None


class SnapshotGone(TableError):
    """A page cursor's snapshot no longer exists (expired); start the read over."""


@dataclass(frozen=True)
class ReadResult:
    """Rows from one snapshot, and which snapshot (None: the table is empty)."""

    rows: pa.Table
    snapshot_id: int | None


def iceberg_schema(
    columns: Iterable[dict], key_columns: Iterable[str] | None = None
) -> Schema:
    """The Iceberg schema for declared columns ({name, type, required, doc}), with
    the key columns as its identifier fields."""
    fields = [
        NestedField(
            field_id=i,
            name=col["name"],
            field_type=COLUMN_TYPES[col["type"]],
            required=bool(col.get("required")),
            doc=col.get("doc"),
        )
        for i, col in enumerate(columns, start=1)
    ]
    ids = {f.name: f.field_id for f in fields}
    return Schema(
        *fields, identifier_field_ids=[ids[name] for name in key_columns or ()]
    )


def keep_unprovided(
    data: pa.Table, current: pa.Table, key_columns: list[str], provided: list[str]
) -> pa.Table:
    """``data`` with the columns it didn't provide taken from ``current`` (the
    matching stored rows); null for new rows. Same schema and row order as
    ``data``."""
    missing = [c for c in data.column_names if c not in provided]
    if not missing:
        return data
    order = "__row"
    left = data.select([*key_columns, *[c for c in provided if c not in key_columns]])
    left = left.append_column(order, pa.array(range(data.num_rows), pa.int64()))
    right = current.select([*key_columns, *missing]).cast(
        pa.schema([data.schema.field(c) for c in [*key_columns, *missing]])
    )
    joined = left.join(right, keys=key_columns, join_type="left outer")
    joined = joined.sort_by(order)
    return pa.Table.from_arrays(
        [joined.column(c) for c in data.column_names], schema=data.schema
    )


def key_range(data: pa.Table, key_columns: list[str]) -> BooleanExpression:
    """A filter keeping every stored row that could share a key with ``data``: the
    first text key column's values, within each other key column's range (times,
    dates, numbers). Small however many rows ``data`` has (PyIceberg's upsert ORs a
    clause per row)."""
    parts: list[BooleanExpression] = []
    listed = False
    for name in key_columns:
        column = data[name]
        kind = column.type
        if pa.types.is_timestamp(kind):  # as microseconds, which Iceberg takes
            column = pc.cast(column.cast(pa.timestamp("us", kind.tz)), pa.int64())
        elif pa.types.is_date32(kind):  # as days
            column = pc.cast(column, pa.int32())
        if pa.types.is_integer(column.type) or pa.types.is_floating(column.type):
            bounds = pc.min_max(column)
            parts += [GreaterThanOrEqual(name, bounds["min"].as_py()),
                      LessThanOrEqual(name, bounds["max"].as_py())]  # fmt: skip
        elif not listed and (
            pa.types.is_string(column.type) or pa.types.is_large_string(column.type)
        ):
            parts.append(In(name, pc.unique(column).to_pylist()))
            listed = True
    out: BooleanExpression = AlwaysTrue()
    for part in parts:
        out = And(out, part)
    return out


def _upsert(table, data: pa.Table) -> None:
    """Upsert ``data`` on the table's identifier fields, in one commit: the stored
    rows in its :func:`key_range` that it doesn't replace are kept, and that range
    is overwritten with them and ``data``."""
    schema = table.schema()
    ids = schema.identifier_field_ids
    key = [f.name for f in schema.fields if f.field_id in ids]
    if not key:
        raise TableError("upsert needs a table with key columns")
    where = key_range(data, key)
    stored = table.scan(row_filter=where).to_arrow()
    if stored.num_rows == 0:
        table.overwrite(data, overwrite_filter=where)  # nothing to keep
        return
    stored = stored.select(data.column_names).cast(data.schema)
    kept = stored.join(data.select(key), keys=key, join_type="left anti")
    rows = pa.concat_tables([data, kept.select(data.column_names).cast(data.schema)])
    table.overwrite(rows, overwrite_filter=where)


def _partition_spec(
    schema: Schema, partition: tuple[str, PartitionTransform] | None
) -> PartitionSpec:
    if partition is None:
        return UNPARTITIONED_PARTITION_SPEC
    column, transform = partition
    field = schema.find_field(column)
    return PartitionSpec(
        PartitionField(
            source_id=field.field_id,
            field_id=1000,
            transform=MonthTransform() if transform == "month" else YearTransform(),
            name=f"{column}_{transform}",
        )
    )


def _sort_order(schema: Schema, columns: tuple[str, ...]) -> SortOrder:
    if not columns:
        return UNSORTED_SORT_ORDER
    return SortOrder(
        *(
            SortField(
                source_id=schema.find_field(c).field_id, transform=IdentityTransform()
            )
            for c in columns
        )
    )


def duplicate_keys(data: pa.Table, key_columns: list[str]) -> int:
    """How many key values occur more than once in ``data``."""
    if not key_columns or data.num_rows == 0:
        return 0
    counts = data.group_by(key_columns).aggregate([([], "count_all")])
    return pc.sum(pc.greater(counts.column("count_all"), 1)).as_py() or 0


_HAS_OFFSET = r"(?:[Zz]|[+-]\d{2}(?::?\d{2})?)$"


def _localize(
    column: pa.ChunkedArray, field: pa.Field, timezone: str
) -> pa.ChunkedArray:
    """Read zone-less times in ``timezone`` for a timestamptz column, then UTC.

    Values that carry an offset keep it. A local time that doesn't exist or occurs
    twice (daylight-saving changes) is refused rather than guessed.
    """
    naive_type = pa.timestamp(field.type.unit)
    if pa.types.is_timestamp(column.type):
        naive = column.cast(naive_type, safe=True)
        with_offset = None
    else:  # text
        has_offset = pc.match_substring_regex(column, _HAS_OFFSET)
        nulls = pa.nulls(len(column), column.type)
        with_offset = pc.if_else(has_offset, column, nulls).cast(field.type, safe=True)
        naive = pc.if_else(has_offset, nulls, column).cast(naive_type, safe=True)
    try:
        local = pc.assume_timezone(
            naive, timezone, ambiguous="raise", nonexistent="raise"
        ).cast(field.type)
    except pa.ArrowInvalid as exc:
        raise DataMismatch(
            f"column '{field.name}': a time doesn't exist or is ambiguous in "
            f"{timezone} (daylight-saving change); send it with an offset ({exc})"
        ) from exc
    return local if with_offset is None else pc.coalesce(with_offset, local)


def conform(
    data: pa.Table, columns: list[dict], timezone: str | None = None
) -> pa.Table:
    """Fit submitted data to the dataset's declared columns, or say why not.

    Column order doesn't matter, and a missing optional column becomes nulls.
    Types convert only where nothing is lost (e.g. int32 -> int64, a time zone ->
    UTC); a timestamp's time-zone-ness must match its column, unless ``timezone``
    says what zone-less times in a timestamptz column are in.
    """
    target = iceberg_schema(columns).as_arrow()
    declared = {f.name for f in target}
    extra = sorted(set(data.column_names) - declared)
    if extra:
        raise DataMismatch(f"columns not in the dataset: {', '.join(extra)}")
    if len(set(data.column_names)) != len(data.column_names):
        raise DataMismatch("duplicate column names")
    arrays = []
    for field in target:
        if field.name not in data.column_names:
            if not field.nullable:
                raise DataMismatch(f"missing required column '{field.name}'")
            arrays.append(pa.nulls(data.num_rows, type=field.type))
            continue
        column = data.column(field.name)
        if not field.nullable and column.null_count:
            raise DataMismatch(
                f"column '{field.name}' is required but has {column.null_count} nulls"
            )
        source = column.type
        is_text = pa.types.is_string(source) or pa.types.is_large_string(source)
        zoned_target = pa.types.is_timestamp(field.type) and field.type.tz
        naive_source = pa.types.is_timestamp(source) and source.tz is None
        if timezone and zoned_target and (naive_source or is_text):
            try:
                arrays.append(_localize(column, field, timezone))
            except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as exc:
                raise DataMismatch(
                    f"column '{field.name}': can't read times as {field.type} ({exc})"
                ) from exc
            continue
        if pa.types.is_timestamp(source) and pa.types.is_timestamp(field.type):
            if (source.tz is None) != (field.type.tz is None):
                kind = "time-zone-aware" if field.type.tz else "time-zone-naive"
                hint = " (or pass timezone=...)" if field.type.tz else ""
                raise DataMismatch(f"column '{field.name}' must be {kind}{hint}")
        try:
            converted = column.cast(field.type, safe=True)
            if pa.types.is_decimal(field.type) and pa.types.is_floating(source):
                # Arrow rounds floats to the scale; refuse rather than round.
                back = converted.cast(pa.float64())
                if not pc.all(
                    pc.or_kleene(
                        pc.equal(back, column.cast(pa.float64())), pc.is_null(column)
                    )
                ).as_py():
                    raise DataMismatch(
                        f"column '{field.name}': values must have at most "
                        f"{field.type.scale} decimal places"
                    )
            arrays.append(converted)
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as exc:
            if pa.types.is_decimal(field.type) and "Rescaling" in str(exc):
                raise DataMismatch(
                    f"column '{field.name}': values must have at most "
                    f"{field.type.scale} decimal places"
                ) from exc
            if is_text and pa.types.is_timestamp(field.type):
                raise DataMismatch(
                    f"column '{field.name}': can't parse a value as {field.type} "
                    "(timestamps with a time zone need an offset such as Z, or "
                    f"timezone=...; without one must have none) ({exc})"
                ) from exc
            if is_text:
                raise DataMismatch(
                    f"column '{field.name}': can't parse a value as {field.type} "
                    f"({exc})"
                ) from exc
            raise DataMismatch(
                f"column '{field.name}': can't convert {source} to {field.type} "
                f"without losing values ({exc})"
            ) from exc
    return pa.Table.from_arrays(arrays, schema=target)


class Tables(Protocol):
    def create_table(
        self,
        token: str,
        namespace: str,
        name: str,
        columns: list[dict],
        key_columns: list[str] | None = None,
        layout: Layout | None = None,
    ) -> str:
        """Create the table as the caller; returns its storage location."""

    def read(
        self,
        token: str,
        namespace: str,
        name: str,
        *,
        columns: tuple[str, ...] | None,
        row_filter: BooleanExpression | None,
        snapshot_id: int | None,
        limit: int,
    ) -> ReadResult:
        """Scan as the caller: up to ``limit`` rows, from ``snapshot_id`` (or the
        current snapshot), in the scan's order (fixed for a given snapshot)."""

    def count(
        self,
        token: str,
        namespace: str,
        name: str,
        row_filter: BooleanExpression | None,
        snapshot_id: int | None,
    ) -> int:
        """Rows matching ``row_filter`` in the snapshot, as the caller."""

    def insert_missing(
        self, token: str, namespace: str, name: str, data: pa.Table
    ) -> int:
        """Insert the rows whose key isn't in the table yet, leaving existing rows
        alone, as the caller. Returns how many were inserted."""

    def merge(
        self,
        token: str,
        namespace: str,
        name: str,
        data: pa.Table,
        key_columns: list[str],
        provided: list[str],
    ) -> WriteResult:
        """Upsert where only the ``provided`` columns change: a matched row keeps
        its current values in the others, as the caller."""

    def existing_keys(
        self, token: str, namespace: str, name: str, column: str, values: list[str]
    ) -> set[str]:
        """Which of ``values`` are in the table's ``column``, as the caller."""

    def drop_table(self, token: str, namespace: str, name: str) -> None:
        """Drop the table and its files, as the caller. A missing table is fine."""

    def write(
        self,
        token: str,
        namespace: str,
        name: str,
        data: pa.Table,
        mode: WriteMode,
        overwrite_filter: BooleanExpression | None = None,
    ) -> WriteResult:
        """Commit conformed data as the caller (one snapshot)."""

    def has_snapshot(
        self, token: str, namespace: str, name: str, snapshot_id: int
    ) -> bool:
        """Whether ``snapshot_id`` is one of the table's snapshots, as the caller."""

    def write_file(
        self, token: str, namespace: str, name: str, data: pa.Table, file_name: str
    ) -> str:
        """Write conformed rows as a Parquet file in the table's data location, not
        yet part of the table (ADR-0017: a shard's output); returns its path."""

    def commit_files(
        self,
        token: str,
        namespace: str,
        name: str,
        paths: list[str],
        overwrite_filter: BooleanExpression | None = None,
    ) -> WriteResult:
        """Replace the rows ``overwrite_filter`` keeps (all, when None) with the
        rows of files :meth:`write_file` wrote, in one commit, as the caller."""

    def delete_files(self, token: str, namespace: str, name: str, paths: list[str]):
        """Delete files :meth:`write_file` wrote that won't be committed."""


def catalog_properties(
    uri: str,
    warehouse: str,
    realm: str,
    region: str = "us-east-1",
    endpoint: str | None = None,
    path_style: bool = False,
) -> dict[str, str]:
    """PyIceberg properties for Polaris's REST catalog (a token is added per call)."""
    properties = {
        "type": "rest",
        "uri": uri,
        "warehouse": warehouse,
        "header.X-Polaris-Realm": realm,
        # Storage credentials scoped to each table, from Polaris.
        "header.X-Iceberg-Access-Delegation": "vended-credentials",
        "s3.region": region,
    }
    if endpoint:
        properties["s3.endpoint"] = endpoint
    if path_style:
        properties["s3.path-style-access"] = "true"
    return properties


class PolarisTables:
    """:class:`Tables` against Polaris's Iceberg REST catalog."""

    def __init__(self, settings: Settings):
        self._properties = catalog_properties(
            f"{settings.polaris_url.rstrip('/')}/api/catalog",
            settings.polaris_catalog,
            settings.polaris_realm,
            settings.storage_region,
            settings.storage_endpoint,
            settings.storage_path_style,
        )

    @classmethod
    def at(cls, properties: dict[str, str]) -> "PolarisTables":
        """Tables at a catalog given by its properties (a job's, ADR-0016)."""
        tables = cls.__new__(cls)
        tables._properties = dict(properties)
        return tables

    def _catalog(self, token: str) -> Catalog:
        return load_catalog("polaris", **self._properties, token=token)

    def create_table(
        self,
        token: str,
        namespace: str,
        name: str,
        columns: list[dict],
        key_columns: list[str] | None = None,
        layout: Layout | None = None,
    ) -> str:
        schema = iceberg_schema(columns, key_columns)
        layout = layout or Layout()
        try:
            table = self._catalog(token).create_table(
                (namespace, name),
                schema=schema,
                partition_spec=_partition_spec(schema, layout.partition),
                sort_order=_sort_order(schema, layout.sort),
                properties=table_properties(layout),
            )
        except (ForbiddenError, UnauthorizedError) as exc:
            raise TableForbidden(str(exc)) from exc
        except NoSuchNamespaceError as exc:
            raise NamespaceMissing(str(exc)) from exc
        except TableAlreadyExistsError as exc:
            raise TableExists(str(exc)) from exc
        except Exception as exc:  # network, server errors
            raise TableError(str(exc)) from exc
        return table.metadata.location

    def write(
        self,
        token: str,
        namespace: str,
        name: str,
        data: pa.Table,
        mode: WriteMode,
        overwrite_filter: BooleanExpression | None = None,
    ) -> WriteResult:
        try:
            table = self._catalog(token).load_table((namespace, name))
            if mode == "append":
                table.append(data)
            elif mode == "upsert":  # on the table's identifier fields
                _upsert(table, data)
            else:
                table.overwrite(data, overwrite_filter=overwrite_filter or AlwaysTrue())
            snapshot = table.current_snapshot()
        except NoSuchTableError as exc:
            raise TableMissing(str(exc)) from exc
        except (ForbiddenError, UnauthorizedError) as exc:
            raise TableForbidden(str(exc)) from exc
        except CommitFailedException as exc:
            raise WriteConflict(str(exc)) from exc
        except Exception as exc:
            raise TableError(str(exc)) from exc
        return WriteResult(snapshot.snapshot_id if snapshot else None)

    def insert_missing(
        self, token: str, namespace: str, name: str, data: pa.Table
    ) -> int:
        try:
            table = self._catalog(token).load_table((namespace, name))
            result = table.upsert(data, when_matched_update_all=False)
        except NoSuchTableError as exc:
            raise TableMissing(str(exc)) from exc
        except (ForbiddenError, UnauthorizedError) as exc:
            raise TableForbidden(str(exc)) from exc
        except CommitFailedException as exc:
            raise WriteConflict(str(exc)) from exc
        except Exception as exc:
            raise TableError(str(exc)) from exc
        return result.rows_inserted

    def merge(
        self,
        token: str,
        namespace: str,
        name: str,
        data: pa.Table,
        key_columns: list[str],
        provided: list[str],
    ) -> WriteResult:
        def run() -> WriteResult:
            table = self._catalog(token).load_table((namespace, name))
            current = table.scan(
                row_filter=create_match_filter(data, key_columns)
            ).to_arrow()
            table.upsert(keep_unprovided(data, current, key_columns, provided))
            snapshot = table.current_snapshot()
            return WriteResult(snapshot.snapshot_id if snapshot else None)

        return self._guard(run)

    def _scan(self, token, namespace, name, columns, row_filter, snapshot_id, limit):
        table = self._catalog(token).load_table((namespace, name))
        if snapshot_id is None:
            current = table.current_snapshot()
            snapshot_id = current.snapshot_id if current else None
        elif table.snapshot_by_id(snapshot_id) is None:
            raise SnapshotGone(f"snapshot {snapshot_id} no longer exists")
        if snapshot_id is None:  # no data yet
            return table, None, None
        scan = table.scan(
            row_filter=row_filter or AlwaysTrue(),
            selected_fields=columns or ("*",),
            snapshot_id=snapshot_id,
            limit=limit,
        )
        return table, snapshot_id, scan

    def _guard(self, call):
        try:
            return call()
        except TableError:
            raise
        except NoSuchTableError as exc:
            raise TableMissing(str(exc)) from exc
        except (ForbiddenError, UnauthorizedError) as exc:
            raise TableForbidden(str(exc)) from exc
        except CommitFailedException as exc:
            raise WriteConflict(str(exc)) from exc
        except Exception as exc:
            raise TableError(str(exc)) from exc

    def read(
        self,
        token: str,
        namespace: str,
        name: str,
        *,
        columns: tuple[str, ...] | None,
        row_filter: BooleanExpression | None,
        snapshot_id: int | None,
        limit: int,
    ) -> ReadResult:
        def run() -> ReadResult:
            table, snap, scan = self._scan(
                token, namespace, name, columns, row_filter, snapshot_id, limit
            )
            if scan is None:
                schema = table.schema().as_arrow()
                if columns:
                    schema = pa.schema([schema.field(c) for c in columns])
                return ReadResult(schema.empty_table(), None)
            return ReadResult(scan.to_arrow(), snap)

        return self._guard(run)

    def count(
        self,
        token: str,
        namespace: str,
        name: str,
        row_filter: BooleanExpression | None,
        snapshot_id: int | None,
    ) -> int:
        def run() -> int:
            _, _, scan = self._scan(
                token, namespace, name, None, row_filter, snapshot_id, None
            )
            return 0 if scan is None else scan.count()

        return self._guard(run)

    def existing_keys(
        self, token: str, namespace: str, name: str, column: str, values: list[str]
    ) -> set[str]:
        def run() -> set[str]:
            table = self._catalog(token).load_table((namespace, name))
            if table.current_snapshot() is None:
                return set()
            wanted = set(values)
            # A few keys: filter the scan by them. Many: read the column once.
            row_filter = (
                In(column, wanted) if len(wanted) <= _IN_FILTER_MAX else AlwaysTrue()
            )
            found = table.scan(row_filter=row_filter, selected_fields=(column,))
            present = set(pc.unique(found.to_arrow()[column]).to_pylist())
            return present & wanted

        return self._guard(run)

    def has_snapshot(
        self, token: str, namespace: str, name: str, snapshot_id: int
    ) -> bool:
        def run() -> bool:
            table = self._catalog(token).load_table((namespace, name))
            return table.snapshot_by_id(snapshot_id) is not None

        return self._guard(run)

    def write_file(
        self, token: str, namespace: str, name: str, data: pa.Table, file_name: str
    ) -> str:
        import pyarrow.parquet as pq

        def run() -> str:
            table = self._catalog(token).load_table((namespace, name))
            path = f"{table.metadata.location.rstrip('/')}/data/{file_name}"
            rows = int(
                table.properties.get(
                    "write.parquet.row-group-limit",
                    LAYOUT_DEFAULTS["write.parquet.row-group-limit"],
                )
            )
            with table.io.new_output(path).create(overwrite=True) as out:
                pq.write_table(data, out, row_group_size=rows, compression="zstd")
            return path

        return self._guard(run)

    def commit_files(
        self,
        token: str,
        namespace: str,
        name: str,
        paths: list[str],
        overwrite_filter: BooleanExpression | None = None,
    ) -> WriteResult:
        def run() -> WriteResult:
            table = self._catalog(token).load_table((namespace, name))
            with table.transaction() as tx:
                tx.delete(overwrite_filter or AlwaysTrue())
                if paths:
                    tx.add_files(paths)
            snapshot = table.current_snapshot()
            return WriteResult(snapshot.snapshot_id if snapshot else None)

        return self._guard(run)

    def delete_files(self, token: str, namespace: str, name: str, paths: list[str]):
        def run() -> None:
            table = self._catalog(token).load_table((namespace, name))
            for path in paths:
                table.io.delete(path)

        return self._guard(run)

    def drop_table(self, token: str, namespace: str, name: str) -> None:
        try:
            self._catalog(token).purge_table((namespace, name))
        except NoSuchTableError:
            return
        except (ForbiddenError, UnauthorizedError) as exc:
            raise TableForbidden(str(exc)) from exc
        except Exception as exc:
            raise TableError(str(exc)) from exc


@lru_cache
def get_tables() -> Tables | None:
    """Table I/O as the caller, or None when Polaris isn't configured."""
    settings = get_settings()
    if not settings.polaris_url:
        return None
    return PolarisTables(settings)
