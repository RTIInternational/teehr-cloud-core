"""Iceberg table I/O as the caller (Phase 2).

Every call uses the caller's own Keycloak token against Polaris, so Polaris
enforces the caller's project roles and vends storage credentials scoped to the
table. The API never reads or writes project data with its own identity.

PyIceberg does the table work (catalog calls, Parquet I/O, commits); there is no
query engine here. Routes depend on :func:`get_tables`, which tests replace.
"""

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
from pyiceberg.expressions import AlwaysTrue, BooleanExpression
from pyiceberg.expressions.parser import parse
from pyiceberg.expressions.visitors import bind
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
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


WriteMode = Literal["append", "overwrite"]


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
            arrays.append(column.cast(field.type, safe=True))
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as exc:
            if is_text:
                raise DataMismatch(
                    f"column '{field.name}': can't parse a value as {field.type} "
                    "(timestamps with a time zone need an offset such as Z, or "
                    f"timezone=...; without one must have none) ({exc})"
                ) from exc
            raise DataMismatch(
                f"column '{field.name}': can't convert {source} to {field.type} "
                f"without losing values ({exc})"
            ) from exc
    return pa.Table.from_arrays(arrays, schema=target)


def parse_filter(text: str, columns: list[dict]) -> BooleanExpression:
    """A row filter in PyIceberg's expression syntax, checked against the columns,
    e.g. ``location_id = 'usgs-01' and value_time >= '2026-01-01T00:00:00+00:00'``."""
    try:
        expression = parse(text)
        bind(iceberg_schema(columns), expression, case_sensitive=True)
    except Exception as exc:
        raise DataMismatch(f"invalid filter: {exc}") from exc
    return expression


class Tables(Protocol):
    def create_table(
        self,
        token: str,
        namespace: str,
        name: str,
        columns: list[dict],
        key_columns: list[str] | None = None,
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


class PolarisTables:
    """:class:`Tables` against Polaris's Iceberg REST catalog."""

    def __init__(self, settings: Settings):
        self._properties = {
            "type": "rest",
            "uri": f"{settings.polaris_url.rstrip('/')}/api/catalog",
            "warehouse": settings.polaris_catalog,
            "header.X-Polaris-Realm": settings.polaris_realm,
            # Storage credentials scoped to each table, from Polaris.
            "header.X-Iceberg-Access-Delegation": "vended-credentials",
            "s3.region": settings.storage_region,
        }
        if settings.storage_endpoint:
            self._properties["s3.endpoint"] = settings.storage_endpoint
        if settings.storage_path_style:
            self._properties["s3.path-style-access"] = "true"

    def _catalog(self, token: str) -> Catalog:
        return load_catalog("polaris", **self._properties, token=token)

    def create_table(
        self,
        token: str,
        namespace: str,
        name: str,
        columns: list[dict],
        key_columns: list[str] | None = None,
    ) -> str:
        try:
            table = self._catalog(token).create_table(
                (namespace, name), schema=iceberg_schema(columns, key_columns)
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
