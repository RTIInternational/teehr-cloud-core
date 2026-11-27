"""Iceberg table I/O as the caller (Phase 2).

Every call uses the caller's own Keycloak token against Polaris, so Polaris
enforces the caller's project roles and vends storage credentials scoped to the
table. The API never reads or writes project data with its own identity.

PyIceberg does the table work (catalog calls, Parquet I/O, commits); there is no
query engine here. Routes depend on :func:`get_tables`, which tests replace.
"""

from collections.abc import Callable
from dataclasses import dataclass
from functools import lru_cache
from typing import Literal, Protocol, TypeVar

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.catalog.rest.auth import AuthManager
from pyiceberg.exceptions import (
    CommitFailedException,
    ForbiddenError,
    NoSuchNamespaceError,
    NoSuchTableError,
    TableAlreadyExistsError,
    UnauthorizedError,
)
from pyiceberg.expressions import AlwaysTrue, BooleanExpression, In
from teehr_core import warehouse as wh

# The warehouse rules (layout, conforming rows, writes) are teehr-core's, shared with
# platform producers (ADR-0019); re-exported here for the routes and jobs.
from teehr_core.warehouse import (  # noqa: F401
    COLUMN_TYPES,
    LAYOUT_DEFAULTS,
    DataMismatch,
    Layout,
    conform,
    duplicate_keys,
    iceberg_schema,
    keep_unprovided,
    key_range,
    table_properties,
)

from src.config import Settings, get_settings

T = TypeVar("T")


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


# Above this many keys, existing_keys reads the whole column instead.
_IN_FILTER_MAX = 5000

WriteMode = Literal["append", "overwrite", "upsert"]


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


#: The caller's access token, or a function that gives a current one. A function is asked on
#: every catalog request, so a long write's commit doesn't go out with a token that expired
#: while its files were written (Keycloak's last minutes; #228).
Token = str | Callable[[], str]


class TokenAuth(AuthManager):
    """Catalog auth that asks ``token`` for the bearer token on every request."""

    def __init__(self, token: Callable[[], str]):
        self._token = token

    def auth_header(self) -> str:
        return f"Bearer {self._token()}"


class Tables(Protocol):
    def create_table(
        self,
        token: Token,
        namespace: str,
        name: str,
        columns: list[dict],
        key_columns: list[str] | None = None,
        layout: Layout | None = None,
    ) -> str:
        """Create the table as the caller; returns its storage location."""

    def read(
        self,
        token: Token,
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
        token: Token,
        namespace: str,
        name: str,
        row_filter: BooleanExpression | None,
        snapshot_id: int | None,
    ) -> int:
        """Rows matching ``row_filter`` in the snapshot, as the caller."""

    def insert_missing(
        self, token: Token, namespace: str, name: str, data: pa.Table
    ) -> int:
        """Insert the rows whose key isn't in the table yet, leaving existing rows
        alone, as the caller. Returns how many were inserted."""

    def merge(
        self,
        token: Token,
        namespace: str,
        name: str,
        data: pa.Table,
        key_columns: list[str],
        provided: list[str],
    ) -> WriteResult:
        """Upsert where only the ``provided`` columns change: a matched row keeps
        its current values in the others, as the caller."""

    def existing_keys(
        self, token: Token, namespace: str, name: str, column: str, values: list[str]
    ) -> set[str]:
        """Which of ``values`` are in the table's ``column``, as the caller."""

    def add_columns(
        self, token: Token, namespace: str, name: str, columns: list[dict]
    ) -> None:
        """Add optional columns to the table's schema, as the caller (Iceberg schema
        evolution: no data is rewritten). Columns it already has are left alone."""

    def drop_table(self, token: Token, namespace: str, name: str) -> None:
        """Drop the table and its files, as the caller. A missing table is fine."""

    def write(
        self,
        token: Token,
        namespace: str,
        name: str,
        data: pa.Table,
        mode: WriteMode,
        overwrite_filter: BooleanExpression | None = None,
    ) -> WriteResult:
        """Commit conformed data as the caller (one snapshot)."""

    def has_snapshot(
        self, token: Token, namespace: str, name: str, snapshot_id: int
    ) -> bool:
        """Whether ``snapshot_id`` is one of the table's snapshots, as the caller."""

    def write_file(
        self, token: Token, namespace: str, name: str, data: pa.Table, file_name: str
    ) -> str:
        """Write conformed rows as a Parquet file in the table's data location, not
        yet part of the table (ADR-0017: a shard's output); returns its path."""

    def commit_files(
        self,
        token: Token,
        namespace: str,
        name: str,
        paths: list[str],
        overwrite_filter: BooleanExpression | None = None,
    ) -> WriteResult:
        """Replace the rows ``overwrite_filter`` keeps (all, when None) with the
        rows of files :meth:`write_file` wrote, in one commit, as the caller."""

    def delete_files(self, token: Token, namespace: str, name: str, paths: list[str]):
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

    def _catalog(self, token: Token) -> Catalog:
        if callable(token):
            auth = {"type": "custom", "impl": f"{__name__}.TokenAuth",
                    "custom": {"token": token}}  # fmt: skip
            return load_catalog("polaris", **self._properties, auth=auth)
        return load_catalog("polaris", **self._properties, token=token)

    def create_table(
        self,
        token: Token,
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
                partition_spec=wh.partition_spec(schema, layout.partition),
                sort_order=wh.sort_order(schema, layout.sort),
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
        token: Token,
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
                if not wh.key_columns(table):
                    raise TableError("upsert needs a table with key columns")
                wh.upsert(table, data)
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
        self, token: Token, namespace: str, name: str, data: pa.Table
    ) -> int:
        return self._guard(
            lambda: wh.insert_missing(
                self._catalog(token).load_table((namespace, name)), data
            )
        )

    def merge(
        self,
        token: Token,
        namespace: str,
        name: str,
        data: pa.Table,
        key_columns: list[str],
        provided: list[str],
    ) -> WriteResult:
        def run() -> WriteResult:
            table = self._catalog(token).load_table((namespace, name))
            return WriteResult(wh.merge(table, data, key_columns, provided))

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

    def _guard(self, call: Callable[[], T]) -> T:
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
        token: Token,
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
        token: Token,
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
        self, token: Token, namespace: str, name: str, column: str, values: list[str]
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
        self, token: Token, namespace: str, name: str, snapshot_id: int
    ) -> bool:
        def run() -> bool:
            table = self._catalog(token).load_table((namespace, name))
            return table.snapshot_by_id(snapshot_id) is not None

        return self._guard(run)

    def write_file(
        self, token: Token, namespace: str, name: str, data: pa.Table, file_name: str
    ) -> str:
        return self._guard(
            lambda: wh.write_file(
                self._catalog(token).load_table((namespace, name)), data, file_name
            )
        )

    def commit_files(
        self,
        token: Token,
        namespace: str,
        name: str,
        paths: list[str],
        overwrite_filter: BooleanExpression | None = None,
    ) -> WriteResult:
        def run() -> WriteResult:
            table = self._catalog(token).load_table((namespace, name))
            return WriteResult(wh.commit_files(table, paths, overwrite_filter))

        return self._guard(run)

    def delete_files(self, token: Token, namespace: str, name: str, paths: list[str]):
        def run() -> None:
            table = self._catalog(token).load_table((namespace, name))
            for path in paths:
                table.io.delete(path)

        return self._guard(run)

    def add_columns(
        self, token: Token, namespace: str, name: str, columns: list[dict]
    ) -> None:
        def run() -> None:
            table = self._catalog(token).load_table((namespace, name))
            have = {f.name for f in table.schema().fields}
            new = [c for c in columns if c["name"] not in have]
            if not new:  # a retry after the record's commit failed
                return
            with table.update_schema() as update:
                for col in new:
                    update.add_column(
                        col["name"], COLUMN_TYPES[col["type"]], doc=col.get("doc")
                    )

        self._guard(run)

    def drop_table(self, token: Token, namespace: str, name: str) -> None:
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
