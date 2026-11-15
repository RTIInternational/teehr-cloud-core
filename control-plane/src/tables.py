"""Iceberg table I/O as the caller (Phase 2).

Every call uses the caller's own Keycloak token against Polaris, so Polaris
enforces the caller's project roles and vends storage credentials scoped to the
table. The API never reads or writes project data with its own identity.

PyIceberg does the table work (catalog calls, Parquet I/O, commits); there is no
query engine here. Routes depend on :func:`get_tables`, which tests replace.
"""

from collections.abc import Iterable
from functools import lru_cache
from typing import Protocol

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import (
    ForbiddenError,
    NoSuchNamespaceError,
    NoSuchTableError,
    TableAlreadyExistsError,
    UnauthorizedError,
)
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

    def drop_table(self, token: str, namespace: str, name: str) -> None:
        """Drop the table and its files, as the caller. A missing table is fine."""


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
