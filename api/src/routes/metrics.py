"""
Generic collection item endpoint (OGC API Features).

Serves any Iceberg table that declares its dimensions and values as table
properties, so metrics tables and the derived summary tables are all handled
here without per-table code. See queryables.get_metrics_table_queryables.
"""

import logging
import time

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from ..auth import effective_limit_for_request
from ..database import (
    execute_query, sanitize_string, trino_catalog, trino_schema
)
from .queryables import get_metrics_table_queryables
from .utils import (
    create_ogc_geojson_response,
    create_ogc_records_response,
    prepare_for_serialization,
)

router = APIRouter()
logger = logging.getLogger("teehr-api.routes.metrics")

# Query parameters the endpoint consumes itself; everything else is a filter.
RESERVED_PARAMS = ["collection_id", "location_id", "limit", "offset", "f"]

GEOJSON = "geojson"
JSON = "json"

# The OGC id column, where a collection has one. Also the leading sort key.
ID_COLUMN = "primary_location_id"

# Collection schemas come from Iceberg table properties, which only change when
# the upstream Prefect flows run. Caching avoids a Trino round trip per request.
_SCHEMA_CACHE: dict[str, tuple[float, dict]] = {}
_SCHEMA_CACHE_TTL_SECONDS = 60


def _get_collection_schema(table: str) -> dict:
    """Return the queryables schema for a collection, memoized briefly."""
    cached = _SCHEMA_CACHE.get(table)
    if cached is not None and time.time() - cached[0] < _SCHEMA_CACHE_TTL_SECONDS:
        return cached[1]

    schema = get_metrics_table_queryables(table)
    _SCHEMA_CACHE[table] = (time.time(), schema)
    return schema


def _verify_filtered_columns(
        schema: dict,
        filtered_columns: list[str]
):
    """Validate filtered columns against group-by columns for collection.

    Respond with 400 if invalid filters found.
    """
    available_columns = schema["x-teehr-group-by"]
    invalid_filters = set(filtered_columns) - set(available_columns)
    if invalid_filters:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported filters: {', '.join(sorted(invalid_filters))}"
        )


def _order_by_clause(schema: dict) -> str:
    """Order by the collection's group-by columns.

    Those columns are the table's uniqueness key, so ordering by all of them
    gives a total order and therefore stable OFFSET/LIMIT pagination. They also
    match the table's Iceberg write order, which keeps the sort cheap.
    """
    order_columns = [
        column for column in schema["x-teehr-group-by"] if column != "geometry"
    ]
    if not order_columns:
        return ""
    sanitized = [sanitize_string(column) for column in order_columns]
    return "ORDER BY " + ", ".join(sanitized)


def _resolve_format(requested_format: str | None, has_geometry: bool) -> str:
    """Resolve the output format, defaulting to the collection's natural shape."""
    if requested_format is None:
        return GEOJSON if has_geometry else JSON

    resolved = requested_format.lower()
    if resolved not in (JSON, GEOJSON):
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported format '{requested_format}'. Use 'json' or 'geojson'.",
        )
    if resolved == GEOJSON and not has_geometry:
        raise HTTPException(
            status_code=400,
            detail="Collection has no geometry column; use f=json.",
        )
    return resolved


@router.get("/collections/{collection_id}/items")
async def get_collection_items(
    collection_id: str,
    request: Request,
    location_id: str | None = Query(
        None, alias="location_id", description="Filter by location ID"
    ),
    configuration_name: str | None = Query(
        None, description="Filter by configuration name"
    ),
    variable_name: str | None = Query(
        None,
        description="Filter by variable name"
    ),
    limit: int | None = Query(
        None, ge=1, description="Maximum number of items to return (omit to return all)"
    ),
    offset: int | None = Query(
        None,
        ge=0,
        description="Starting index for pagination"
    ),
    f: str | None = Query(
        None,
        description=(
            "Output format. 'geojson' returns an OGC GeoJSON FeatureCollection; "
            "'json' returns an OGC-style paging envelope "
            "({items, numberReturned, links}). Defaults to 'geojson' for "
            "collections with geometry and 'json' for those without."
        ),
    ),
):
    """Get items from any collection (OGC API Features endpoint).

    Handles any collection described by Iceberg table properties. The
    locations, timeseries and reference data collections have their own
    endpoints.

    Dynamic filtering is supported through the inclusion of additional query parameters.
    All additional query parameters will be interpreted as equality filters against
    collection columns.

    If an invalid filter is requested, endpoint will respond with HTTP 400.

    A filter value of "null" is interpreted as the SQL NULL type.
    """
    try:
        limit = effective_limit_for_request(request, limit)

        # Use the collection_id as the table name
        sanitized_table = sanitize_string(collection_id)
        schema = _get_collection_schema(sanitized_table)

        filters = {
            k: v
            for k, v in request.query_params.items()
            if k not in RESERVED_PARAMS
        }

        _verify_filtered_columns(schema, filters.keys())

        where_conditions = []

        # location_id is an alias for the collection's id column, and only
        # applies to collections that have one.
        if ID_COLUMN in schema["x-teehr-group-by"]:
            if "location_id" in request.query_params:
                sanitized_location_id = sanitize_string(
                    request.query_params["location_id"]
                )
                where_conditions.append(
                    f"{ID_COLUMN} = '{sanitized_location_id}'"
                )
            else:
                # Restricts results to gage locations. Basin-level rows use a
                # 'usgsbasin-' prefix and carry polygon rather than point
                # geometry, which no current client can render -- and which
                # would be far too large to ship as GeoJSON anyway. It
                # keeps unrenderable geometry off the wire.
                where_conditions.append(f"{ID_COLUMN} LIKE 'usgs-%'")

        for column, value in filters.items():
            sanitized_column = sanitize_string(column)
            sanitized_value = sanitize_string(value)
            if sanitized_value == "null":
                where_conditions.append(f"{sanitized_column} IS NULL")
            else:
                where_conditions.append(f"{sanitized_column} = '{sanitized_value}'")

        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"

        pagination = ""
        if offset is not None:
            pagination += f" OFFSET {offset}"
        if limit is not None:
            pagination += f" LIMIT {limit}"

        query = f"""
            SELECT *
            FROM {trino_catalog}.{trino_schema}.{sanitized_table}
            WHERE {where_clause}
            {_order_by_clause(schema)}
            {pagination}
        """

        query_start = time.time()
        df = execute_query(query)
        query_time = time.time() - query_start
        logger.debug("Metrics query execution time: %.3f seconds", query_time)

        output_format = _resolve_format(f, "geometry" in df.columns)

        # Record responses have no use for the geometry blob.
        if output_format == JSON and "geometry" in df.columns:
            df = df.drop(columns=["geometry"])

        # Timestamps must be formatted explicitly for the record path, where
        # they would otherwise reach json.dumps as datetime objects.
        datetime_columns = list(
            dict.fromkeys(
                df.select_dtypes(
                    include=["datetime64[ns]", "datetimetz"]
                ).columns.tolist()
                + ["created_at", "updated_at"]
            )
        )
        df = prepare_for_serialization(df, datetime_columns=datetime_columns)

        if output_format == JSON:
            return JSONResponse(
                content=create_ogc_records_response(
                    df,
                    str(request.url),
                    collection_id=collection_id,
                    limit=limit,
                    offset=offset,
                ),
                media_type="application/json",
            )

        geojson = create_ogc_geojson_response(
            df,
            str(request.url),
            collection_id=collection_id,
            limit=limit,
            offset=offset,
        )

        return JSONResponse(
            content=geojson,
            headers={
                "Content-Type": "application/geo+json",
                "Content-Crs": "<http://www.opengis.net/def/crs/OGC/1.3/CRS84>",  # noqa: E501
            },
        )

    except HTTPException:
        raise

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to load collection '{collection_id}': {str(e)}",
        ) from e
