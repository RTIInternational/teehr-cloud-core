"""
Mapping collection endpoints: location crosswalks, location ID aliases and
configuration pairs.
"""

import time
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from ..auth import effective_limit_for_request
from ..database import execute_query, sanitize_string, trino_catalog, trino_schema
from .utils import prepare_for_serialization

router = APIRouter()


def _in_condition(column: str, values: list[str] | None) -> list[str]:
    """Return an ``IN`` predicate for ``values``, or nothing if none given."""
    if not values:
        return []
    safe_values = [f"'{sanitize_string(v)}'" for v in values]
    return [f"{column} IN ({', '.join(safe_values)})"]


def _prefix_condition(column: str, prefixes: list[str] | None) -> list[str]:
    """Return a predicate matching any of the ``<prefix>-`` ID prefixes."""
    if not prefixes:
        return []
    like_clauses = " OR ".join(
        f"{column} LIKE '{sanitize_string(p)}-%'" for p in prefixes
    )
    return [f"({like_clauses})"]


def _query_records(
    request: Request,
    collection_id: str,
    where_conditions: list[str],
    order_by: str,
    limit: int | None,
    offset: int | None,
) -> JSONResponse:
    """Query a mapping table and return an OGC-style records response."""
    limit = effective_limit_for_request(request, limit)
    where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"

    pagination = ""
    if offset is not None:
        pagination += f" OFFSET {offset}"
    if limit is not None:
        pagination += f" LIMIT {limit}"

    query = f"""
        SELECT *
        FROM {trino_catalog}.{trino_schema}.{collection_id}
        WHERE {where_clause}
        ORDER BY {order_by}
        {pagination}
    """

    query_start = time.time()
    df = execute_query(query)
    query_time = time.time() - query_start
    print(f"{collection_id} query execution time: {query_time:.3f} seconds")

    self_link = {"href": str(request.url), "rel": "self", "type": "application/json"}

    if df.empty:
        return JSONResponse(
            content={"items": [], "numberReturned": 0, "links": [self_link]},
            media_type="application/json",
        )

    df = prepare_for_serialization(df)
    items = df.to_dict(orient="records")

    response = {
        "items": items,
        "numberReturned": len(items),
        "links": [
            self_link,
            {
                "href": f"/collections/{collection_id}",
                "rel": "collection",
                "type": "application/json",
            },
        ],
    }

    if limit is not None and len(items) == limit:
        parsed = urlparse(str(request.url))
        query_params = parse_qs(parsed.query)
        query_params["offset"] = [str((offset or 0) + limit)]
        query_params["limit"] = [str(limit)]
        next_query = urlencode(query_params, doseq=True)
        next_url = urlunparse(parsed._replace(query=next_query))
        response["links"].append(
            {"href": next_url, "rel": "next", "type": "application/json"}
        )

    return JSONResponse(content=response, media_type="application/json")


@router.get("/collections/location_crosswalks/items")
def get_crosswalk_items(
    request: Request,
    primary_location_id: list[str] | None = Query(
        None, description="Filter by primary location ID (can be specified multiple times)"
    ),
    secondary_location_id: list[str] | None = Query(
        None, description="Filter by secondary location ID (can be specified multiple times)"
    ),
    primary_location_id_prefix: list[str] | None = Query(
        None, description="Filter by primary location ID prefix (can be specified multiple times)"
    ),
    secondary_location_id_prefix: list[str] | None = Query(
        None, description="Filter by secondary location ID prefix (can be specified multiple times)"
    ),
    limit: int | None = Query(
        None, ge=1, description="Maximum number of items to return (omit to return all)"
    ),
    offset: int | None = Query(
        None, ge=0, description="Starting index for pagination"
    ),
):
    """Get location crosswalk mappings.

    Returns mappings between primary (e.g., USGS) and secondary (e.g., NWM)
    location identifiers. This is a non-spatial collection (no geometry).
    Use primary_location_id_prefix or secondary_location_id_prefix to filter by ID prefix (can be specified multiple times).
    """
    try:
        where_conditions = [
            *_in_condition("primary_location_id", primary_location_id),
            *_in_condition("secondary_location_id", secondary_location_id),
            *_prefix_condition("primary_location_id", primary_location_id_prefix),
            *_prefix_condition("secondary_location_id", secondary_location_id_prefix),
        ]
        return _query_records(
            request,
            "location_crosswalks",
            where_conditions,
            "primary_location_id, secondary_location_id",
            limit,
            offset,
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to load crosswalks: {str(e)}",
        ) from e


@router.get("/collections/location_id_aliases/items")
def get_location_id_alias_items(
    request: Request,
    location_id: list[str] | None = Query(
        None, description="Filter by location ID (can be specified multiple times)"
    ),
    location_id_alias: list[str] | None = Query(
        None,
        description="Filter by location ID alias (can be specified multiple times)",
    ),
    location_id_prefix: list[str] | None = Query(
        None,
        description="Filter by location ID prefix (can be specified multiple times)",
    ),
    location_id_alias_prefix: list[str] | None = Query(
        None,
        description=(
            "Filter by location ID alias prefix (can be specified multiple times)"
        ),
    ),
    limit: int | None = Query(
        None, ge=1, description="Maximum number of items to return (omit to return all)"
    ),
    offset: int | None = Query(
        None, ge=0, description="Starting index for pagination"
    ),
):
    """Get location ID aliases.

    Maps alternate IDs (e.g., a second observation source at the same site)
    to a location ID in the locations table. Primary timeseries may be stored
    under either. This is a non-spatial collection (no geometry).
    """
    try:
        where_conditions = [
            *_in_condition("location_id", location_id),
            *_in_condition("location_id_alias", location_id_alias),
            *_prefix_condition("location_id", location_id_prefix),
            *_prefix_condition("location_id_alias", location_id_alias_prefix),
        ]
        return _query_records(
            request,
            "location_id_aliases",
            where_conditions,
            "location_id, location_id_alias",
            limit,
            offset,
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to load location ID aliases: {str(e)}",
        ) from e


@router.get("/collections/configuration_pairs/items")
def get_configuration_pair_items(
    request: Request,
    primary_configuration_name: list[str] | None = Query(
        None,
        description=(
            "Filter by primary configuration name (can be specified multiple times)"
        ),
    ),
    secondary_configuration_name: list[str] | None = Query(
        None,
        description=(
            "Filter by secondary configuration name (can be specified multiple times)"
        ),
    ),
    limit: int | None = Query(
        None, ge=1, description="Maximum number of items to return (omit to return all)"
    ),
    offset: int | None = Query(
        None, ge=0, description="Starting index for pagination"
    ),
):
    """Get configuration pairs.

    Lists which primary configurations each secondary configuration is
    evaluated against. This is a non-spatial collection (no geometry).
    """
    try:
        where_conditions = [
            *_in_condition("primary_configuration_name", primary_configuration_name),
            *_in_condition(
                "secondary_configuration_name", secondary_configuration_name
            ),
        ]
        return _query_records(
            request,
            "configuration_pairs",
            where_conditions,
            "primary_configuration_name, secondary_configuration_name",
            limit,
            offset,
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to load configuration pairs: {str(e)}",
        ) from e
