"""
OGC API - Features Part 3: Queryables endpoints.

Provides machine-readable schema for filterable properties in each collection.
Extends standard JSON Schema with x-teehr-role to indicate group_by vs metric
fields.
"""

import logging

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from ..config import config
from ..database import (
    execute_query,
    get_trino_connection,
    sanitize_string,
    trino_catalog,
    trino_schema,
)
from .filtering import (
    build_equality_filter_conditions,
    get_filterable_columns,
    resolve_column_alias,
    resolve_filter_aliases,
    verify_filtered_columns,
)
from .utils import get_id_column, prepare_for_serialization

logger = logging.getLogger("teehr-api.queryables")

# Populated by a deployment workflow with the distinct group_by combinations
# of each metrics table, because a live DISTINCT on large tables takes seconds.
COMBINATIONS_TABLE = "queryable_combinations"

router = APIRouter()


def _build_collection_schema(collection_id: str) -> dict:
    """Return the declared queryables schema for a collection."""
    if collection_id in COLLECTION_CONFIGS:
        config = COLLECTION_CONFIGS[collection_id]
        return {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": f"/collections/{collection_id}/queryables",
            "type": "object",
            "title": collection_id,
            "description": config["description"],
            "properties": config["static_properties"],
        }

    sanitized = sanitize_string(collection_id)
    return get_metrics_table_queryables(sanitized)


def _validate_queryable_property(schema: dict, property_name: str):
    """Ensure the requested property exists in the collection schema."""
    if property_name not in schema["properties"]:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported property: {property_name}",
        )


# Known collections and their configurations
COLLECTION_CONFIGS = {
    "locations": {
        "table": "locations",
        "type": "feature",
        "description": "Geographic locations where observations are collected",
        "static_properties": {
            "id": {"title": "Location ID", "type": "string", "x-ogc-role": "id"},
            "name": {"title": "Location Name", "type": "string"},
            "geometry": {
                "$ref": "https://geojson.org/schema/Point.json",
                "x-ogc-role": "primary-geometry",
            },
            "properties": {
                "title": "Additional Properties",
                "type": "object",
            },
            "created_at": {
                "title": "Created At",
                "type": "string",
                "format": "date-time",
            },
            "updated_at": {
                "title": "Updated At",
                "type": "string",
                "format": "date-time",
            },
        },
    },
    "primary_timeseries": {
        "table": "primary_timeseries",
        "type": "feature",
        "description": "Observed timeseries data at monitoring locations",
        "static_properties": {
            "location_id": {
                "title": "Location ID",
                "type": "string",
                "x-ogc-role": "id",
            },
            "value_time": {
                "title": "Value Time",
                "type": "string",
                "format": "date-time",
            },
            "value": {"title": "Observed Value", "type": "number"},
            "variable_name": {"title": "Variable/Parameter", "type": "string"},
            "configuration_name": {"title": "Configuration", "type": "string"},
            "unit_name": {"title": "Unit", "type": "string"},
            "created_at": {
                "title": "Created At",
                "type": "string",
                "format": "date-time",
            },
            "updated_at": {
                "title": "Updated At",
                "type": "string",
                "format": "date-time",
            },
        },
    },
    "secondary_timeseries": {
        "table": "secondary_timeseries",
        "type": "feature",
        "description": "Forecast/simulated timeseries data",
        "static_properties": {
            "location_id": {
                "title": "Location ID",
                "type": "string",
                "x-ogc-role": "id",
            },
            "value_time": {
                "title": "Value Time",
                "type": "string",
                "format": "date-time",
            },
            "reference_time": {
                "title": "Reference/Forecast Time",
                "type": "string",
                "format": "date-time",
            },
            "value": {"title": "Forecast Value", "type": "number"},
            "variable_name": {"title": "Variable/Parameter", "type": "string"},
            "configuration_name": {"title": "Configuration", "type": "string"},
            "member": {"title": "Ensemble Member", "type": "string"},
            "unit_name": {"title": "Unit", "type": "string"},
            "created_at": {
                "title": "Created At",
                "type": "string",
                "format": "date-time",
            },
            "updated_at": {
                "title": "Updated At",
                "type": "string",
                "format": "date-time",
            },
        },
    },
    "location_crosswalks": {
        "table": "location_crosswalks",
        "type": "feature",
        "description": "Crosswalk mapping between primary and secondary location identifiers",
        "static_properties": {
            "primary_location_id": {
                "title": "Primary Location ID",
                "type": "string",
            },
            "secondary_location_id": {
                "title": "Secondary Location ID",
                "type": "string",
            },
            "properties": {
                "title": "Additional Properties",
                "type": "object",
            },
            "created_at": {
                "title": "Created At",
                "type": "string",
                "format": "date-time",
            },
            "updated_at": {
                "title": "Updated At",
                "type": "string",
                "format": "date-time",
            },
        },
    },
    "configurations": {
        "table": "configurations",
        "type": "feature",
        "description": "Configuration definitions for data sources",
        "static_properties": {
            "name": {
                "title": "Configuration Name",
                "type": "string",
                "x-ogc-role": "id",
            },
            "timeseries_type": {
                "title": "Type",
                "type": "string",
            },
            "description": {
                "title": "Description",
                "type": "string",
            },
            "properties": {
                "title": "Additional Properties",
                "type": "object",
            },
            "created_at": {
                "title": "Created At",
                "type": "string",
                "format": "date-time",
            },
            "updated_at": {
                "title": "Updated At",
                "type": "string",
                "format": "date-time",
            },
        },
    },
    "units": {
        "table": "units",
        "type": "feature",
        "description": "Unit definitions for measurements",
        "static_properties": {
            "name": {
                "title": "Unit Name",
                "type": "string",
                "x-ogc-role": "id",
            },
            "long_name": {
                "title": "Long Name",
                "type": "string",
            },
            "created_at": {
                "title": "Created At",
                "type": "string",
                "format": "date-time",
            },
            "updated_at": {
                "title": "Updated At",
                "type": "string",
                "format": "date-time",
            },
        },
    },
    "variables": {
        "table": "variables",
        "type": "feature",
        "description": "Variable definitions for measured quantities",
        "static_properties": {
            "name": {
                "title": "Variable Name",
                "type": "string",
                "x-ogc-role": "id",
            },
            "long_name": {
                "title": "Long Name",
                "type": "string",
            },
            "created_at": {
                "title": "Created At",
                "type": "string",
                "format": "date-time",
            },
            "updated_at": {
                "title": "Updated At",
                "type": "string",
                "format": "date-time",
            },
        },
    },
    "attributes": {
        "table": "attributes",
        "type": "feature",
        "description": "Attribute definitions for location attribute types",
        "static_properties": {
            "name": {
                "title": "Attribute Name",
                "type": "string",
                "x-ogc-role": "id",
            },
            "description": {
                "title": "Description",
                "type": "string",
            },
            "type": {
                "title": "Type",
                "type": "string",
            },
            "created_at": {
                "title": "Created At",
                "type": "string",
                "format": "date-time",
            },
            "updated_at": {
                "title": "Updated At",
                "type": "string",
                "format": "date-time",
            },
        },
    },
    "location_attributes": {
        "table": "location_attributes",
        "type": "feature",
        "description": "Attribute values associated with locations",
        "static_properties": {
            "location_id": {
                "title": "Location ID",
                "type": "string",
            },
            "attribute_name": {
                "title": "Attribute Name",
                "type": "string",
            },
            "value": {
                "title": "Value",
                "type": "string",
            },
            "properties": {
                "title": "Additional Properties",
                "type": "object",
            },
            "created_at": {
                "title": "Created At",
                "type": "string",
                "format": "date-time",
            },
            "updated_at": {
                "title": "Updated At",
                "type": "string",
                "format": "date-time",
            },
        },
    },
}


def get_metrics_table_queryables(table_name: str) -> dict:
    """
    Build queryables schema for a metrics table by reading Iceberg properties.

    Returns JSON Schema with x-teehr-role extensions for group_by and metric
    fields.
    """
    try:
        with get_trino_connection() as conn:
            cur = conn.cursor()

            # Get table properties from Iceberg metadata
            query = f"""
                SELECT key, value FROM "{table_name}$properties"
                WHERE key IN ('metrics', 'group_by', 'description')
            """
            cur.execute(query)
            results = cur.fetchall()

        properties_meta = {}
        for key, value in results:
            if key in ("metrics", "group_by"):
                properties_meta[key] = [s.strip() for s in value.split(",")]
            else:
                properties_meta[key] = value

        group_by = properties_meta.get("group_by", [])
        metrics = properties_meta.get("metrics", [])
        description = properties_meta.get("description", f"Metrics table: {table_name}")

        # Build properties schema
        properties = {}

        # Add geometry (all metrics tables have it)
        if "geometry" in group_by:
            properties["geometry"] = {
                "$ref": "https://geojson.org/schema/Point.json",
                "x-ogc-role": "primary-geometry",
            }

        # Add group_by fields
        ogc_id_field = get_id_column(group_by)
        for field in group_by:
            # geometry is handled separately as a GeoJSON primary geometry;
            # avoid overwriting its schema with a generic string schema.
            if field == "geometry":
                continue
            properties[field] = {
                "title": field.replace("_", " ").title(),
                "type": "string",
                "x-teehr-role": "group_by",
            }
            # Mark the location id column as the OGC id
            if field == ogc_id_field:
                properties[field]["x-ogc-role"] = "id"

        # Add metric fields
        for field in metrics:
            properties[field] = {
                "title": field.replace("_", " ").title(),
                "type": "number",
                "x-teehr-role": "metric",
            }

        return {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": f"/collections/{table_name}/queryables",
            "type": "object",
            "title": table_name,
            "description": description,
            "properties": properties,
            # TEEHR extensions for quick access
            "x-teehr-group-by": group_by,
            "x-teehr-metrics": metrics,
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to load queryables for {table_name}: {str(e)}",
        ) from e


@router.get("/collections/{collection_id}/queryables")
def get_collection_queryables(collection_id: str):
    """
    Get queryable properties for a collection (OGC API - Features Part 3).

    Returns a JSON Schema describing filterable properties. For metrics tables,
    includes x-teehr-role extensions indicating whether each field is a
    'group_by' dimension or a 'metric' value.

    Standard clients can use the JSON Schema for validation and UI generation.
    TEEHR-aware clients can use x-teehr-group-by and x-teehr-metrics for
    specialized handling.
    """
    schema = _build_collection_schema(collection_id)

    return JSONResponse(content=schema, media_type="application/schema+json")


def _query_cached_values(
    collection: str,
    property_name: str,
    filters: dict[str, str],
) -> list | None:
    """
    Return distinct values from the combinations table, or None when it has
    no answer and the caller must query the source table.
    """
    keys = [property_name, *filters]
    conditions = [f"source_table = '{collection}'"] + [
        f"contains(map_keys(dimensions), '{key}')" for key in keys
    ]
    for key, value in filters.items():
        sanitized_value = sanitize_string(value)
        if sanitized_value == "null":
            conditions.append(f"element_at(dimensions, '{key}') IS NULL")
        else:
            conditions.append(f"element_at(dimensions, '{key}') = '{sanitized_value}'")

    if config.CACHE_REQUIRES_CURRENT_SNAPSHOT:
        conditions.append(
            f"""source_snapshot_id = (
                SELECT CAST(value AS BIGINT)
                FROM {trino_catalog}.{trino_schema}."{collection}$properties"
                WHERE key = 'current-snapshot-id'
            )"""
        )

    query = f"""
        SELECT DISTINCT element_at(dimensions, '{property_name}')
            AS {property_name}
        FROM {trino_catalog}.{trino_schema}.{COMBINATIONS_TABLE}
        WHERE {" AND ".join(conditions)}
        ORDER BY {property_name}
    """
    try:
        df = execute_query(query)
    except Exception as e:
        # Deployments without the workflow never create the table.
        if "TABLE_NOT_FOUND" not in str(e):
            logger.warning(f"Combinations lookup failed, falling back: {e}")
        return None

    if df.empty:
        return None
    return prepare_for_serialization(df)[property_name].tolist()


@router.get("/collections/{collection_id}/queryables/{property_name}/values")
def get_queryable_values(
    collection_id: str,
    property_name: str,
    request: Request,
):
    """
    Get distinct values for a queryable property (TEEHR extension).

    This is an extension to OGC API - Features Part 3 that returns the unique
    values available for a specific queryable property. Useful for populating
    filter dropdowns in UI applications.

    Returns a JSON array of distinct values.

    Additional query parameters are interpreted as equality filters, using the
    same schema-driven approach as the collection items endpoint.
    """
    # Validate and sanitize inputs
    sanitized_collection = sanitize_string(collection_id)
    sanitized_property = sanitize_string(property_name)

    if not sanitized_collection or not sanitized_property:
        raise HTTPException(
            status_code=400, detail="Invalid collection or property name"
        )

    try:
        schema = _build_collection_schema(collection_id)
        columns = get_filterable_columns(schema, default_to_properties=True)
        property_name = resolve_column_alias(property_name, columns)
        sanitized_property = sanitize_string(property_name)
        _validate_queryable_property(schema, property_name)

        filters = resolve_filter_aliases(dict(request.query_params.items()), columns)
        verify_filtered_columns(
            schema,
            list(filters.keys()),
            default_to_properties=True,
        )
        where_clause = " AND ".join(build_equality_filter_conditions(filters))

        if sanitized_property in schema.get("x-teehr-group-by", []):
            cached = _query_cached_values(
                sanitized_collection, sanitized_property, filters
            )
            if cached is not None:
                return JSONResponse(content=cached, media_type="application/json")

        # Query distinct values
        query = f"""
            SELECT DISTINCT {sanitized_property}
            FROM {trino_catalog}.{trino_schema}.{sanitized_collection}
            {f"WHERE {where_clause}" if where_clause else ""}
            ORDER BY {sanitized_property}
        """
        raw_df = execute_query(query)
        df = prepare_for_serialization(raw_df)
        values = df[sanitized_property].tolist() if not df.empty else []

        return JSONResponse(content=values, media_type="application/json")

    except HTTPException:
        raise

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get values for {property_name}: {str(e)}",
        ) from e
