"""Shared helpers for schema-driven collection filtering."""

from collections.abc import Mapping

from fastapi import HTTPException

from ..database import sanitize_string


def get_filterable_columns(
    schema: dict,
    *,
    default_to_properties: bool = False,
) -> list[str]:
    """Return the columns that support equality filters for a collection."""
    columns = schema.get("x-teehr-group-by")
    if columns is not None:
        return columns
    if default_to_properties:
        return list(schema["properties"].keys())
    return []


def verify_filtered_columns(
    schema: dict,
    filtered_columns: list[str],
    *,
    default_to_properties: bool = False,
):
    """Validate requested filters against the collection schema."""
    available_columns = get_filterable_columns(
        schema,
        default_to_properties=default_to_properties,
    )
    invalid_filters = set(filtered_columns) - set(available_columns)
    if invalid_filters:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported filters: {', '.join(sorted(invalid_filters))}",
        )


def build_equality_filter_conditions(filters: Mapping[str, str]) -> list[str]:
    """Build SQL equality predicates for validated query-parameter filters."""
    where_conditions = []
    for column, value in filters.items():
        sanitized_column = sanitize_string(column)
        sanitized_value = sanitize_string(value)
        if sanitized_value == "null":
            where_conditions.append(f"{sanitized_column} IS NULL")
        else:
            where_conditions.append(f"{sanitized_column} = '{sanitized_value}'")

    return where_conditions
