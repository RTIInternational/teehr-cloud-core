"""Layer geometry (layers-and-crosswalks.md): stored as WKB in EPSG:4326, with its
bounds in four columns so Iceberg can skip files for a spatial filter.

Writes take WKB (Arrow, Parquet), WKT text, or GeoJSON geometry objects (JSON);
every geometry is checked (parses, not empty, valid, coordinates in range) and every
problem reported at once. Reads give WKB (Arrow; Parquet with GeoParquet metadata),
GeoJSON geometry objects in JSON rows, or a GeoJSON FeatureCollection.
"""

import json

import numpy as np
import pyarrow as pa
import shapely

from src.dataset_types import BBOX
from src.tables import DataMismatch

GEOJSON = "application/geo+json"
EXAMPLES = 5

# GeoParquet 1.1: no "crs" means OGC:CRS84 (EPSG:4326, longitude/latitude).
GEOPARQUET = {
    "version": "1.1.0",
    "primary_column": "geometry",
    "columns": {"geometry": {"encoding": "WKB", "geometry_types": []}},
}


def _keys(data: pa.Table, mask: np.ndarray) -> str:
    if "location_key" not in data.column_names:
        rows = np.flatnonzero(mask)[:EXAMPLES]
        return "rows " + ", ".join(str(i + 1) for i in rows)
    keys = np.asarray(data["location_key"].to_pylist(), dtype=object)[mask]
    return ", ".join(str(k) for k in keys[:EXAMPLES])


def _parse(column: pa.ChunkedArray) -> np.ndarray:
    """Shapely geometries (None where a value doesn't parse or is null)."""
    if pa.types.is_string(column.type) or pa.types.is_large_string(column.type):
        values = np.asarray(column.to_pylist(), dtype=object)
        return shapely.from_wkt(values, on_invalid="ignore")
    if pa.types.is_binary(column.type) or pa.types.is_large_binary(column.type):
        values = np.asarray(column.to_pylist(), dtype=object)
        return shapely.from_wkb(values, on_invalid="ignore")
    raise DataMismatch(
        f"column 'geometry' must be WKB (binary) or WKT (text), not {column.type}"
    )


def prepare_layer(data: pa.Table) -> pa.Table:
    """Check a layer's geometries and give it WKB geometry and bounds columns.
    Raises DataMismatch with every problem found."""
    if "geometry" not in data.column_names:
        raise DataMismatch("missing required column 'geometry'")
    column = data["geometry"]
    nulls = np.asarray(column.is_null().to_pylist(), dtype=bool)
    geoms = _parse(column)
    unparsed = ~nulls & np.equal(geoms, None)
    ok = ~np.equal(geoms, None)
    empty = ok & shapely.is_empty(geoms)
    ok &= ~empty
    invalid = ok & ~shapely.is_valid(geoms)
    bounds = shapely.bounds(geoms)  # NaN where there's no geometry
    with np.errstate(invalid="ignore"):
        out_of_range = (
            ok
            & ~invalid
            & (
                (bounds[:, 0] < -180)
                | (bounds[:, 2] > 180)
                | (bounds[:, 1] < -90)
                | (bounds[:, 3] > 90)
            )
        )
    problems = []
    for mask, what in (
        (nulls, "have no geometry"),
        (unparsed, "have geometry that isn't valid WKB/WKT/GeoJSON"),
        (empty, "have an empty geometry"),
        (invalid, "have an invalid geometry (e.g. a self-intersecting polygon)"),
        (
            out_of_range,
            "have coordinates outside longitude -180..180 / latitude -90..90 "
            "(projected coordinates? geometry must be EPSG:4326)",
        ),
    ):
        if mask.any():
            problems.append(f"{int(mask.sum())} rows {what}: {_keys(data, mask)}")
    if problems:
        raise DataMismatch("; ".join(problems))
    wkb = pa.array(shapely.to_wkb(geoms, output_dimension=2).tolist(), pa.binary())
    out = data.drop_columns([c for c in (*BBOX, "geometry") if c in data.column_names])
    out = out.append_column("geometry", wkb)
    for i, name in enumerate(BBOX):
        out = out.append_column(name, pa.array(bounds[:, i], pa.float64()))
    return out


def json_geometry(rows: list[dict]) -> tuple[list[dict], list]:
    """JSON rows without their geometry, and the geometries as WKT (from GeoJSON
    geometry objects or WKT strings; None where it doesn't parse, for the checks
    to report)."""
    rest, wkt = [], []
    for row in rows:
        row = dict(row)
        value = row.pop("geometry", None)
        if isinstance(value, dict):
            try:
                value = shapely.from_geojson(json.dumps(value)).wkt
            except shapely.errors.GEOSException:
                value = ""  # reported as unparseable
        elif value is not None and not isinstance(value, str):
            value = ""
        rest.append(row)
        wkt.append(value)
    return rest, wkt


def as_geojson(column: pa.ChunkedArray) -> list[dict | None]:
    """WKB values as GeoJSON geometry objects."""
    geoms = shapely.from_wkb(np.asarray(column.to_pylist(), dtype=object))
    return [
        None if g is None else json.loads(shapely.to_geojson(g)) for g in geoms.tolist()
    ]


def feature_collection(rows: list[dict], geometry: list[dict | None]) -> dict:
    """Rows (JSON-ready, without geometry/bounds) as a GeoJSON FeatureCollection."""
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "id": row.get("location_key"),
                "geometry": geom,
                "properties": {k: v for k, v in row.items() if k not in BBOX},
            }
            for row, geom in zip(rows, geometry, strict=True)
        ],
    }


def with_geoparquet(table: pa.Table) -> pa.Table:
    """The table with GeoParquet metadata when it has the geometry column."""
    if "geometry" not in table.column_names:
        return table
    metadata = {**(table.schema.metadata or {}), b"geo": json.dumps(GEOPARQUET)}
    return table.replace_schema_metadata(metadata)


def bbox_filter(text: str) -> str:
    """A ``bbox=minx,miny,maxx,maxy`` parameter as a filter on the bounds columns:
    the geometries whose bounds overlap it."""
    try:
        minx, miny, maxx, maxy = (float(v) for v in text.split(","))
    except ValueError as exc:
        raise DataMismatch("bbox must be minx,miny,maxx,maxy (4 numbers)") from exc
    if minx > maxx or miny > maxy:
        raise DataMismatch("bbox: min must not exceed max")
    return (
        f"bbox_xmax >= {minx} and bbox_xmin <= {maxx} and "
        f"bbox_ymax >= {miny} and bbox_ymin <= {maxy}"
    )
