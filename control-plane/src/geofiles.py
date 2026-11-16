"""Reading uploaded location files (layers-and-crosswalks.md): GeoJSON, zipped
Shapefile and GeoPackage through GDAL (pyogrio), GeoParquet through Arrow, and
reprojecting to EPSG:4326.

Inspect reads a staged file where it is (GDAL range requests), never all of it;
the load job reads a local copy. Geometry comes out as a WKB column named
``geometry``.
"""

import json

import numpy as np
import pyarrow as pa
import pyogrio
import pyproj
import shapely

GEOJSON, SHAPEFILE, GEOPACKAGE = "geojson", "shapefile", "geopackage"
GEO_FORMATS = (GEOJSON, SHAPEFILE, GEOPACKAGE)
SUFFIX = {GEOJSON: ".geojson", SHAPEFILE: ".zip", GEOPACKAGE: ".gpkg"}
WGS84 = "EPSG:4326"

# A presigned URL is for GET only: GDAL mustn't try HEAD first.
pyogrio.set_gdal_config_options(
    {"CPL_VSIL_CURL_USE_HEAD": False, "GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR"}
)


class GeoFileError(ValueError):
    """The file can't be read as the format it claims."""


def gdal_path(fmt: str, path: str) -> str:
    """The path GDAL opens: a zip's Shapefile is read inside the zip."""
    if fmt == SHAPEFILE:
        return f"/vsizip/{{{path}}}"
    return path


def _plain_geometry(table: pa.Table, name: str) -> pa.Table:
    """The table with its geometry column as plain binary named ``geometry``
    (other columns keep their names)."""
    if not name or name not in table.column_names:
        return table
    index = table.column_names.index(name)
    column = table[name].cast(pa.binary())
    table = table.remove_column(index)
    if "geometry" in table.column_names:  # an attribute called geometry
        table = table.rename_columns(
            ["geometry_" if c == "geometry" else c for c in table.column_names]
        )
    return table.add_column(index, "geometry", column)


def layers(path: str) -> list[str]:
    try:
        return [str(name) for name, _ in pyogrio.list_layers(path)]
    except Exception as exc:  # GDAL raises its own errors
        raise GeoFileError(f"can't read the file: {exc}") from exc


def read_geo(
    fmt: str, path: str, layer: str | None = None, max_features: int | None = None
) -> tuple[pa.Table, dict]:
    """A geo file's features (geometry as WKB in ``geometry``) and its info:
    ``crs`` (None if the file doesn't say), ``features``, ``geometry_type``,
    ``layers`` (several in a GeoPackage) and the ``layer`` read."""
    source = gdal_path(fmt, path)
    try:
        names = layers(source)
        if not names:
            raise GeoFileError("the file has no layers")
        if layer is not None and layer not in names:
            raise GeoFileError(f"no layer '{layer}'; it has: {', '.join(names)}")
        layer = layer or names[0]
        info = pyogrio.read_info(source, layer=layer)
        meta, table = pyogrio.read_arrow(source, layer=layer, max_features=max_features)
    except GeoFileError:
        raise
    except Exception as exc:
        raise GeoFileError(f"can't read the file as {fmt}: {exc}") from exc
    geometry = meta.get("geometry_name") or "wkb_geometry"
    crs = info.get("crs")
    if fmt == GEOJSON:
        crs = crs or WGS84  # RFC 7946
    return _plain_geometry(table, geometry), {
        "crs": crs,
        "features": count if (count := info.get("features", -1)) >= 0 else None,
        "geometry_type": info.get("geometry_type"),
        "layers": names,
        "layer": layer,
    }


def geoparquet(schema: pa.Schema) -> tuple[str | None, str | None]:
    """A GeoParquet file's geometry column and CRS (EPSG:4326 when it says none,
    per the spec); (None, None) for plain Parquet."""
    raw = (schema.metadata or {}).get(b"geo")
    if not raw:
        return None, None
    try:
        geo = json.loads(raw)
        column = geo["primary_column"]
        crs = geo["columns"][column].get("crs")
    except (ValueError, KeyError, TypeError):
        return None, None
    if crs is None:
        return column, WGS84
    return column, pyproj.CRS.from_user_input(crs).to_string()


def describe_crs(crs: str) -> str:
    """A CRS as EPSG:<code> when it has one, else as given."""
    try:
        parsed = pyproj.CRS.from_user_input(crs)
    except pyproj.exceptions.CRSError as exc:
        raise GeoFileError(f"unknown CRS '{crs}'") from exc
    code = parsed.to_epsg()
    return f"EPSG:{code}" if code else parsed.to_string()


def is_wgs84(crs: str) -> bool:
    parsed = pyproj.CRS.from_user_input(crs)
    return parsed.to_epsg() == 4326 or parsed.equals(pyproj.CRS("OGC:CRS84"))


def to_wgs84(geometry: pa.ChunkedArray | pa.Array, crs: str) -> pa.Array:
    """WKB geometries in ``crs``, reprojected to EPSG:4326 (longitude, latitude).
    Values that don't parse are passed through for the layer checks to report."""
    values = np.asarray(geometry.to_pylist(), dtype=object)
    if is_wgs84(crs):
        return pa.array(values.tolist(), pa.binary())
    geoms = shapely.from_wkb(values, on_invalid="ignore")
    transformer = pyproj.Transformer.from_crs(crs, WGS84, always_xy=True)
    moved = shapely.transform(
        geoms, lambda xy: np.column_stack(transformer.transform(xy[:, 0], xy[:, 1]))
    )
    out = shapely.to_wkb(moved, output_dimension=2)
    unparsed = np.equal(geoms, None)
    out[unparsed] = values[unparsed]
    return pa.array(out.tolist(), pa.binary())


def points(longitude: pa.ChunkedArray, latitude: pa.ChunkedArray) -> pa.Array:
    """WKB points from longitude/latitude columns (None where either is missing
    or not a number, for the layer checks to report)."""

    def numbers(column):
        try:
            return np.asarray(column.cast(pa.float64()).to_pylist(), dtype=float)
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
            out = []
            for v in column.to_pylist():
                try:
                    out.append(float(v))
                except (TypeError, ValueError):
                    out.append(np.nan)
            return np.asarray(out, dtype=float)

    x = np.asarray(numbers(longitude), dtype=float)
    y = np.asarray(numbers(latitude), dtype=float)
    missing = np.isnan(x) | np.isnan(y)
    geoms = shapely.points(np.where(missing, 0, x), np.where(missing, 0, y))
    wkb = shapely.to_wkb(geoms).astype(object)
    wkb[missing] = None
    return pa.array(wkb.tolist(), pa.binary())
