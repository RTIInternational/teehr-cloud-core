"""Inspecting an uploaded file: what's in it, and how it maps to a dataset.

Reads only the start of the file (a Parquet file's footer and first rows; a CSV's
first megabyte), and proposes a mapping from its columns to the timeseries columns
(warehouse data model v1), plus what the uploader still has to say (role,
variable, unit, ...). Nothing here writes data; the load job does (Phase 3, jobs).
"""

import csv
import io
import re
from dataclasses import dataclass, field

import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

from src.formats import json_rows

FORMATS = ("csv", "parquet", "geojson", "shapefile", "geopackage")
KIND_FORMATS = {
    "timeseries": ("csv", "parquet"),
    "crosswalk": ("csv", "parquet"),
    "layer": FORMATS,
}
CSV_HEAD_BYTES = 1024 * 1024
SAMPLE_ROWS = 10

# Column names that usually mean each timeseries column (lower-case).
CANDIDATES: dict[str, tuple[str, ...]] = {
    "series_key": (
        "series_key", "location_id", "location", "site_no", "site", "station",
        "station_id", "gage_id", "gauge_id", "feature_id", "id",
    ),
    "value_time": (
        "value_time", "datetime", "date_time", "valid_time", "time", "timestamp",
        "date", "obs_time",
    ),
    "value": (
        "value", "streamflow", "discharge", "flow", "stage", "precipitation",
        "precip", "temperature", "obs", "observation",
    ),
    "reference_time": (
        "reference_time", "issue_time", "issued", "forecast_time", "init_time",
    ),
    "member": ("member", "ensemble_member", "ens_member", "trace"),
    "quantile": ("quantile", "probability", "percentile"),
    "quality": ("quality", "qualifier", "qualifiers", "qc", "flag", "flags"),
}  # fmt: skip
MAPPED = tuple(CANDIDATES)
_LEADING_ZERO = re.compile(r"^\s*[+-]?0\d")
_OFFSET = re.compile(r"(?:[Zz]|[+-]\d{2}:?\d{2})$")


class UploadError(ValueError):
    """The file can't be read as the format it claims."""


def detect_format(filename: str) -> str | None:
    suffix = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    return {
        "csv": "csv",
        "parquet": "parquet",
        "pq": "parquet",
        "geoparquet": "parquet",
        "geojson": "geojson",
        "json": "geojson",
        "zip": "shapefile",
        "gpkg": "geopackage",
    }.get(suffix)


def dataset_name(filename: str) -> str:
    """A dataset name proposed from a file name (``USGS Obs-2026.csv`` ->
    ``usgs_obs_2026``)."""
    stem = filename.rsplit("/", 1)[-1].rsplit(".", 1)[0].lower()
    slug = re.sub(r"[^a-z0-9]+", "_", stem).strip("_")
    if not slug or not slug[0].isalpha():
        slug = f"upload_{slug}".rstrip("_")
    return slug[:60]


def read_head(file, fmt: str) -> tuple[pa.Table, int | None]:
    """The first rows of a staged file, and its row count when the format says."""
    try:
        if fmt == "parquet":
            parquet = pq.ParquetFile(file)
            rows = parquet.metadata.num_rows
            if parquet.metadata.num_row_groups == 0:
                return parquet.schema_arrow.empty_table(), rows
            batch = next(parquet.iter_batches(batch_size=SAMPLE_ROWS), None)
            head = (
                pa.Table.from_batches([batch])
                if batch is not None
                else parquet.schema_arrow.empty_table()
            )
            return head, rows
        raw = file.read(CSV_HEAD_BYTES)
        complete = len(raw) < CSV_HEAD_BYTES
        if not complete:  # drop the partial last line
            raw = raw[: raw.rfind(b"\n") + 1]
        table = read_csv_text(raw)
        return table, (table.num_rows if complete else None)
    except (pa.ArrowInvalid, OSError) as exc:
        raise UploadError(f"can't read the file as {fmt}: {exc}") from exc


def read_csv_text(data: bytes) -> pa.Table:
    """A CSV with every column as text (empty = null), so nothing is guessed:
    ``01010000`` stays ``01010000``. Values are typed later, by the dataset's
    columns, the same way JSON writes are."""
    header = next(
        csv.reader(io.StringIO(data.split(b"\n", 1)[0].decode("utf-8-sig"))), []
    )
    if not header:
        raise UploadError("the CSV has no header row")
    options = pcsv.ConvertOptions(
        column_types={name: pa.string() for name in header},
        strings_can_be_null=True,
    )
    return pcsv.read_csv(
        io.BytesIO(data), read_options=pcsv.ReadOptions(encoding="utf-8-sig"),
        convert_options=options,
    )  # fmt: skip


def looks_like(column: pa.ChunkedArray) -> str:
    """What a column's values look like: integer, number, time, text or empty."""
    if column.null_count == len(column):
        return "empty"
    if not (pa.types.is_string(column.type) or pa.types.is_large_string(column.type)):
        t = column.type
        if pa.types.is_integer(t):
            return "integer"
        if pa.types.is_floating(t) or pa.types.is_decimal(t):
            return "number"
        if pa.types.is_timestamp(t) or pa.types.is_date(t):
            return "time"
        return "text"
    for kind, target in (("integer", pa.int64()), ("number", pa.float64())):
        try:
            column.cast(target)
            return kind
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
            pass
    try:
        column.cast(pa.timestamp("us"))
        return "time"
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
        pass
    if all(_OFFSET.search(v.strip()) for v in column.to_pylist() if v):
        try:
            column.cast(pa.timestamp("us", tz="UTC"))
            return "time"
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
            pass
    return "text"


def _guess(names: list[str], table: pa.Table) -> dict[str, str | None]:
    lower = {n.lower(): n for n in names}
    mapping: dict[str, str | None] = {}
    taken: set[str] = set()
    for target in MAPPED:
        match = next(
            (
                lower[c]
                for c in CANDIDATES[target]
                if c in lower and lower[c] not in taken
            ),
            None,
        )
        mapping[target] = match
        if match:
            taken.add(match)
    if mapping["value"] is None:  # the only numeric column left
        numeric = [
            n for n in names
            if n not in taken and looks_like(table[n]) in ("integer", "number")
        ]  # fmt: skip
        if len(numeric) == 1:
            mapping["value"] = numeric[0]
    return mapping


def _has_zone(column: pa.ChunkedArray) -> bool | None:
    """Whether a time column's values carry a zone (None: can't tell)."""
    if pa.types.is_timestamp(column.type):
        return column.type.tz is not None
    if pa.types.is_string(column.type) or pa.types.is_large_string(column.type):
        values = [v for v in column.to_pylist() if v]
        if values:
            return all(_OFFSET.search(v.strip()) for v in values)
    return None


def _column_info(head: pa.Table, geometry: str | None = None) -> list[dict]:
    out = []
    for n in head.column_names:
        if n == geometry:
            sample = [None if v is None else _wkt(v) for v in head[n].to_pylist()[:5]]
            out.append({"name": n, "type": "geometry", "looks_like": "geometry",
                        "sample": sample})  # fmt: skip
            continue
        out.append(
            {
                "name": n,
                "type": str(head.schema.field(n).type),
                "looks_like": looks_like(head[n]),
                "sample": [
                    None if v is None else str(v) for v in head[n].to_pylist()[:5]
                ],
            }
        )
    return out


def _wkt(value) -> str:
    import shapely

    try:
        geom = (
            shapely.from_wkb(value)
            if isinstance(value, bytes)
            else shapely.from_wkt(value)
        )
    except shapely.errors.GEOSException:
        return "<not a geometry>"
    text = geom.wkt
    return text if len(text) <= 120 else text[:117] + "..."


def _sample(head: pa.Table, geometry: str | None) -> list[dict]:
    rows = json_rows(head.drop_columns([geometry]) if geometry else head)
    if geometry:
        for row, value in zip(rows, head[geometry].to_pylist(), strict=True):
            row[geometry] = None if value is None else _wkt(value)
    return rows[:SAMPLE_ROWS]


def _pick(names: list[str], candidates: tuple[str, ...], taken=()) -> str | None:
    lower = {n.lower(): n for n in names if n not in taken}
    return next((lower[c] for c in candidates if c in lower), None)


def attribute_name(column: str) -> str:
    """A valid attribute name for a file column (``Drainage Area`` ->
    ``Drainage_Area``)."""
    name = re.sub(r"[^A-Za-z0-9_]+", "_", column).strip("_") or "column"
    if name[0].isdigit():
        name = f"_{name}"
    return name[:128]


def infer_type(column: pa.ChunkedArray) -> str:
    """A dataset column type for a file column: Arrow's type for Parquet and geo
    files; for CSV text, int64 / float64 when every value parses (and none has a
    leading zero), else string."""
    t = column.type
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        # Codes with leading zeros (HUCs, site numbers) are text, not numbers.
        if any(_LEADING_ZERO.match(v) for v in column.to_pylist() if v):
            return "string"
        return {"integer": "int64", "number": "float64"}.get(
            looks_like(column), "string"
        )
    if pa.types.is_boolean(t):
        return "boolean"
    if pa.types.is_integer(t):
        return (
            "int32" if t.bit_width <= 32 and pa.types.is_signed_integer(t) else "int64"
        )
    if pa.types.is_floating(t):
        return "float32" if t.bit_width == 32 else "float64"
    if pa.types.is_date(t):
        return "date"
    if pa.types.is_timestamp(t):
        return "timestamptz" if t.tz else "timestamp"
    return "string"


def propose_attributes(
    head: pa.Table, columns: list[str], reserved: frozenset[str]
) -> list[dict]:
    """The file columns to keep, with valid, distinct names and types."""
    out, seen = [], set(reserved)
    for column in columns:
        name = attribute_name(column)
        while name.lower() in seen:
            name = f"{name}_"
        seen.add(name.lower())
        out.append({"column": column, "name": name, "type": infer_type(head[column])})
    return out


@dataclass
class Preview:
    columns: list[dict]
    sample: list[dict]
    rows: int | None
    mapping: dict
    dataset: dict
    needed: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    kind: str = "timeseries"
    extra: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        body = {
            "columns": self.columns,
            "sample": self.sample,
            "rows": self.rows,
            "mapping": self.mapping,
            "dataset": self.dataset,
            "needed": self.needed,
            "notes": self.notes,
            **self.extra,
        }
        if self.kind == "timeseries":
            body["ingest"] = ingest_body(self.dataset["name"], self.needed)
        else:
            body["ingest"] = _other_ingest_body(self.kind, self.dataset, self.needed)
        return body


_PLACEHOLDERS = {
    "mapping.timezone": ("mapping", "timezone", "<IANA zone of the times, e.g. UTC>"),
    "mapping.value_time": ("mapping", "value_time", "<column with the value times>"),
    "mapping.value": ("mapping", "value", "<column with the values>"),
}


def ingest_body(name: str, needed: list[str]) -> dict:
    """An ingest request to edit: the proposed name, and a ``<...>`` placeholder for
    everything the uploader still has to say (ingest refuses unreplaced ones)."""
    mapping: dict = {}
    for need in needed:
        if need in _PLACEHOLDERS:
            _, key, text = _PLACEHOLDERS[need]
            mapping[key] = text
        elif need.startswith("mapping.series_key"):
            mapping["series_key_value"] = "<one series key for every row>"
    body: dict = {
        "dataset": {
            "name": name,
            "unit": "<unit of the file's values (UCUM), e.g. m3/s or [cft_i]/s>",
            "role": "<observed | simulated | forecast>",
            "variable": "<e.g. streamflow; see GET /v1/domains/variables>",
            "statistic": "<inst | mean | sum | min | max>",
        }
    }
    if mapping:
        body["mapping"] = mapping
    return body


_OTHER_PLACEHOLDERS = {
    "mapping.location_key": ("location_key", "<column with location keys>"),
    "mapping.geometry": (
        "geometry",
        "<column with WKB/WKT geometry, or set longitude and latitude>",
    ),
    "mapping.crs": ("crs", "<the file's coordinate system, e.g. EPSG:26918>"),
    "mapping.left_key": ("left_key", "<column with the left series keys>"),
    "mapping.right_key": ("right_key", "<column with the right series keys>"),
}


def _other_ingest_body(kind: str, dataset: dict, needed: list[str]) -> dict:
    """An ingest request to edit, for a layer or crosswalk."""
    mapping = {
        key: text for need, (key, text) in _OTHER_PLACEHOLDERS.items() if need in needed
    }
    body: dict = {"dataset": {"name": dataset["name"]}}
    if kind == "crosswalk":
        body["dataset"] |= {
            "left": "<a timeseries dataset: <name> or public.<name>>",
            "right": "<a timeseries dataset: <name> or public.<name>>",
        }
    if mapping:
        body["mapping"] = mapping
    return body


LOCATION_KEYS = ("location_key", "location_id", "site_no", "site_id", "site",
                 "station_id", "station", "gage_id", "gauge_id", "feature_id",
                 "comid", "huc", "huc8", "huc10", "huc12", "id")  # fmt: skip
NAMES = ("name", "station_nm", "station_name", "site_name", "gage_name",
         "gauge_name", "location_name", "title")  # fmt: skip
GEOMETRIES = ("geometry", "geom", "wkt", "wkb", "wkb_geometry", "the_geom", "shape")
LONGITUDES = ("longitude", "lon", "long", "lng", "x", "dec_long_va")
LATITUDES = ("latitude", "lat", "y", "dec_lat_va")


def inspect_layer(
    head: pa.Table,
    rows: int | None,
    filename: str,
    *,
    geometry: str | None = None,
    crs: str | None = None,
    crs_known: bool = True,
    extra: dict | None = None,
) -> Preview:
    """What a location file holds, and a proposed mapping to a layer: key, name,
    geometry (or longitude/latitude), CRS and the rest as attributes. ``geometry``
    is the file's geometry column when its format says (geo files, GeoParquet)."""
    from src.dataset_types import LAYER_COLUMNS
    from src.geofiles import describe_crs, is_wgs84

    names = head.column_names
    geometry = geometry or _pick(names, GEOMETRIES)
    key = _pick(names, LOCATION_KEYS, {geometry})
    name = _pick(names, NAMES, {geometry, key})
    lon = lat = None
    if geometry is None:
        lon = _pick(names, LONGITUDES)
        lat = _pick(names, LATITUDES, {lon})
    used = {c for c in (geometry, key, name, lon, lat) if c}
    attributes = propose_attributes(
        head, [n for n in names if n not in used], LAYER_COLUMNS
    )
    needed, notes = [], []
    if key is None:
        needed.append("mapping.location_key")
    if geometry is None and not (lon and lat):
        needed.append("mapping.geometry")
    if lon and lat:
        crs = crs or "EPSG:4326"
        notes.append(f"points from '{lon}' and '{lat}', taken as EPSG:4326")
    elif crs is None and not crs_known:
        needed.append("mapping.crs")
        notes.append("the file doesn't say its coordinate system; set mapping.crs")
    if crs:
        crs = describe_crs(crs)
        if not is_wgs84(crs):
            notes.append(f"coordinates are in {crs}; reprojected to EPSG:4326 on load")
    if rows is None:
        notes.append("row count is known after loading (CSV)")
    mapping = {
        "location_key": key, "name": name, "geometry": geometry,
        "longitude": lon, "latitude": lat, "crs": crs,
        "source_layer": (extra or {}).get("layer"),
        "attributes": attributes,
    }  # fmt: skip
    return Preview(
        columns=_column_info(head, geometry),
        sample=_sample(head, geometry),
        rows=rows,
        mapping=mapping,
        dataset={"name": dataset_name(filename)},
        needed=needed,
        notes=notes,
        kind="layer",
        extra={"layers": (extra or {}).get("layers")} if extra else {},
    )


LEFT_KEYS = ("left_key", "primary_location_id", "primary", "left", "from",
             "source_id", "from_id")  # fmt: skip
RIGHT_KEYS = ("right_key", "secondary_location_id", "secondary", "right", "to",
              "target_id", "to_id")  # fmt: skip


def inspect_crosswalk(head: pa.Table, rows: int | None, filename: str) -> Preview:
    """What a crosswalk file holds: its left and right key columns and the rest
    as extra columns."""
    from src.dataset_types import CROSSWALK_COLUMNS

    names = head.column_names
    left = _pick(names, LEFT_KEYS)
    right = _pick(names, RIGHT_KEYS, {left})
    if left is None and right is None and len(names) == 2:
        left, right = names  # two columns: left then right
    used = {c for c in (left, right) if c}
    extras = propose_attributes(
        head, [n for n in names if n not in used], CROSSWALK_COLUMNS
    )
    needed = [f"mapping.{t}" for t, c in (("left_key", left), ("right_key", right))
              if c is None]  # fmt: skip
    needed += ["dataset.left", "dataset.right"]
    notes = ["row count is known after loading (CSV)"] if rows is None else []
    return Preview(
        columns=_column_info(head),
        sample=_sample(head, None),
        rows=rows,
        mapping={"left_key": left, "right_key": right, "attributes": extras},
        dataset={"name": dataset_name(filename)},
        needed=needed,
        notes=notes,
        kind="crosswalk",
    )


def inspect(head: pa.Table, rows: int | None, filename: str) -> Preview:
    """What the file holds, and a proposed mapping to a timeseries."""
    names = head.column_names
    columns = [
        {
            "name": n,
            "type": str(head.schema.field(n).type),
            "looks_like": looks_like(head[n]),
            "sample": [None if v is None else str(v) for v in head[n].to_pylist()[:5]],
        }
        for n in names
    ]
    mapping: dict = {**_guess(names, head), "series_key_value": None, "timezone": None}
    needed, notes = [], []
    if mapping["series_key"] is None:
        needed.append(
            "mapping.series_key (a column) or mapping.series_key_value (one series)"
        )
    for target in ("value_time", "value"):
        if mapping[target] is None:
            needed.append(f"mapping.{target}")
    for target in ("value_time", "reference_time"):
        column = mapping[target]
        if column and _has_zone(head[column]) is False:
            needed.append("mapping.timezone")
            notes.append(
                f"'{column}' times have no zone offset; set mapping.timezone "
                "(e.g. UTC or America/Denver)"
            )
            break
    if rows is None:
        notes.append("row count is known after loading (CSV)")
    needed += ["dataset.role", "dataset.variable", "dataset.unit", "dataset.statistic"]
    return Preview(
        columns=columns,
        sample=json_rows(head.slice(0, SAMPLE_ROWS)),
        rows=rows,
        mapping=mapping,
        dataset={"name": dataset_name(filename)},
        needed=needed,
        notes=notes,
    )
