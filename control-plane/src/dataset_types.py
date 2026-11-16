"""Dataset types (warehouse data model v1): what each type's table looks like.

The one place the typed shapes are defined; moves to the shared package with
the spec (ADR-0011) when there is one.
"""

TABLE = "table"  # untyped: a test path, not part of the model
TIMESERIES = "timeseries"
SERIES = "series"  # a timeseries' series table, created with it
LAYER = "layer"  # locations: geometry + attributes
CROSSWALK = "crosswalk"  # links between two series tables' series
TYPED = (TIMESERIES, SERIES, LAYER, CROSSWALK)

BBOX = ("bbox_xmin", "bbox_ymin", "bbox_xmax", "bbox_ymax")
LAYER_COLUMNS = frozenset({"location_key", "name", "geometry", *BBOX})
CROSSWALK_COLUMNS = frozenset({"left_key", "right_key"})


def _col(name: str, type_: str, required: bool, doc: str) -> dict:
    return {"name": name, "type": type_, "required": required, "doc": doc}


def timeseries_columns(
    optional: list[str], period_form: str, extra: list[dict]
) -> tuple[list[dict], list[str]]:
    """A timeseries dataset's columns and key (warehouse data model v1)."""
    on = set(optional)
    columns = [_col("series_key", "string", True, "The series a value belongs to")]
    if "reference_time" in on:
        columns.append(_col("reference_time", "timestamptz", True, "Issue time"))
    columns.append(_col("value_time", "timestamptz", True, "When the value applies"))
    if "member" in on:
        columns.append(_col("member", "string", True, "Ensemble member"))
    if "quantile" in on:
        columns.append(_col("quantile", "decimal(5,4)", True, "Quantile, 0 to 1"))
    columns.append(_col("value", "float64", False, "Null: a missing value"))
    if "quality" in on:
        columns.append(_col("quality", "string", False, "The source's code(s)"))
    if period_form == "absolute":
        columns.append(_col("period_start", "timestamptz", True, "Window start"))
        columns.append(_col("period_end", "timestamptz", True, "Window end"))
    columns.extend(extra)
    key = [
        c
        for c in ("series_key", "reference_time", "value_time", "member", "quantile")
        if any(col["name"] == c for col in columns)
    ]
    return columns, key


def series_columns(attributes: list[dict]) -> list[dict]:
    """A series table's columns: one row per series_key (ADR-0010)."""
    return [
        _col("series_key", "string", True, "Unique series id"),
        _col("location_key", "string", False, "Where the series is"),
        _col("location_layer", "string", False, "Layer of location_key"),
        _col("period", "string", False, "Overrides the dataset's period"),
        _col("resolution", "string", False, "How often values recur (ISO 8601)"),
        *attributes,
    ]


def layer_columns(attributes: list[dict]) -> list[dict]:
    """A layer's columns (warehouse data model v1; layers-and-crosswalks.md):
    geometry as WKB in EPSG:4326, its bounds for file pruning, then attributes."""
    return [
        _col("location_key", "string", True, "Unique location id"),
        _col("name", "string", False, "Display name"),
        _col("geometry", "binary", True, "WKB, EPSG:4326 (longitude, latitude)"),
        _col("bbox_xmin", "float64", True, "Westmost longitude"),
        _col("bbox_ymin", "float64", True, "Southmost latitude"),
        _col("bbox_xmax", "float64", True, "Eastmost longitude"),
        _col("bbox_ymax", "float64", True, "Northmost latitude"),
        *attributes,
    ]


def crosswalk_columns(extra: list[dict]) -> list[dict]:
    """A crosswalk's columns: a left series key, a right one, then extras."""
    return [
        _col("left_key", "string", True, "A series key of the left series table"),
        _col("right_key", "string", True, "A series key of the right series table"),
        *extra,
    ]
