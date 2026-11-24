"""Loading an upload into a dataset, in the job's pod, as the caller.

1. Download the staged file and map its columns to the timeseries columns.
2. Check the whole file against the dataset's columns (the same checks writes
   make: types, required values, time zones, repeated keys) before writing any of
   it; every problem found is reported.
3. Register the dataset if it's new.
4. Write it in one commit, here, as the caller, with the write path's own code
   (ADR-0016): units converted, new series added to the series table, the rows
   upserted. The commit is reported to the control plane, which records it in the
   dataset's write history as for any other write. A load that fails leaves the
   dataset as it was (new series apart). Each series' location, when the file
   gives one, is then set in the series table, the same way.
"""

import logging
import tempfile
import time
from types import SimpleNamespace
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import shapely

from src.dataset_types import crosswalk_columns, layer_columns, timeseries_columns
from src.domains import convert
from src.geofiles import GEO_FORMATS, SUFFIX, WGS84, points, read_geo, to_wgs84
from src.geometry import prepare_layer
from src.jobs.grant import GrantClient, LoadFailed, _problem
from src.jobs.writes import as_caller, open_tables, report
from src.notes import missing_note
from src.refs import split
from src.tables import DataMismatch, Tables, conform, duplicate_keys
from src.uploads import read_csv_text

__all__ = ["GrantClient", "LoadFailed", "load_upload"]

log = logging.getLogger("src.jobs")

STRING_TARGETS = ("series_key", "member", "approval_status", "qualifier")


def read_file(path: str, fmt: str, layer: str | None = None) -> pa.Table:
    if fmt in GEO_FORMATS:
        return read_geo(fmt, path, layer)[0]
    if fmt == "parquet":
        return pq.read_table(path)
    with open(path, "rb") as f:
        return read_csv_text(f.read())


def parse_times(data: pa.Table, mapping: dict) -> pa.Table:
    """Times stored as text in ``mapping.time_format`` (strptime), parsed; with
    ``%z`` they're UTC, without it they're local times for ``mapping.timezone``."""
    fmt = mapping.get("time_format")
    if not fmt:
        return data
    for target in ("value_time", "reference_time"):
        source = mapping.get(target)
        if not source:
            continue
        column = data[source]
        if not (
            pa.types.is_string(column.type) or pa.types.is_large_string(column.type)
        ):
            continue
        try:
            parsed = pc.strptime(column, format=fmt, unit="us")
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as exc:
            raise LoadFailed([f"column '{source}': {exc} (time_format {fmt})"]) from exc
        data = data.set_column(data.column_names.index(source), source, parsed)
    return data


def unpivot(data: pa.Table, mapping: dict) -> pa.Table:
    """A wide file as long rows: one row per (row, value column), the column's
    header as the series key. Empty cells are missing values (null)."""
    series = mapping["series_columns"]
    columns = [data[c] for c in series]
    if any(
        pa.types.is_string(c.type) or pa.types.is_large_string(c.type) for c in columns
    ):
        target = pa.string()  # CSV text: typed later, like any value column
    else:
        target = pa.float64()
    shared = [t for t in ("reference_time", "value_time", "member", "quantile",
                          "approval_status", "qualifier") if mapping.get(t)]  # fmt: skip
    parts = []
    for name, column in zip(series, columns, strict=True):
        part = {"series_key": pa.array([name] * data.num_rows, pa.string())}
        for target_name in shared:
            part[target_name] = data[mapping[target_name]]
        part["value"] = column.cast(target)
        parts.append(pa.table(part))
    long = pa.concat_tables(parts)
    for name in ("member", "approval_status", "qualifier"):
        if name in long.column_names and not pa.types.is_string(long[name].type):
            long = long.set_column(
                long.column_names.index(name), name, long[name].cast(pa.string())
            )
    return long


def apply_mapping(data: pa.Table, mapping: dict) -> pa.Table:
    """The file's columns under the timeseries column names; keys and codes as
    text (a numeric site number from Parquet becomes its digits)."""
    columns: dict[str, Any] = {}
    if mapping.get("series_key_value"):
        columns["series_key"] = pa.array(
            [mapping["series_key_value"]] * data.num_rows, pa.string()
        )
    for target in ("series_key", "reference_time", "value_time", "member",
                   "quantile", "value", "approval_status", "qualifier"):  # fmt: skip
        source = mapping.get(target)
        if not source or target in columns:
            continue
        column = data[source]
        if target in STRING_TARGETS and not pa.types.is_string(column.type):
            column = column.cast(pa.string())
        columns[target] = column
    return pa.table(columns)


def locations(data: pa.Table, mapping: dict) -> pa.Table | None:
    """(series_key, location_key) for each series in the file, when the mapping
    names a location key column; one location per series."""
    source = mapping.get("location_key")
    if not source:
        return None
    keys = (
        pa.array([mapping["series_key_value"]] * data.num_rows, pa.string())
        if mapping.get("series_key_value")
        else data[mapping["series_key"]].cast(pa.string())
    )
    pairs = (
        pa.table({"series_key": keys, "location_key": data[source].cast(pa.string())})
        .group_by(["series_key", "location_key"])
        .aggregate([])
    )
    if pc.sum(pc.is_null(pairs["location_key"])).as_py():
        raise LoadFailed(["some rows have no location key"])
    if duplicate_keys(pairs, ["series_key"]):
        raise LoadFailed(["a series has more than one location key in the file"])
    return pairs


def set_locations(
    client: GrantClient, tables: Tables, project: str, dataset: dict, pairs: pa.Table
) -> list[str]:
    """Write each series' location key to the dataset's series table, as the
    caller (other series attributes are kept); returns notes on location keys that
    aren't in the series' layer."""
    ts = _described(client, project, dataset["name"])["timeseries"]
    series = _described(client, project, ts["series_table"])
    name = series["name"]
    provided = list(pairs.column_names)
    try:
        data = conform(pairs, series["columns"])
    except DataMismatch as exc:
        raise LoadFailed([f"setting location keys: {exc}"]) from None
    notes = _location_notes(client, tables, project, name, pairs)
    result = as_caller(
        client, name,
        lambda token: tables.merge(token, project, name, data,
                                   series["key_columns"], provided),
    )  # fmt: skip
    if result.snapshot_id is not None:
        report(client, name, "upsert", None, data.num_rows, result.snapshot_id,
               notes=notes or None)  # fmt: skip
    return notes


def _location_notes(
    client: GrantClient, tables: Tables, project: str, series: str, pairs: pa.Table
) -> list[str]:
    """Location keys not in the layer of the timeseries using this series table
    (when they share one), as writes to it report them."""
    resp = client.api("GET", f"/projects/{project}/datasets")
    if resp.status_code != 200:
        raise LoadFailed([f"datasets: {_problem(resp)}"])
    layers = {
        d["timeseries"].get("location_layer")
        for d in resp.json()
        if d.get("timeseries") and d["timeseries"].get("series_table") == series
    } - {None}
    if len(layers) != 1:
        return []
    ref = layers.pop()
    layer = _table_of(client, project, ref)
    if layer is None or layer["kind"] != "layer":
        raise LoadFailed([f"'{ref}' is not a layer"])
    wanted = sorted(set(pairs["location_key"].to_pylist()) - {None})
    missing = _missing_keys(client, tables, layer, "location_key", wanted)
    return [missing_note(missing, len(wanted), "location keys", ref)] if missing else []


def _table_of(client: GrantClient, project: str, ref: str) -> dict | None:
    """The dataset a reference names (``<name>`` or ``public.<name>``), or None."""
    owner, name = split(ref)
    resp = client.api("GET", f"/projects/{owner or project}/datasets/{name}")
    if resp.status_code == 404:
        return None
    if resp.status_code != 200:
        raise LoadFailed([f"dataset '{ref}': {_problem(resp)}"])
    return resp.json()


def _missing_keys(
    client: GrantClient, tables: Tables, dataset: dict, column: str, values: list
) -> list[str]:
    """Which of ``values`` aren't in the dataset's ``column``, read as the caller."""
    if not values:
        return []
    namespace, name = dataset["table"].split(".", 1)
    present = as_caller(
        client, name,
        lambda token: tables.existing_keys(token, namespace, name, column, values),
    )  # fmt: skip
    return sorted(set(values) - present)


def target_columns(client: GrantClient, project: str, dataset: dict) -> list[dict]:
    """The dataset's columns: as registered, or as a new one will be."""
    if dataset.get("existing"):
        resp = client.api("GET", f"/projects/{project}/datasets/{dataset['name']}")
        if resp.status_code != 200:
            raise LoadFailed([f"dataset '{dataset['name']}': {_problem(resp)}"])
        return resp.json()["columns"]
    period = dataset.get("period") or (
        "instant" if dataset.get("statistic") == "inst" else None
    )
    resp = client.api("GET", "/domains/periods", params={"project": project})
    forms = (
        {p["name"]: p["form"] for p in resp.json()} if resp.status_code == 200 else {}
    )
    if period not in forms:
        raise LoadFailed([f"dataset.period: unknown or missing period '{period}'"])
    columns, _ = timeseries_columns(
        dataset.get("optional_columns", []), forms[period], []
    )
    return columns


def check(data: pa.Table, columns: list[dict], timezone: str | None) -> pa.Table:
    """The whole file, checked and typed as the dataset's columns."""
    problems = []
    try:
        typed = conform(data, columns, timezone)
    except DataMismatch as exc:
        raise LoadFailed([str(exc)]) from exc
    key = [c for c in ("series_key", "reference_time", "value_time", "member",
                       "quantile") if c in typed.column_names]  # fmt: skip
    repeated = duplicate_keys(typed, key)
    if repeated:
        problems.append(
            f"{repeated} key values ({', '.join(key)}) occur more than once in the file"
        )
    blank = pc.sum(pc.equal(typed["series_key"], "")).as_py() or 0
    if blank:
        problems.append(f"{blank} rows have an empty series key")
    if problems:
        raise LoadFailed(problems)
    return typed


def register(client: GrantClient, project: str, dataset: dict) -> None:
    fields = ("name", "role", "variable", "statistic", "period", "optional_columns",
              "location_layer", "time_partition", "description", "source")  # fmt: skip
    body = {"type": "timeseries", **{k: dataset[k] for k in fields if dataset.get(k)}}
    resp = client.api("POST", f"/projects/{project}/datasets", json=body)
    if resp.status_code != 201:
        raise LoadFailed([f"registering '{dataset['name']}': {_problem(resp)}"])


def _described(client: GrantClient, project: str, name: str) -> dict:
    resp = client.api("GET", f"/projects/{project}/datasets/{name}")
    if resp.status_code != 200:
        raise LoadFailed([f"dataset '{name}': {_problem(resp)}"])
    return resp.json()


def _units(client: GrantClient, project: str, given: str, canonical: str):
    """(the unit values arrive in, the dataset's unit), checked as writes check
    them; a project's unit wins over a public one of the same symbol."""
    resp = client.api("GET", "/domains/units", params={"project": project})
    if resp.status_code != 200:
        raise LoadFailed([f"units: {_problem(resp)}"])
    found = {
        u["symbol"]: SimpleNamespace(**u)
        for u in sorted(resp.json(), key=lambda u: u.get("project") is not None)
    }
    source, target = found.get(given), found.get(canonical)
    if source is None or target is None:
        raise LoadFailed([f"unknown unit '{given if source is None else canonical}'"])
    if source.dimension != target.dimension:
        raise LoadFailed([f"{source.symbol} ({source.dimension}) doesn't convert to "
                          f"{target.symbol} ({target.dimension})"])  # fmt: skip
    return source, target


def write_timeseries(
    client: GrantClient,
    tables: Tables,
    project: str,
    name: str,
    typed: pa.Table,
    unit: str | None,
) -> dict:
    """The rows, in the dataset's unit, upserted in one commit after their new
    series are added to the series table; then reported."""
    ts = _described(client, project, name)["timeseries"]
    source, target = _units(client, project, unit or ts["unit"], ts["unit"])
    data = typed
    if source.symbol != target.symbol:
        i = data.schema.get_field_index("value")
        data = data.set_column(
            i, data.schema.field(i), convert(data["value"], source, target)
        )
    series = _described(client, project, ts["series_table"])
    keys = pc.unique(data["series_key"])
    rows: dict[str, Any] = {"series_key": keys}
    if ts.get("location_layer"):
        rows["location_layer"] = pa.array(
            [ts["location_layer"]] * len(keys), pa.string()
        )
    try:
        new = conform(pa.table(rows), series["columns"])
    except DataMismatch as exc:
        raise LoadFailed([f"adding series: {exc}"]) from None
    added = as_caller(
        client, series["name"],
        lambda token: tables.insert_missing(token, project, series["name"], new),
    )  # fmt: skip
    out = _commit(client, tables, project, name, data)
    stamps = pc.strftime(data["value_time"], format="%Y-%m", locale="C")
    months = sorted(m for m in pc.unique(stamps).to_pylist() if m)
    if out["snapshot_id"] is not None:
        report(client, name, "upsert", None, data.num_rows, out["snapshot_id"],
               months=months, unit=source.symbol, series_added=added)  # fmt: skip
    return {**out, "series_added": added}


def write_layer(
    client: GrantClient, tables: Tables, project: str, name: str, typed: pa.Table
) -> dict:
    out = _commit(client, tables, project, name, typed)
    if out["snapshot_id"] is not None:
        report(client, name, "upsert", None, typed.num_rows, out["snapshot_id"])
    return out


def _commit(
    client: GrantClient, tables: Tables, project: str, name: str, data: pa.Table
) -> dict:
    started = time.monotonic()
    result = as_caller(
        client, name, lambda token: tables.write(token, project, name, data, "upsert")
    )
    client.progress(data.num_rows)
    log.info("wrote %s: %d rows, snapshot %s (%.1fs)", name, data.num_rows,
             result.snapshot_id, time.monotonic() - started)  # fmt: skip
    return {"rows": data.num_rows, "snapshot_id": result.snapshot_id}


def write_crosswalk(
    client: GrantClient, tables: Tables, project: str, name: str, typed: pa.Table
) -> dict:
    """The pairs, upserted in one commit; notes on keys not (yet) in their side's
    series table (reported, not refused: timeseries and their crosswalk can be
    loaded in either order)."""
    crosswalk = _described(client, project, name)["crosswalk"]
    notes = []
    for side in ("left", "right"):
        ref = crosswalk[f"{side}_series_table"]
        keys = pc.unique(typed[f"{side}_key"]).to_pylist()
        found = _table_of(client, project, ref)
        if found is None:
            notes.append(f"{side} series table '{ref}' no longer exists")
            continue
        missing = _missing_keys(client, tables, found, "series_key", keys)
        if missing:
            notes.append(missing_note(missing, len(keys), f"{side}_key values", ref))
    out = _commit(client, tables, project, name, typed)
    if out["snapshot_id"] is not None:
        report(client, name, "upsert", None, typed.num_rows, out["snapshot_id"],
               notes=notes or None)  # fmt: skip
    return {**out, "notes": notes} if notes else out


def layer_table(data: pa.Table, mapping: dict, fmt: str) -> pa.Table:
    """The file as a layer: key and name as text, geometry as WKB in EPSG:4326
    (from a geometry column or longitude/latitude), attributes renamed."""
    columns: dict[str, Any] = {
        "location_key": data[mapping["location_key"]].cast(pa.string())
    }
    if mapping.get("name"):
        columns["name"] = data[mapping["name"]].cast(pa.string())
    if mapping.get("geometry"):
        geometry = data[mapping["geometry"]]
        if pa.types.is_string(geometry.type) or pa.types.is_large_string(geometry.type):
            parsed = shapely.from_wkt(
                np.asarray(geometry.to_pylist(), dtype=object), on_invalid="ignore"
            )
            geometry = pa.array(shapely.to_wkb(parsed).tolist(), pa.binary())
        crs = mapping.get("crs") or WGS84
        columns["geometry"] = to_wgs84(geometry, crs)
    else:
        columns["geometry"] = points(
            data[mapping["longitude"]], data[mapping["latitude"]]
        )
    for attribute in mapping.get("attributes") or []:
        columns[attribute["name"]] = data[attribute["column"]]
    return pa.table(columns)


def check_layer(table: pa.Table, columns: list[dict]) -> pa.Table:
    """The whole layer, checked (geometry, types, repeated keys) and typed."""
    try:
        typed = conform(prepare_layer(table), columns)
    except DataMismatch as exc:
        raise LoadFailed([str(exc)]) from exc
    repeated = duplicate_keys(typed, ["location_key"])
    if repeated:
        raise LoadFailed([f"{repeated} location keys occur more than once in the file"])
    return typed


def crosswalk_table(data: pa.Table, mapping: dict) -> pa.Table:
    columns: dict[str, Any] = {
        "left_key": data[mapping["left_key"]].cast(pa.string()),
        "right_key": data[mapping["right_key"]].cast(pa.string()),
    }
    for extra in mapping.get("attributes") or []:
        columns[extra["name"]] = data[extra["column"]]
    return pa.table(columns)


def check_crosswalk(table: pa.Table, columns: list[dict]) -> pa.Table:
    try:
        typed = conform(table, columns)
    except DataMismatch as exc:
        raise LoadFailed([str(exc)]) from exc
    repeated = duplicate_keys(typed, ["left_key", "right_key"])
    if repeated:
        raise LoadFailed(
            [f"{repeated} (left_key, right_key) pairs occur more than once in the file"]
        )
    return typed


def _declared(mapping: dict) -> list[dict]:
    return [
        {"name": a["name"], "type": a["type"], "required": False}
        for a in mapping.get("attributes") or []
    ]


def existing_columns(client: GrantClient, project: str, dataset: dict) -> list[dict]:
    resp = client.api("GET", f"/projects/{project}/datasets/{dataset['name']}")
    if resp.status_code != 200:
        raise LoadFailed([f"dataset '{dataset['name']}': {_problem(resp)}"])
    return resp.json()["columns"]


def register_other(client: GrantClient, project: str, dataset: dict, mapping: dict):
    """Register a new layer or crosswalk with the file's attribute columns."""
    body: dict = {"type": dataset["kind"], "name": dataset["name"]}
    for key in ("description", "source", "left", "right"):
        if dataset.get(key):
            body[key] = dataset[key]
    extras = [{"name": a["name"], "type": a["type"]} for a in mapping["attributes"]]
    body["attributes" if dataset["kind"] == "layer" else "extra_columns"] = extras
    resp = client.api("POST", f"/projects/{project}/datasets", json=body)
    if resp.status_code != 201:
        raise LoadFailed([f"registering '{dataset['name']}': {_problem(resp)}"])


def load_other(
    client: GrantClient, tables: Tables, project: str, upload: dict, data
) -> dict:
    """Load a layer or crosswalk upload."""
    mapping, dataset = upload["mapping"], upload["dataset"]
    kind = dataset["kind"]
    if dataset.get("existing"):
        columns = existing_columns(client, project, dataset)
    elif kind == "layer":
        columns = layer_columns(_declared(mapping))
    else:
        columns = crosswalk_columns(_declared(mapping))
    if kind == "layer":
        typed = check_layer(layer_table(data, mapping, upload["format"]), columns)
    else:
        typed = check_crosswalk(crosswalk_table(data, mapping), columns)
    created = not dataset.get("existing")
    if created:
        register_other(client, project, dataset, mapping)
    written = (
        write_layer(client, tables, project, dataset["name"], typed)
        if kind == "layer"
        else write_crosswalk(client, tables, project, dataset["name"], typed)
    )
    return {"dataset": dataset["name"], "created": created, **written}


def load_upload(client: GrantClient, task: dict) -> dict:
    upload, project = task["upload"], task["project"]
    mapping, dataset = upload["mapping"], upload["dataset"]
    suffix = SUFFIX.get(upload["format"], "")
    with tempfile.NamedTemporaryFile(suffix=suffix) as tmp:
        client.download(upload["download_url"], tmp)
        tmp.flush()
        try:
            data = read_file(tmp.name, upload["format"], mapping.get("source_layer"))
        except (pa.ArrowInvalid, OSError, ValueError) as exc:
            raise LoadFailed([f"can't read the file: {exc}"]) from exc
    log.info("read %s: %d rows, %d columns", upload["filename"], data.num_rows,
             data.num_columns)  # fmt: skip
    tables = open_tables(task)
    if dataset.get("kind", "timeseries") != "timeseries":
        return load_other(client, tables, project, upload, data)
    data = parse_times(data, mapping)
    mapped = (
        unpivot(data, mapping)
        if mapping.get("layout") == "wide"
        else apply_mapping(data, mapping)
    )
    pairs = locations(data, mapping)
    typed = check(
        mapped, target_columns(client, project, dataset), mapping.get("timezone")
    )
    log.info("checked %d rows for %s: no problems", typed.num_rows, dataset["name"])
    created = False
    if not dataset.get("existing"):
        register(client, project, dataset)
        created = True
        log.info("registered %s", dataset["name"])
    result = {
        "dataset": dataset["name"],
        "created": created,
        **write_timeseries(
            client, tables, project, dataset["name"], typed, dataset.get("unit")
        ),
    }
    if pairs is not None:
        notes = set_locations(client, tables, project, dataset, pairs)
        result["locations_set"] = pairs.num_rows
        log.info("set %d series' locations", pairs.num_rows)
        if notes:
            result["notes"] = notes
    return result
