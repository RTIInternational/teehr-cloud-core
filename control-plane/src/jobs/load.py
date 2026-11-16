"""Loading an upload into a dataset, in the job's pod, as the caller.

1. Download the staged file and map its columns to the timeseries columns.
2. Check the whole file against the dataset's columns (the same checks writes
   make: types, required values, time zones, repeated keys) before writing any of
   it; every problem found is reported.
3. Register the dataset if it's new.
4. Write through the control plane's own write path (``POST .../data``), as the
   caller, in Arrow chunks under the request size limit: units are converted,
   new series added and every chunk recorded in the dataset's write history
   there, exactly as for any other write. Each chunk is its own write: a load
   that fails partway keeps the chunks before it (upserts make a retry safe).
"""

import tempfile
import time
from dataclasses import dataclass
from typing import Any

import httpx
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import shapely

from src.dataset_types import crosswalk_columns, layer_columns, timeseries_columns
from src.geofiles import GEO_FORMATS, SUFFIX, WGS84, points, read_geo, to_wgs84
from src.geometry import prepare_layer
from src.tables import DataMismatch, conform, duplicate_keys
from src.uploads import read_csv_text

ARROW = "application/vnd.apache.arrow.stream"
CHUNK_BYTES = 48 * 1024 * 1024  # under the API's 64 MiB write limit
STRING_TARGETS = ("series_key", "member", "quality")


class LoadFailed(Exception):
    """The load can't go ahead; ``problems`` says why."""

    def __init__(self, problems: list[str]):
        super().__init__("; ".join(problems))
        self.problems = problems


@dataclass
class GrantClient:
    """The job's side of its grant: tokens, task, progress, outcome."""

    base_url: str
    grant: str
    http: httpx.Client | None = None

    def __post_init__(self):
        self.http = self.http or httpx.Client(timeout=120)
        self._token, self._expires = "", 0.0

    def _grant(self, method: str, path: str, **kwargs) -> httpx.Response:
        resp = self.http.request(
            method,
            f"{self.base_url}/v1/job-grants/{path}",
            headers={"Authorization": f"Bearer {self.grant}"},
            **kwargs,
        )
        resp.raise_for_status()
        return resp

    def download(self, url: str, file) -> None:
        """Copy the staged file into ``file``."""
        with self.http.stream("GET", url) as resp:
            if resp.status_code != 200:
                raise LoadFailed(
                    [f"couldn't read the staged file (HTTP {resp.status_code})"]
                )
            for part in resp.iter_bytes(1024 * 1024):
                file.write(part)

    def task(self) -> dict:
        return self._grant("GET", "task").json()

    def token(self, fresh: bool = False) -> str:
        if fresh or time.time() > self._expires - 60:
            body = self._grant("POST", "token").json()
            self._token, self._expires = (
                body["access_token"],
                time.time() + body["expires_in"],
            )
        return self._token

    def progress(self, rows_done: int) -> None:
        self._grant("POST", "progress", json={"rows_done": rows_done})

    def finish(self, status: str, **outcome) -> None:
        self._grant("POST", "finish", json={"status": status, **outcome})

    def api(self, method: str, path: str, **kwargs) -> httpx.Response:
        """Call the control plane's API as the caller (one retry with a fresh
        token if it's refused)."""
        extra = kwargs.pop("headers", {})
        for fresh in (False, True):
            headers = {**extra, "Authorization": f"Bearer {self.token(fresh)}"}
            resp = self.http.request(
                method, f"{self.base_url}/v1{path}", headers=headers, **kwargs
            )
            if resp.status_code != 401:
                return resp
        return resp


def _problem(resp: httpx.Response) -> str:
    try:
        body = resp.json()
    except ValueError:
        return f"HTTP {resp.status_code}"
    errors = body.get("errors") or []
    if errors:
        return "; ".join(f"{'.'.join(e['loc'][1:])}: {e['msg']}" for e in errors)
    return body.get("detail") or f"HTTP {resp.status_code}"


def read_file(path: str, fmt: str, layer: str | None = None) -> pa.Table:
    if fmt in GEO_FORMATS:
        return read_geo(fmt, path, layer)[0]
    if fmt == "parquet":
        return pq.read_table(path)
    with open(path, "rb") as f:
        return read_csv_text(f.read())


def apply_mapping(data: pa.Table, mapping: dict) -> pa.Table:
    """The file's columns under the timeseries column names; keys and codes as
    text (a numeric site number from Parquet becomes its digits)."""
    columns: dict[str, Any] = {}
    if mapping.get("series_key_value"):
        columns["series_key"] = pa.array(
            [mapping["series_key_value"]] * data.num_rows, pa.string()
        )
    for target in ("series_key", "reference_time", "value_time", "member",
                   "quantile", "value", "quality"):  # fmt: skip
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
    client: GrantClient, project: str, dataset: dict, pairs: pa.Table
) -> list[str]:
    """Write each series' location key to the dataset's series table (other
    series attributes are kept); returns the write's notes."""
    resp = client.api("GET", f"/projects/{project}/datasets/{dataset['name']}")
    if resp.status_code != 200:
        raise LoadFailed([f"dataset '{dataset['name']}': {_problem(resp)}"])
    series = resp.json()["timeseries"]["series_table"]
    resp = client.api(
        "POST",
        f"/projects/{project}/datasets/{series}/data",
        content=arrow_bytes(pairs),
        headers={"Content-Type": ARROW},
    )
    if resp.status_code != 201:
        raise LoadFailed([f"setting location keys: {_problem(resp)}"])
    return resp.json().get("notes") or []


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


def chunks(table: pa.Table):
    per_row = max(1, table.nbytes // max(1, table.num_rows))
    size = max(1, CHUNK_BYTES // per_row)
    for start in range(0, table.num_rows, size):
        yield table.slice(start, size)


def arrow_bytes(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def write(client: GrantClient, project: str, dataset: dict, typed: pa.Table) -> dict:
    path = f"/projects/{project}/datasets/{dataset['name']}/data"
    done = added = 0
    snapshot = None
    notes: list[str] = []
    params = {"mode": "upsert"}
    if dataset.get("unit"):
        params["unit"] = dataset["unit"]
    for chunk in chunks(typed):
        for attempt in range(4):
            resp = client.api(
                "POST",
                path,
                params=params,
                content=arrow_bytes(chunk),
                headers={"Content-Type": ARROW},
            )
            if resp.status_code != 409 or attempt == 3:
                break
            time.sleep(2**attempt)  # another write committed first
        if resp.status_code != 201:
            raise LoadFailed([f"after {done} rows, writing failed: {_problem(resp)}"])
        body = resp.json()
        done += chunk.num_rows
        added += body.get("series_added") or 0
        snapshot = body.get("snapshot_id")
        notes += [n for n in body.get("notes") or [] if n not in notes]
        client.progress(done)
    out = {"rows": done, "snapshot_id": snapshot}
    if dataset.get("kind", "timeseries") == "timeseries":
        out["series_added"] = added
    if notes:
        out["notes"] = notes[:20]
    return out


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


def load_other(client: GrantClient, project: str, upload: dict, data) -> dict:
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
    return {"dataset": dataset["name"], "created": created,
            **write(client, project, dataset, typed)}  # fmt: skip


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
    if dataset.get("kind", "timeseries") != "timeseries":
        return load_other(client, project, upload, data)
    mapped = apply_mapping(data, mapping)
    pairs = locations(data, mapping)
    typed = check(
        mapped, target_columns(client, project, dataset), mapping.get("timezone")
    )
    created = False
    if not dataset.get("existing"):
        register(client, project, dataset)
        created = True
    result = {
        "dataset": dataset["name"],
        "created": created,
        **write(client, project, dataset, typed),
    }
    if pairs is not None:
        notes = set_locations(client, project, dataset, pairs)
        result["locations_set"] = pairs.num_rows
        if notes:
            result["notes"] = notes
    return result
