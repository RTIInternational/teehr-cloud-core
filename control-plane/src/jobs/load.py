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
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from src.dataset_types import timeseries_columns
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


def read_file(path: str, fmt: str) -> pa.Table:
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
    for chunk in chunks(typed):
        for attempt in range(4):
            resp = client.api(
                "POST",
                path,
                params={"mode": "upsert", "unit": dataset["unit"]},
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
        client.progress(done)
    return {"rows": done, "series_added": added, "snapshot_id": snapshot}


def load_upload(client: GrantClient, task: dict) -> dict:
    upload, project = task["upload"], task["project"]
    mapping, dataset = upload["mapping"], upload["dataset"]
    with tempfile.NamedTemporaryFile() as tmp:
        client.download(upload["download_url"], tmp)
        tmp.flush()
        try:
            data = read_file(tmp.name, upload["format"])
        except (pa.ArrowInvalid, OSError, ValueError) as exc:
            raise LoadFailed([f"can't read the file: {exc}"]) from exc
    mapped = apply_mapping(data, mapping)
    typed = check(
        mapped, target_columns(client, project, dataset), mapping.get("timezone")
    )
    created = False
    if not dataset.get("existing"):
        register(client, project, dataset)
        created = True
    return {
        "dataset": dataset["name"],
        "created": created,
        **write(client, project, dataset, typed),
    }
