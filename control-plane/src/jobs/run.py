"""Running an evaluation version, in the job's pod, as the caller (Phase 5).

1. Read every dataset the run's manifest names (and each timeseries' series table)
   through the control plane's read API as the caller, in Arrow pages pinned to
   one snapshot each. A timeseries input's spec ``filter`` goes with the read, so
   only the rows it keeps leave the warehouse (the engine applies it again).
2. Build teehr-core's engine catalog from the manifest and the rows
   (``evaluations.catalog_from``) and run the spec.
3. Add lineage to the results (``run_id``, ``spec_version``, ``engine_version``),
   have the results table created (``results_<slug>_v<version>``, job grant) and
   write the rows through the write API as the caller: the first chunk replaces
   an earlier run's rows, the rest append.

The inputs' snapshot ids are the run's result, with the rows written, so a run
says exactly what it read.
"""

from typing import Any

import httpx
import pyarrow as pa
import pyarrow.compute as pc
from teehr_core import __version__ as ENGINE_VERSION
from teehr_core.engine import ContractError, PlanError, evaluate

from src.evaluations import SpecProblems, catalog_from
from src.jobs.load import ARROW, GrantClient, LoadFailed, _problem, arrow_bytes, chunks

PAGE_ROWS = 1_000_000  # the API's largest binary page


def read_all(
    client: GrantClient, project: str, name: str, row_filter: str | None = None
) -> tuple[pa.Table, int | None]:
    """A dataset's rows (those ``row_filter`` keeps), page by page from one
    snapshot; and that snapshot's id."""
    parts, cursor, snapshot = [], None, None
    while True:
        params: dict[str, Any] = {"limit": PAGE_ROWS}
        if row_filter:
            params["filter"] = row_filter
        if cursor:
            params["cursor"] = cursor
        resp = client.api(
            "GET", f"/projects/{project}/datasets/{name}/data",
            params=params, headers={"Accept": ARROW},
        )  # fmt: skip
        if resp.status_code != 200:
            raise LoadFailed([f"reading {project}.{name}: {_problem(resp)}"])
        parts.append(pa.ipc.open_stream(resp.content).read_all())
        snapshot = snapshot or resp.headers.get("x-snapshot-id")
        cursor = resp.headers.get("x-next-cursor")
        if not cursor:
            break
    table = pa.concat_tables(parts) if len(parts) > 1 else parts[0]
    return table, int(snapshot) if snapshot else None


def _input_filters(spec: dict) -> dict[str, str]:
    """Each timeseries input's filter, by the name the spec gives the dataset."""
    inputs = spec["inputs"]
    secondary = inputs["secondary"]
    sides = [
        inputs["primary"],
        *(secondary if isinstance(secondary, list) else [secondary]),
    ]
    return {
        s["dataset"]: s["filter"] for s in sides if s.get("dataset") and s.get("filter")
    }


def read_inputs(
    client: GrantClient, manifest: dict, spec: dict
) -> tuple[dict[str, pa.Table], dict[str, pa.Table], dict[str, int | None]]:
    """The rows of every dataset in the manifest, their series tables', and the
    snapshot each was read at (keyed ``<project>.<name>``)."""
    filters = _input_filters(spec)
    rows, series, snapshots = {}, {}, {}
    for ref, d in manifest["datasets"].items():
        rows[ref], snapshots[f"{d['project']}.{d['name']}"] = read_all(
            client, d["project"], d["name"], filters.get(ref)
        )
        if d.get("series"):
            s = d["series"]
            series[ref], snapshots[f"{s['project']}.{s['name']}"] = read_all(
                client, s["project"], s["name"]
            )
    return rows, series, snapshots


def _column_type(t: pa.DataType) -> tuple[str, pa.DataType]:
    """The dataset column type for an Arrow type, and the Arrow type to cast to."""
    if pa.types.is_dictionary(t):
        t = t.value_type
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return "string", pa.string()
    if pa.types.is_boolean(t):
        return "boolean", pa.bool_()
    if pa.types.is_integer(t):
        small = t.bit_width < 32 or (
            t.bit_width == 32 and pa.types.is_signed_integer(t)
        )
        return ("int32", pa.int32()) if small else ("int64", pa.int64())
    if pa.types.is_float16(t) or pa.types.is_float32(t):
        return "float32", pa.float32()
    if pa.types.is_floating(t) or pa.types.is_null(t) or pa.types.is_decimal(t):
        return "float64", pa.float64()
    if pa.types.is_timestamp(t):
        if t.tz:
            return "timestamptz", pa.timestamp("us", tz="UTC")
        return "timestamp", pa.timestamp("us")
    if pa.types.is_date(t):
        return "date", pa.date32()
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        return "binary", pa.binary()
    raise LoadFailed([f"results have a column of type {t}, which datasets can't hold"])


def results_table(table: pa.Table, run: dict) -> tuple[pa.Table, list[dict]]:
    """The results with lineage, typed as dataset columns, and those columns."""
    n = table.num_rows
    table = table.append_column("run_id", pa.array([run["id"]] * n, pa.string()))
    table = table.append_column(
        "spec_version", pa.array([run["version"]] * n, pa.int32())
    )
    table = table.append_column(
        "engine_version", pa.array([f"teehr-core {ENGINE_VERSION}"] * n, pa.string())
    )
    columns, arrays = [], []
    for field in table.schema:
        name, arrow = _column_type(field.type)
        columns.append({"name": field.name, "type": name, "required": False})
        arrays.append(pc.cast(table[field.name], arrow))
    return pa.Table.from_arrays(arrays, names=table.column_names), columns


def write_results(
    client: GrantClient, project: str, name: str, table: pa.Table
) -> int | None:
    """Replace the results table's rows with ``table`` (chunked); the last snapshot."""
    path = f"/projects/{project}/datasets/{name}/data"
    snapshot = None
    pieces = list(chunks(table)) if table.num_rows else [table]
    for i, chunk in enumerate(pieces):
        if chunk.num_rows == 0:
            break  # nothing to write; an earlier run's rows stay
        mode = "overwrite" if i == 0 else "append"
        resp = client.api(
            "POST", path, params={"mode": mode}, content=arrow_bytes(chunk),
            headers={"Content-Type": ARROW},
        )  # fmt: skip
        if resp.status_code != 201:
            raise LoadFailed([f"writing the results: {_problem(resp)}"])
        snapshot = resp.json().get("snapshot_id")
    return snapshot


def run_evaluation(client: GrantClient, task: dict) -> dict:
    """Do a run's task; returns its result (raises LoadFailed with what went wrong)."""
    run, project = task["run"], task["project"]
    manifest, spec = run["manifest"], run["spec"]
    rows, series, snapshots = read_inputs(client, manifest, spec)
    try:
        catalog = catalog_from(manifest, rows, series)
        result = evaluate(spec, catalog)
    except (SpecProblems, PlanError, ContractError, ValueError) as exc:
        raise LoadFailed([str(exc)]) from None
    table, columns = results_table(result.table, run)
    try:
        resp = client._grant("POST", "results-table", json={"columns": columns})
    except httpx.HTTPStatusError as exc:
        raise LoadFailed([f"the results table: {_problem(exc.response)}"]) from None
    dataset = resp.json()["dataset"]
    snapshot = write_results(client, project, dataset, table)
    return {
        "dataset": dataset,
        "rows": table.num_rows,
        "snapshot_id": snapshot,
        "inputs": snapshots,
        "engine_version": f"teehr-core {ENGINE_VERSION}",
    }
