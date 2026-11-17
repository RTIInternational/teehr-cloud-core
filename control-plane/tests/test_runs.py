"""Runs of a saved evaluation (Phase 5): started as a job acting as the caller,
which reads the inputs through the API, runs the spec with teehr-core's engine and
writes results_<slug>_v<version> through the API."""

from datetime import UTC, datetime, timedelta

import numpy as np
import pyarrow as pa
import pytest
from pyiceberg.expressions.visitors import bind
from pyiceberg.io.pyarrow import expression_to_pyarrow

from src.jobs.load import GrantClient
from src.jobs.run import run_evaluation
from src.tables import ReadResult, iceberg_schema
from tests.test_datasets import ALICE, add_member
from tests.test_datasets import tables as tables  # noqa: F401  (fixture)
from tests.test_ingest import ALICE_NOW, grant_of
from tests.test_ingest import jobs as jobs  # noqa: F401  (fixture)
from tests.test_runs_plan import datasets as datasets  # noqa: F401  (fixture)
from tests.test_runs_plan import spec
from tests.test_uploads import staging as staging  # noqa: F401  (fixture)

EVALUATIONS = "/v1/projects/flood/evaluations"
T0 = datetime(2026, 1, 1, tzinfo=UTC)
TIMESTAMP = pa.timestamp("us", tz="UTC")
OBS = {"usgs-a": [1.0, 2.0, 3.0, 4.0], "usgs-b": [10.0, 20.0, 30.0, 40.0]}
SIM = {"nwm-1": [1.5, 2.0, 2.0, 5.0], "nwm-2": [10.0, 22.0, 30.0, 38.0]}
LINKS = {"usgs-a": "nwm-1", "usgs-b": "nwm-2"}


class Lake:
    """Every table's rows, read as Iceberg would: the pushed-down filter applied,
    one snapshot per table."""

    def __init__(self, client, fake):
        self.client, self.data, self.reads = client, {}, []
        fake.read, fake.count = self.read, lambda *a, **k: 0

    def put(self, name, table):
        self.data[("flood", name)] = table

    def read(self, token, ns, name, *, columns, row_filter, snapshot_id, limit):
        self.reads.append((ns, name, row_filter))
        table = self.data[(ns, name)]
        if row_filter is not None:
            declared = self.client.get(
                f"/v1/projects/{ns}/datasets/{name}", headers=ALICE
            ).json()["columns"]
            bound = bind(iceberg_schema(declared), row_filter, case_sensitive=True)
            table = table.filter(expression_to_pyarrow(bound))
        if columns:
            table = table.select(list(columns))
        return ReadResult(table.slice(0, limit), 7_000 + len(name))


def _series(keys):
    return pa.table(
        {"series_key": keys, **{c: pa.array([None] * len(keys), pa.string())
                                for c in ("location_key", "location_layer", "period",
                                          "resolution")}}
    )  # fmt: skip


@pytest.fixture
def lake(client, datasets, jobs):  # noqa: F811
    store = Lake(client, datasets)
    hours = [T0 + timedelta(hours=h) for h in range(4)]
    store.put("usgs_obs", pa.table({
        "series_key": [k for k in OBS for _ in hours],
        "value_time": pa.array(hours * len(OBS), TIMESTAMP),
        "value": [v for vs in OBS.values() for v in vs],
    }))  # fmt: skip
    store.put("nwm_sr", pa.table({
        "series_key": [k for k in SIM for _ in hours],
        "reference_time": pa.array([T0] * 4 * len(SIM), TIMESTAMP),
        "value_time": pa.array(hours * len(SIM), TIMESTAMP),
        "value": [v for vs in SIM.values() for v in vs],
    }))  # fmt: skip
    store.put("usgs_obs_series", _series(list(OBS)))
    store.put("nwm_sr_series", _series(list(SIM)))
    store.put(
        "usgs_nwm",
        pa.table({"left_key": list(LINKS), "right_key": list(LINKS.values())}),
    )
    return store


def run_job(client, kube):
    job = GrantClient("http://testserver", grant_of(kube.created[-1]), http=client)
    task = job.task()
    result = run_evaluation(job, task)
    job.finish("succeeded", result=result)
    return result


def saved(client, body=None, slug="e"):
    resp = client.put(f"{EVALUATIONS}/{slug}", json=body or spec(), headers=ALICE)
    assert resp.status_code in (200, 201), resp.text


def test_a_run_reads_runs_and_writes_its_results(client, lake, jobs, tables):  # noqa: F811
    kube, delegation = jobs
    body = spec()
    body["aggregate"][0]["metrics"] = ["count", "rmse"]
    body["inputs"]["primary"]["filter"] = "value_time < '2026-01-01T03:00:00Z'"
    saved(client, body)
    resp = client.post(f"{EVALUATIONS}/e/runs", headers=ALICE_NOW)
    assert resp.status_code == 202, resp.text
    run = resp.json()
    assert (run["status"], run["evaluation"], run["version"]) == ("queued", "e", 1)
    assert run["results_dataset"] == "results_e_v1"
    assert kube.created[-1]["metadata"]["generateName"].startswith("run-")

    result = run_job(client, kube)

    # Read as the caller, with the input's filter pushed down to the scan.
    assert ("flood", "usgs_obs") in {(ns, n) for ns, n, _ in lake.reads}
    assert next(f for ns, n, f in lake.reads if n == "usgs_obs") is not None
    assert result["rows"] == 2 and result["dataset"] == "results_e_v1"
    assert set(result["inputs"]) == {
        "flood.usgs_obs", "flood.usgs_obs_series", "flood.nwm_sr",
        "flood.nwm_sr_series", "flood.usgs_nwm",
    }  # fmt: skip

    got = client.get(f"/v1/projects/flood/runs/{run['id']}", headers=ALICE).json()
    assert got["status"] == "succeeded" and got["result"]["rows"] == 2
    dataset = client.get(
        "/v1/projects/flood/datasets/results_e_v1", headers=ALICE
    ).json()
    assert dataset["kind"] == "table"
    assert {c["name"] for c in dataset["columns"]} >= {
        "primary_series_key", "secondary_dataset", "count", "root_mean_square_error",
        "run_id", "spec_version", "engine_version",
    }  # fmt: skip

    assert {m for m, _, _ in tables.writes} == {"overwrite"}
    written = tables.written[("flood", "results_e_v1")][-1]
    rows = {r["primary_series_key"]: r for r in written.to_pylist()}
    for obs_key, sim_key in LINKS.items():  # the first three hours (the filter)
        o, s = np.array(OBS[obs_key][:3]), np.array(SIM[sim_key][:3])
        assert rows[obs_key]["count"] == 3
        assert rows[obs_key]["root_mean_square_error"] == pytest.approx(
            np.sqrt(np.mean((s - o) ** 2))
        )
        assert (
            rows[obs_key]["run_id"] == run["id"] and rows[obs_key]["spec_version"] == 1
        )

    # Runs of the evaluation, newest first; a rerun replaces the results.
    client.post(f"{EVALUATIONS}/e/runs", headers=ALICE_NOW)
    run_job(client, kube)
    runs = client.get(f"{EVALUATIONS}/e/runs", headers=ALICE).json()
    assert [r["status"] for r in runs] == ["succeeded", "succeeded"]
    assert tables.writes[-1][0] == "overwrite"


def test_a_crosswalk_the_other_way_round_is_turned_round(client, lake, jobs):  # noqa: F811
    kube, _ = jobs
    from tests.test_runs_plan import crosswalk

    crosswalk(client, "nwm_usgs", "nwm_sr", "usgs_obs")
    lake.put("nwm_usgs", pa.table({"left_key": list(LINKS.values()),
                                   "right_key": list(LINKS)}))  # fmt: skip
    body = spec(relation="nwm_usgs")
    body["aggregate"][0]["metrics"] = ["count"]
    saved(client, body)
    client.post(f"{EVALUATIONS}/e/runs", headers=ALICE_NOW)
    assert run_job(client, kube)["rows"] == 2


def test_starting_a_run(client, lake, jobs):  # noqa: F811
    kube, delegation = jobs
    assert client.post(f"{EVALUATIONS}/nope/runs", headers=ALICE_NOW).status_code == 404
    saved(client)
    resp = client.post(f"{EVALUATIONS}/e/runs", json={"version": 9}, headers=ALICE_NOW)
    assert resp.status_code == 404
    # A token without the project role would run without it.
    resp = client.post(f"{EVALUATIONS}/e/runs", headers=ALICE)
    assert resp.status_code == 403 and resp.json()["code"] == "token_refresh_required"
    add_member(client, "bob", "read")
    from tests.test_datasets import BOB

    assert client.post(f"{EVALUATIONS}/e/runs", headers=BOB).status_code == 403
    # Datasets changed since saving: the run is refused with what's wrong.
    client.delete("/v1/projects/flood/datasets/usgs_nwm", headers=ALICE)
    resp = client.post(f"{EVALUATIONS}/e/runs", headers=ALICE_NOW)
    assert resp.status_code == 422 and "usgs_nwm" in resp.text
    assert kube.created == []


def test_location_fields_arent_run_yet(client, lake, jobs):  # noqa: F811
    from tests.test_layers import layer, timeseries
    from tests.test_runs_plan import crosswalk

    layer(client, "gauges", attributes=[{"name": "huc2", "type": "string"}])
    timeseries(client, "obs_located", location_layer="gauges")
    crosswalk(client, "located_nwm", "obs_located", "nwm_sr")
    field = {"name": "huc", "fn": "attribute", "attribute": "huc2", "of": "location"}
    body = spec(primary="obs_located", relation="located_nwm", fields=[field])
    saved(client, body)
    resp = client.post(f"{EVALUATIONS}/e/runs", headers=ALICE_NOW)
    assert resp.status_code == 422 and "don't do yet" in resp.text


def test_a_failed_run_says_why(client, lake, jobs):  # noqa: F811
    kube, _ = jobs
    saved(client)
    client.post(f"{EVALUATIONS}/e/runs", headers=ALICE_NOW)
    job = GrantClient("http://testserver", grant_of(kube.created[-1]), http=client)
    job.task()
    job.finish("failed", error="reading flood.usgs_obs: HTTP 502")
    run = client.get(f"{EVALUATIONS}/e/runs", headers=ALICE).json()[0]
    assert run["status"] == "failed" and "HTTP 502" in run["error"]


def test_deleting_an_evaluation_deletes_its_runs_and_results(
    client, lake, jobs, tables
):  # noqa: F811
    kube, _ = jobs
    saved(client)
    run = client.post(f"{EVALUATIONS}/e/runs", headers=ALICE_NOW).json()
    # Not while a run is going: it would write into a dropped table.
    resp = client.delete(f"{EVALUATIONS}/e", headers=ALICE)
    assert resp.status_code == 409 and "going" in resp.text
    run_job(client, kube)
    assert client.delete(f"{EVALUATIONS}/e", headers=ALICE).status_code == 204
    run_url = f"/v1/projects/flood/runs/{run['id']}"
    assert client.get(run_url, headers=ALICE).status_code == 404
    results = "/v1/projects/flood/datasets/results_e_v1"
    assert client.get(results, headers=ALICE).status_code == 404
    assert ("drop", "flood", "results_e_v1") in {
        (c[0], c[2], c[3]) for c in tables.calls
    }


def test_deleting_an_evaluation_can_keep_its_results(client, lake, jobs):  # noqa: F811
    kube, _ = jobs
    saved(client)
    client.post(f"{EVALUATIONS}/e/runs", headers=ALICE_NOW)
    run_job(client, kube)
    resp = client.delete(
        f"{EVALUATIONS}/e", params={"keep_results": True}, headers=ALICE
    )
    assert resp.status_code == 204
    results = "/v1/projects/flood/datasets/results_e_v1"
    assert client.get(results, headers=ALICE).status_code == 200


def test_pairs_are_kept_when_the_spec_asks(client, lake, jobs, tables):  # noqa: F811
    kube, _ = jobs
    body = spec(output={"pairs": "all"})
    body["aggregate"][0]["metrics"] = ["count"]
    saved(client, body)
    run = client.post(f"{EVALUATIONS}/e/runs", headers=ALICE_NOW).json()
    result = run_job(client, kube)
    assert result["pairs"]["dataset"] == "pairs_e_v1" and result["pairs"]["rows"] == 8
    pairs = client.get("/v1/projects/flood/datasets/pairs_e_v1", headers=ALICE).json()
    names = {c["name"] for c in pairs["columns"]}
    assert {"primary_series_key", "secondary_series_key", "value_time",
            "primary_value", "secondary_value", "run_id"} <= names  # fmt: skip
    written = next(
        d for m, _, d in tables.writes if "secondary_value" in d.column_names
    )
    assert written.num_rows == 8 and set(written["run_id"].to_pylist()) == {run["id"]}
    # Deleting the evaluation drops its pairs too.
    assert client.delete(f"{EVALUATIONS}/e", headers=ALICE).status_code == 204
    gone = client.get("/v1/projects/flood/datasets/pairs_e_v1", headers=ALICE)
    assert gone.status_code == 404


def test_no_pairs_by_default(client, lake, jobs):  # noqa: F811
    kube, _ = jobs
    saved(client)
    client.post(f"{EVALUATIONS}/e/runs", headers=ALICE_NOW)
    assert "pairs" not in run_job(client, kube)
    resp = client.get("/v1/projects/flood/datasets/pairs_e_v1", headers=ALICE)
    assert resp.status_code == 404


def test_state_is_kept_and_gives_the_results_back(client, lake, jobs, tables):  # noqa: F811
    from teehr_core.engine import DuckDBBackend
    from teehr_core.spec import load_spec

    from src.jobs.run import decode_state

    kube, _ = jobs
    body = spec()
    body["aggregate"][0]["metrics"] = ["count", "rmse", "relative_median"]
    saved(client, body)
    client.post(f"{EVALUATIONS}/e/runs", headers=ALICE_NOW)
    result = run_job(client, kube)
    # One row per group, transform and month: two series, all in January 2026.
    assert result["state"]["dataset"] == "state_e_v1" and result["state"]["rows"] == 2
    assert result["state"]["bucket"] == "month"
    columns = client.get("/v1/projects/flood/datasets/state_e_v1", headers=ALICE).json()
    names = {c["name"] for c in columns["columns"]}
    assert {
        "__transform",
        "__bucket",
        "n",
        "sum_obs",
        "xs__" + b"~obs".hex(),
    } <= names

    stored = tables.written[("flood", "state_e_v1")][-1]
    first = DuckDBBackend().first_stage_from_state(
        load_spec(body), decode_state(stored)
    )
    results = tables.written[("flood", "results_e_v1")][-1]
    by_key = {r["primary_series_key"]: r for r in results.to_pylist()}
    for row in first.to_pylist():
        want = by_key[row["primary_series_key"]]
        for metric in ("count", "root_mean_square_error", "relative_median"):
            assert row[metric] == pytest.approx(want[metric])


def test_no_state_when_a_metric_needs_every_pair(client, lake, jobs):  # noqa: F811
    kube, _ = jobs
    body = spec()
    body["aggregate"][0]["metrics"] = ["count", "spearman_correlation"]
    saved(client, body)
    client.post(f"{EVALUATIONS}/e/runs", headers=ALICE_NOW)
    assert "state" not in run_job(client, kube)
