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
        self.client, self.fake, self.data, self.reads = client, fake, {}, []
        self.shard_reads = []
        fake.read, fake.count = self.read, lambda *a, **k: 0

    def put(self, name, table):
        self.data[("flood", name)] = table

    def read(self, token, ns, name, *, columns, row_filter, snapshot_id, limit):
        self.reads.append((ns, name, row_filter))
        if (ns, name) in self.data:
            table = self.data[(ns, name)]
        else:  # a run's table: the rows last written to it
            table = self.fake.written[(ns, name)][-1]
        if row_filter is not None:
            declared = self.client.get(
                f"/v1/projects/{ns}/datasets/{name}", headers=ALICE
            ).json()["columns"]
            bound = bind(iceberg_schema(declared), row_filter, case_sensitive=True)
            table = table.filter(expression_to_pyarrow(bound))
        if columns:
            table = table.select(list(columns))
        return ReadResult(table.slice(0, limit), 7_000 + len(name))


class FakeWarehouse:
    """The run job's warehouse over the ``Lake``'s tables: keys and filters applied
    (in DuckDB, as the real one does), each table at one snapshot."""

    def __init__(self, lake):
        self.lake, self.snapshots = lake, {}

    def read(self, read):
        from teehr_core.engine.connect import arrow_table, memory_connection, register
        from teehr_core.engine.shards import in_list
        from teehr_core.spec.expressions import to_duckdb

        lake, key = self.lake, (read.project, read.name)
        lake.reads.append((read.project, read.name, read.where))
        lake.shard_reads.append((read.project, read.name, read.keys))
        if key in lake.data:
            table = lake.data[key]
        elif lake.fake.written.get(key):  # a run's table: the rows last written to it
            table = lake.fake.written[key][-1]
        else:
            raise KeyError(f"no table {key}")
        self.snapshots.setdefault(f"{read.project}.{read.name}", 7_000 + len(read.name))
        where = []
        if read.keys is not None:
            where.append(f"{read.key} {in_list(sorted(set(read.keys)))}")
        if read.where:
            where.append(f"({to_duckdb(read.where)})")
        if not where:
            return table
        con = memory_connection()
        con.execute("SET TimeZone = 'UTC'")
        register(con, "t", table)
        return arrow_table(con, f"SELECT * FROM t WHERE {' AND '.join(where)}").cast(
            table.schema
        )

    def rows(self, read):  # from the catalog: not a read
        n = self.read(read).num_rows
        self.lake.reads.pop(), self.lake.shard_reads.pop()
        return n

    def close(self):
        pass


def _series(keys):
    return pa.table(
        {"series_key": keys, **{c: pa.array([None] * len(keys), pa.string())
                                for c in ("location_key", "location_layer", "period",
                                          "resolution")}}
    )  # fmt: skip


@pytest.fixture
def lake(client, datasets, jobs, monkeypatch):  # noqa: F811
    store = Lake(client, datasets)
    monkeypatch.setattr(
        "src.jobs.run.open_warehouse", lambda client, task: FakeWarehouse(store)
    )
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


def test_location_fields_label_series_by_their_locations(client, lake, jobs, tables):  # noqa: F811
    import shapely

    from tests.test_layers import layer, timeseries
    from tests.test_runs_plan import crosswalk

    kube, _ = jobs
    layer(client, "gauges", attributes=[{"name": "huc2", "type": "string"}])
    layer(client, "basins")
    timeseries(client, "obs_located", location_layer="gauges")
    crosswalk(client, "located_nwm", "obs_located", "nwm_sr")
    lake.put("obs_located", lake.data[("flood", "usgs_obs")])
    series = _series(list(OBS))
    series = series.set_column(1, "location_key", pa.array(["g-a", "g-b"]))
    lake.put("obs_located_series", series)
    lake.put("located_nwm", lake.data[("flood", "usgs_nwm")])

    def rows(places):
        return pa.table({
            "location_key": [k for k, _, _ in places],
            "name": [None] * len(places),
            "geometry": [shapely.to_wkb(g) for _, g, _ in places],
            **{c: [0.0] * len(places)
               for c in ("bbox_xmin", "bbox_ymin", "bbox_xmax", "bbox_ymax")},
            "huc2": [h for _, _, h in places],
        })  # fmt: skip

    lake.put("gauges", rows([("g-a", shapely.Point(1, 1), "02"),
                             ("g-b", shapely.Point(5, 5), "05")]))  # fmt: skip
    lake.put("basins", rows([
        ("b-west", shapely.box(0, 0, 3, 3), None),
        ("b-east", shapely.box(4, 4, 6, 6), None),
        ("b-overlap", shapely.box(0, 0, 10, 10), None),  # contains both: larger key
    ]).drop_columns(["huc2"]))  # fmt: skip

    body = spec(primary="obs_located", relation="located_nwm", fields=[
        {"name": "huc", "fn": "attribute", "attribute": "huc2", "of": "location"},
        {"name": "basin", "fn": "within", "layer": "basins"},
    ])  # fmt: skip
    body["aggregate"][0]["group_by"] = ["huc", "basin"]
    body["aggregate"][0]["metrics"] = ["count"]
    saved(client, body)
    result = run_once(client, kube)
    assert result["rows"] == 2
    written = tables.written[("flood", "results_e_v1")][-1].to_pylist()
    # g-a is in b-west and b-overlap, g-b in b-east and b-overlap: the smaller key.
    assert sorted((r["huc"], r["basin"], r["count"]) for r in written) == [
        ("02", "b-overlap", 4),
        ("05", "b-east", 4),
    ]
    # Labels come from the locations layer: a change there makes the next run full.
    again = run_once(client, kube)
    assert again["mode"] == "incremental", again.get("why")
    moved = [{"location_key": "g-b", "geometry": "POINT (1 1)", "huc2": "02"}]
    resp = client.post(
        "/v1/projects/flood/datasets/gauges/data", json=moved, headers=ALICE
    )
    assert resp.status_code == 201, resp.text
    assert run_once(client, kube)["why"] == "'flood.gauges' changed (series' locations)"


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


# --- incremental runs -----------------------------------------------------------

FEB = T0 + timedelta(days=31)
OBS_FEB = {"usgs-a": [5.0, 6.0], "usgs-b": [50.0, 60.0]}
SIM_FEB = {"nwm-1": [4.0, 7.0], "nwm-2": [55.0, 58.0]}


def _feb_rows(values, forecast=False):
    rows = []
    for key, vs in values.items():
        for h, v in enumerate(vs):
            row = {
                "series_key": key,
                "value_time": (FEB + timedelta(hours=h)).isoformat(),
                "value": v,
            }
            if forecast:
                row["reference_time"] = FEB.isoformat()
            rows.append(row)
    return rows  # fmt: skip


def add_february(client, lake):
    """Write February through the API (lineage records the month) and into the lake."""
    for name, values, forecast in (
        ("usgs_obs", OBS_FEB, False),
        ("nwm_sr", SIM_FEB, True),
    ):
        rows = _feb_rows(values, forecast)
        resp = client.post(f"/v1/projects/flood/datasets/{name}/data", json=rows,
                           headers=ALICE)  # fmt: skip
        assert resp.status_code == 201, resp.text
        assert resp.json()["months"] == ["2026-02"]
        old = lake.data[("flood", name)]
        new = pa.table({
            c: pa.array([
                (datetime.fromisoformat(r[c]) if c.endswith("_time") else r[c])
                for r in rows
            ], old.schema.field(c).type)
            for c in old.column_names
        })  # fmt: skip
        lake.put(name, pa.concat_tables([old, new]))


def run_once(client, kube, body=None):
    resp = client.post(f"{EVALUATIONS}/e/runs", json=body, headers=ALICE_NOW)
    assert resp.status_code == 202, resp.text
    return run_job(client, kube)


def test_a_rerun_recomputes_only_the_changed_months(client, lake, jobs, tables):  # noqa: F811
    kube, _ = jobs
    body = spec()
    body["aggregate"][0]["metrics"] = ["count", "rmse"]
    saved(client, body)
    first = run_once(client, kube)
    assert (first["mode"], first["why"]) == ("full", "no earlier run kept state")

    add_february(client, lake)
    lake.reads.clear()
    second = run_once(client, kube)
    assert (second["mode"], second["months"]) == ("incremental", ["2026-02"])
    # Only February's rows were read from the timeseries.
    obs_filter = next(f for _, n, f in lake.reads if n == "usgs_obs")
    assert obs_filter is not None
    assert second["state"]["rows"] == 4  # two series by two months

    results = tables.written[("flood", "results_e_v1")][-1]
    rows = {r["primary_series_key"]: r for r in results.to_pylist()}
    for obs_key, sim_key in LINKS.items():
        o = np.array(OBS[obs_key] + OBS_FEB[obs_key])
        s = np.array(SIM[sim_key] + SIM_FEB[sim_key])
        assert rows[obs_key]["count"] == 6
        assert rows[obs_key]["root_mean_square_error"] == pytest.approx(
            np.sqrt(np.mean((s - o) ** 2))
        )

    # Nothing changed since: nothing to read, the same results.
    third = run_once(client, kube)
    assert (third["mode"], third["months"]) == ("incremental", [])


def test_full_runs_when_asked_or_when_more_than_months_changed(client, lake, jobs):  # noqa: F811
    kube, _ = jobs
    body = spec()
    body["aggregate"][0]["metrics"] = ["count", "rmse"]
    saved(client, body)
    run_once(client, kube)
    assert run_once(client, kube, {"full": True})["why"] == "a full run was asked for"

    client.post("/v1/projects/flood/datasets/usgs_nwm/data",
                json=[{"left_key": "usgs-a", "right_key": "nwm-1"}], headers=ALICE)  # fmt: skip
    assert run_once(client, kube)["why"] == "'flood.usgs_nwm' changed"

    rows = _feb_rows(OBS_FEB)
    resp = client.post("/v1/projects/flood/datasets/usgs_obs/data", json=rows,
                       params={"mode": "overwrite"}, headers=ALICE)  # fmt: skip
    assert resp.status_code == 201 and resp.json()["months"] is None
    assert "overwritten" in run_once(client, kube)["why"]


def test_specs_whose_months_depend_on_others_run_in_full():
    from teehr_core.spec import load_spec

    from src.incremental import blockers

    assert blockers(load_spec(spec())) == []
    lead = {"name": "lead", "fn": "lead_time_bins", "width": "PT6H"}
    assert blockers(load_spec(spec(fields=[lead]))) == []
    ranked = {"name": "rank", "fn": "exceedance", "side": "primary"}
    assert blockers(load_spec(spec(fields=[ranked]))) == [
        "field 'rank' looks beyond each pair's month"
    ]


def test_rollups_keep_the_finest_sets_state(client, lake, jobs, tables):  # noqa: F811
    kube, _ = jobs
    body = spec()
    body["aggregate"] = [
        {"rollup": ["primary_series_key"], "metrics": ["count", "rmse"]}
    ]
    saved(client, body)

    def check(months):
        results = tables.written[("flood", "results_e_v1")][-1].to_pylist()
        by_key = {r["primary_series_key"]: r for r in results}
        assert set(by_key) == {"usgs-a", "usgs-b", "(all)"}
        o = np.array([v for k in OBS for v in OBS[k] + (OBS_FEB[k] if months else [])])
        s = np.array([v for k in SIM for v in SIM[k] + (SIM_FEB[k] if months else [])])
        assert by_key["(all)"]["count"] == len(o)
        assert by_key["(all)"]["root_mean_square_error"] == pytest.approx(
            np.sqrt(np.mean((s - o) ** 2))
        )

    first = run_once(client, kube)
    assert (
        first["mode"] == "full" and first["state"]["rows"] == 2
    )  # the finest set only
    check(months=False)
    add_february(client, lake)
    second = run_once(client, kube)
    assert (second["mode"], second["months"]) == ("incremental", ["2026-02"])
    check(months=True)


def _rows(table):
    keys = ("primary_series_key", "secondary_dataset")
    return sorted(table.to_pylist(), key=lambda r: tuple(str(r.get(k)) for k in keys))


def test_persistence_and_dm_runs_are_incremental_and_match_full_ones(  # noqa: F811
    client, lake, jobs, tables
):
    kube, _ = jobs
    body = spec(secondary=())
    body["inputs"]["secondary"] = [
        {"dataset": "nwm_sr"},
        {"generate": "persistence", "like": "nwm_sr", "max_age": "PT6H"},
    ]
    body["aggregate"][0]["metrics"] = ["count", "rmse"]
    body["aggregate"][0]["skill"] = {
        "reference": "persistence:nwm_sr", "metrics": ["rmse"], "test": "dm",
    }  # fmt: skip
    saved(client, body)

    first = run_once(client, kube)
    assert first["mode"] == "full" and first["state"]["rows"] > 0
    assert first["dm"]["max_lag"] >= 0 and first["dm"]["step"] == 3600
    assert first["persistence_reach"] == 6 * 3600 + 3 * 3600  # max_age + longest lead

    add_february(client, lake)
    second = run_once(client, kube)
    assert second["mode"] == "incremental" and second["changed"] == ["2026-02"]
    assert "2026-02" in second["months"]
    incremental = _rows(tables.written[("flood", "results_e_v1")][-1])

    full = run_once(client, kube, {"full": True})
    assert full["mode"] == "full"
    recomputed = _rows(tables.written[("flood", "results_e_v1")][-1])
    assert len(incremental) == len(recomputed) > 0
    for a, b in zip(incremental, recomputed, strict=True):
        for column, value in b.items():
            if column == "run_id":
                continue
            if isinstance(value, float):
                assert a[column] == pytest.approx(value, nan_ok=True), column
            else:
                assert a[column] == value, column


# --- shards ---------------------------------------------------------------------


def _same(a, b):
    assert len(a) == len(b) > 0
    for x, y in zip(a, b, strict=True):
        for column, value in y.items():
            if column == "run_id":
                continue
            if isinstance(value, float):
                assert x[column] == pytest.approx(value, nan_ok=True), column
            else:
                assert x[column] == value, column


LEAD = {"name": "lead", "fn": "lead_time_bins", "width": "PT2H"}


@pytest.mark.parametrize(
    "shape",
    ["by_series", "by_lead_time", "rollup", "persistence_dm"],
)
def test_a_run_in_shards_gives_the_same_results(
    client,
    lake,
    jobs,
    tables,
    monkeypatch,
    shape,  # noqa: F811
):
    kube, _ = jobs
    body = spec(fields=[LEAD])
    stage = body["aggregate"][0]
    stage["metrics"] = ["count", "rmse", "kge", "relative_median"]
    if shape == "by_lead_time":  # spans series: every shard has a part of each group
        stage["group_by"] = ["secondary_dataset", "lead"]
    elif shape == "rollup":
        body["aggregate"] = [{"rollup": ["primary_series_key", "lead"],
                              "metrics": ["count", "rmse"]}]  # fmt: skip
    elif shape == "persistence_dm":
        body["inputs"]["secondary"] = [
            {"dataset": "nwm_sr"},
            {"generate": "persistence", "like": "nwm_sr", "max_age": "PT6H"},
        ]
        stage["metrics"] = ["count", "rmse"]
        stage["skill"] = {
            "reference": "persistence:nwm_sr", "metrics": ["rmse"], "test": "dm",
        }  # fmt: skip
    saved(client, body)

    whole = run_once(client, kube)
    assert "shards" in whole and whole["shards"] == 1
    want = _rows(tables.written[("flood", "results_e_v1")][-1])

    monkeypatch.setenv("RUN_ROWS_PER_SHARD", "1")  # a shard per series
    lake.shard_reads.clear()
    sharded = run_once(client, kube, {"full": True})
    assert sharded["shards"] == 2
    _same(_rows(tables.written[("flood", "results_e_v1")][-1]), want)
    assert sharded["state"]["rows"] == whole["state"]["rows"]  # merged per month

    # Each shard read its own series only.
    obs = [keys for _, n, keys in lake.shard_reads if n == "usgs_obs"]
    sim = [keys for _, n, keys in lake.shard_reads if n == "nwm_sr"]
    assert sorted(map(tuple, obs)) == [("usgs-a",), ("usgs-b",)]
    assert sorted(map(tuple, sim)) == [("nwm-1",), ("nwm-2",)]


def test_a_run_in_shards_keeps_every_shards_pairs(
    client, lake, jobs, tables, monkeypatch
):  # noqa: F811
    kube, _ = jobs
    body = spec(output={"pairs": "all"})
    body["aggregate"][0]["metrics"] = ["count", "rmse"]
    saved(client, body)
    monkeypatch.setenv("RUN_ROWS_PER_SHARD", "1")
    result = run_once(client, kube)
    assert result["shards"] == 2 and result["pairs"]["rows"] == 8
    pairs = tables.written[("flood", "pairs_e_v1")]
    assert [t.num_rows for t in pairs] == [4, 4]  # a shard's, then the other's added


def test_grouping_sets_that_dont_nest_keep_state_for_every_set(
    client, lake, jobs, tables
):  # noqa: F811
    kube, _ = jobs
    body = spec(fields=[LEAD])
    body["aggregate"] = [{
        "grouping_sets": [["primary_series_key"], ["lead"]],
        "metrics": ["count", "rmse"],
    }]  # fmt: skip
    saved(client, body)
    result = run_once(client, kube)
    assert result["mode"] == "full" and result["state"]["rows"] > 0
    rows = tables.written[("flood", "results_e_v1")][-1].to_pylist()
    by_gauge = [r for r in rows if r["primary_series_key"] != "(all)"]
    by_lead = [r for r in rows if r["lead"] != "(all)"]
    assert {r["primary_series_key"] for r in by_gauge} == {"usgs-a", "usgs-b"}
    assert sum(r["count"] for r in by_gauge) == sum(r["count"] for r in by_lead) == 8


def test_cancel_a_run(client, lake, jobs):  # noqa: F811
    from tests.test_datasets import BOB

    kube, _ = jobs
    saved(client)
    run = client.post(f"{EVALUATIONS}/e/runs", headers=ALICE_NOW).json()
    url = f"/v1/projects/flood/runs/{run['id']}/cancel"
    add_member(client, "bob", "read")
    assert client.post(url, headers=BOB).status_code == 403
    resp = client.post(url, headers=ALICE)
    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "cancelled" and resp.json()["finished_at"]
    assert kube.deleted == [f"upload-{len(kube.created) - 1}"]  # the run's Job
    assert client.post(url, headers=ALICE).status_code == 409
    # With nothing going, the evaluation can be deleted.
    assert client.delete(f"{EVALUATIONS}/e", headers=ALICE).status_code == 204
