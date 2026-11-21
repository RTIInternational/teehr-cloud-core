"""Runs of a saved evaluation (Phase 5): started as a job acting as the caller,
which reads the inputs through the API, runs the spec with teehr-core's engine and
writes results_<slug>_v<version> through the API."""

import dataclasses
import logging
import uuid
from datetime import UTC, datetime, timedelta

import numpy as np
import pyarrow as pa
import pytest
from pyiceberg.expressions import EqualTo
from pyiceberg.expressions.visitors import bind
from pyiceberg.io.pyarrow import expression_to_pyarrow

from src.config import get_settings
from src.jobs.load import GrantClient
from src.jobs.run import run_evaluation
from src.main import app
from src.models import Run
from src.tables import ReadResult, get_tables, iceberg_schema
from tests.test_datasets import ALICE, add_member
from tests.test_datasets import tables as tables  # noqa: F401  (fixture)
from tests.test_ingest import ALICE_NOW, db, grant_of
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
        # Every commit to a table, as Iceberg keeps them: (snapshot, mode, rows).
        self.commits: dict[tuple[str, str], list[tuple[int, str, pa.Table]]] = {}
        write = fake.write

        def logged(token, ns, name, data, mode, overwrite_filter=None):
            result = write(token, ns, name, data, mode, overwrite_filter)
            self.commits.setdefault((ns, name), []).append(
                (
                    result.snapshot_id,
                    mode if overwrite_filter is None else "filtered",
                    data,
                )
            )
            return result

        fake.write = logged

    def base(self, ns, name):
        """The snapshot a table seeded with ``put`` starts at."""
        return 7_000 + len(name)

    def current(self, ns, name):
        commits = self.commits.get((ns, name))
        return commits[-1][0] if commits else self.base(ns, name)

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
        self.lake, self.snapshots, self.read_counts = lake, {}, {}

    def read(self, read):
        from teehr_core.engine.connect import arrow_table, memory_connection, register
        from teehr_core.engine.shards import in_list
        from teehr_core.spec.expressions import to_duckdb

        lake, key = self.lake, (read.project, read.name)
        lake.reads.append((read.project, read.name, read.where))
        self.read_counts[key] = self.read_counts.get(key, 0) + 1
        lake.shard_reads.append((read.project, read.name, read.keys))
        if key in lake.data:
            table = lake.data[key]
        elif self.snapshots.get(f"{read.project}.{read.name}") in getattr(
            lake.fake, "by_snapshot", {}
        ):  # a run's table, at the snapshot it's pinned to
            table = lake.fake.by_snapshot[self.snapshots[f"{read.project}.{read.name}"]]
        elif lake.fake.written.get(key):  # a run's table: the rows last written to it
            table = lake.fake.written[key][-1]
        else:
            raise KeyError(f"no table {key}")
        self.snapshots.setdefault(
            f"{read.project}.{read.name}", self._current(read.project, read.name)
        )
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

    def pin(self, project, name):
        self.snapshots.setdefault(f"{project}.{name}", self._current(project, name))

    def _current(self, project, name):
        if (project, name) in self.lake.data:
            return self.lake.current(project, name)
        return 7_000 + len(name)

    def state(self, project, name):
        """The real one records live files' rows and months; commits stand in here."""
        return {"sequence": self.snapshots.setdefault(
            f"{project}.{name}", self._current(project, name))}  # fmt: skip

    def changes(self, project, name, since):
        """What comparing with the recorded state would find (teehr_core.warehouse)."""
        from teehr_core.warehouse import Changes

        pinned = self.snapshots.setdefault(
            f"{project}.{name}", self._current(project, name)
        )
        months, added_only, n = set(), True, 0

        def of(rows):
            if "value_time" not in rows.column_names:
                return set()
            return {t.strftime("%Y-%m") for t in rows["value_time"].to_pylist() if t}

        for snapshot, mode, rows in self.lake.commits.get((project, name), []):
            if not since["sequence"] < snapshot <= pinned:
                continue
            n += 1
            added_only = added_only and mode == "append"
            months |= of(rows)
            if mode == "overwrite":  # every file goes: their months too
                months |= of(self.lake.data.get((project, name), rows))
        return Changes(new_files=n, added_only=added_only, months=tuple(sorted(months)))

    def count(self, read):  # the rows a read keeps (the real one counts in files)
        return self.rows(read)

    def counts(self):  # one read per table read (the real one counts files too)
        return {f"{ns}.{n}": {"reads": c} for (ns, n), c in self.read_counts.items()}

    def read_stats(self, others=()):
        total = {ref: dict(t) for ref, t in self.counts().items()}
        for other in others:
            for ref, t in (other or {}).items():
                total.setdefault(ref, {"reads": 0})["reads"] += t["reads"]
        return total

    def close(self):
        pass


def _series(keys):
    return pa.table(
        {"series_key": keys, **{c: pa.array([None] * len(keys), pa.string())
                                for c in ("location_key", "location_layer", "period",
                                          "resolution")}}
    )  # fmt: skip


@pytest.fixture
def lake(client, datasets, jobs, staging, monkeypatch):  # noqa: F811
    store = Lake(client, datasets)

    # Work files go to the staging bucket by presigned URL (ADR-0017).
    def key(url):
        return url.split("://", 1)[1].split("/", 1)[1].split("?", 1)[0]

    monkeypatch.setattr(
        GrantClient,
        "put",
        lambda self, url, data: staging.objects.__setitem__(key(url), data),
    )
    monkeypatch.setattr(GrantClient, "get",
                        lambda self, url: staging.objects[key(url)])  # fmt: skip
    monkeypatch.setattr(
        "src.jobs.run.open_warehouse", lambda client, task: FakeWarehouse(store)
    )
    # The job writes its tables itself (ADR-0016), to the same fake tables.
    monkeypatch.setattr(
        "src.jobs.run.open_tables", lambda task: app.dependency_overrides[get_tables]()
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


def _located(client, lake):
    """Observed series at gauges (g-a, g-b; huc2 02, 05), inside basins."""
    import shapely

    from tests.test_layers import layer, timeseries
    from tests.test_runs_plan import crosswalk

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


def test_location_fields_label_series_by_their_locations(client, lake, jobs, tables):  # noqa: F811
    kube, _ = jobs
    _located(client, lake)
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
    # The snapshots say which months the overwrite removed rows from, and added to.
    again = run_once(client, kube)
    assert (again["mode"], again["changed"]) == ("incremental", ["2026-01", "2026-02"])


def test_writes_made_outside_the_api_are_seen(client, lake, jobs):  # noqa: F811
    kube, _ = jobs
    body = spec()
    body["aggregate"][0]["metrics"] = ["count", "rmse"]
    saved(client, body)
    run_once(client, kube)
    # A flow writes February straight to the table (ADR-0019): no write record.
    lake.commits[("flood", "usgs_obs")] = [
        (9_100, "filtered", pa.table({"value_time": pa.array([FEB], TIMESTAMP)}))
    ]
    again = run_once(client, kube)
    assert (again["mode"], again["changed"]) == ("incremental", ["2026-02"])


def test_a_run_is_full_when_the_last_run_recorded_no_state(
    client, lake, jobs, monkeypatch
):  # noqa: F811
    kube, _ = jobs
    body = spec()
    body["aggregate"][0]["metrics"] = ["count", "rmse"]
    saved(client, body)
    recorded = FakeWarehouse.state
    monkeypatch.setattr(FakeWarehouse, "state", lambda self, p, n: None)  # unknown
    first = run_once(client, kube)
    assert first["input_states"]["flood.usgs_obs"] is None
    monkeypatch.setattr(FakeWarehouse, "state", recorded)
    assert "didn't record 'flood.usgs_obs''s state" in run_once(client, kube)["why"]


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
    client, lake, jobs, tables, staging, monkeypatch
):  # noqa: F811
    kube, _ = jobs
    body = spec(output={"pairs": "all"})
    body["aggregate"][0]["metrics"] = ["count", "rmse"]
    saved(client, body)
    monkeypatch.setenv("RUN_ROWS_PER_SHARD", "1")
    result = run_once(client, kube)
    assert result["shards"] == 2 and result["pairs"]["rows"] == 8
    pairs = tables.written[("flood", "pairs_e_v1")]
    assert [t.num_rows for t in pairs] == [8]  # both shards' files, one commit
    assert not tables.files  # every file written was committed
    assert not staging.objects  # the shards' state parts are gone


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


def test_a_run_writes_its_tables_itself_and_the_control_plane_records_them(
    client, lake, jobs, tables, monkeypatch
):  # noqa: F811
    kube, _ = jobs
    body = spec(output={"pairs": "all"})
    body["aggregate"][0]["metrics"] = ["count", "rmse"]
    saved(client, body)

    def through_data(*args, **kwargs):
        raise AssertionError("a run's data went through the control plane's /data")

    monkeypatch.setattr(GrantClient, "api", through_data)  # ADR-0016
    result = run_once(client, kube)

    for kind, written in (
        ("results", result),
        ("pairs", result["pairs"]),
        ("state", result["state"]),
    ):
        writes = client.get(
            f"/v1/projects/flood/datasets/{written['dataset']}/writes", headers=ALICE
        ).json()
        assert [(w["mode"], w["rows"], w["snapshot_id"]) for w in writes] == [
            ("overwrite", written["rows"], str(written["snapshot_id"]))
        ], kind


def test_a_jobs_write_report_is_checked_and_recorded_once(client, lake, jobs, tables):  # noqa: F811
    kube, _ = jobs
    body = spec()
    body["aggregate"][0]["metrics"] = ["count", "rmse"]
    saved(client, body)
    run_once(client, kube)  # creates and writes results_e_v1
    client.post(f"{EVALUATIONS}/e/runs", json={"full": True}, headers=ALICE_NOW)
    job = GrantClient("http://testserver", grant_of(kube.created[-1]), http=client)
    job.task()

    def report(dataset="results_e_v1", snapshot_id=None):
        return client.post(
            "/v1/job-grants/writes",
            headers={"Authorization": f"Bearer {job.grant}"},
            json={"dataset": dataset, "mode": "overwrite", "rows": 1,
                  "snapshot_id": snapshot_id or tables.snapshot},
        )  # fmt: skip

    assert report("usgs_obs").status_code == 403  # not one of the run's tables
    assert report(snapshot_id=1).status_code == 409  # no such snapshot
    first, again = report(), report()
    assert first.status_code == again.status_code == 201
    assert first.json()["id"] == again.json()["id"]


def test_an_incremental_run_builds_on_the_last_succeeded_runs_state(
    client, lake, jobs, tables
):  # noqa: F811
    kube, _ = jobs
    body = spec()
    body["aggregate"][0]["metrics"] = ["count", "rmse"]
    saved(client, body)
    run_once(client, kube)
    # A later run failed after committing its state: other state is current.
    stored = tables.written[("flood", "state_e_v1")][-1]
    tables.write("t", "flood", "state_e_v1", stored.slice(0, 0), "overwrite")

    add_february(client, lake)
    assert run_once(client, kube)["mode"] == "incremental"
    incremental = _rows(tables.written[("flood", "results_e_v1")][-1])
    run_once(client, kube, {"full": True})
    _same(incremental, _rows(tables.written[("flood", "results_e_v1")][-1]))


def test_a_version_runs_one_at_a_time(client, lake, jobs, tables):  # noqa: F811
    kube, _ = jobs
    saved(client)
    start = lambda: client.post(f"{EVALUATIONS}/e/runs", headers=ALICE_NOW)  # noqa: E731
    assert start().status_code == 202
    second = start()
    assert second.status_code == 409 and second.json()["code"] == "run_in_progress"

    kube.jobs.pop(next(iter(kube.jobs)))  # its pod died without reporting
    assert start().status_code == 202  # the dead run no longer holds the version
    run_job(client, kube)
    assert start().status_code == 202  # nor does a finished one


def test_a_run_that_fails_leaves_its_tables_and_no_files(
    client, lake, jobs, tables, staging, monkeypatch
):  # noqa: F811
    kube, _ = jobs
    body = spec(output={"pairs": "all"})
    body["aggregate"][0]["metrics"] = ["count", "rmse"]
    saved(client, body)
    run_once(client, kube)
    before = {k: len(v) for k, v in tables.written.items()}

    def broken(*args, **kwargs):
        raise RuntimeError("the merge broke")

    monkeypatch.setenv("RUN_ROWS_PER_SHARD", "1")
    monkeypatch.setattr("src.jobs.run.first_stage_from_state", broken)
    with pytest.raises(RuntimeError):
        run_once(client, kube, {"full": True})
    assert {k: len(v) for k, v in tables.written.items()} == before  # nothing written
    assert not tables.files  # the shards' pairs files were deleted
    assert not staging.objects  # and their state parts


def test_work_files_live_under_the_run_and_go_when_it_finishes(
    client, lake, jobs, tables, staging
):  # noqa: F811
    kube, _ = jobs
    saved(client)
    run = client.post(f"{EVALUATIONS}/e/runs", headers=ALICE_NOW).json()
    job = GrantClient("http://testserver", grant_of(kube.created[-1]), http=client)
    job.task()
    grant = {"Authorization": f"Bearer {job.grant}"}

    bad = client.post("/v1/job-grants/work", headers=grant, json={"put": ["../x"]})
    assert bad.status_code == 422
    urls = client.post("/v1/job-grants/work", headers=grant,
                       json={"put": ["state/shard-00000-a1.parquet"]}).json()  # fmt: skip
    url = urls["put"]["state/shard-00000-a1.parquet"]
    assert f"/runs/{run['id']}/state/shard-00000-a1.parquet" in url
    staging.objects[f"runs/{run['id']}/state/shard-00000-a1.parquet"] = b"x"
    staging.objects["runs/another/state/x.parquet"] = b"y"

    job.finish("failed", error="stopped")
    assert list(staging.objects) == ["runs/another/state/x.parquet"]  # only its own


def _queued_run(client, kube, plan=None, shards=(["a"], ["b"])):
    """A run's job with a queue of ``shards``; returns (job client, grant headers)."""
    saved(client)
    client.post(f"{EVALUATIONS}/e/runs", headers=ALICE_NOW)
    job = GrantClient("http://testserver", grant_of(kube.created[-1]), http=client)
    job.task()
    grant = {"Authorization": f"Bearer {job.grant}"}
    r = client.put("/v1/job-grants/shards", headers=grant,
                   json={"plan": plan or {"mode": "full"}, "shards": list(shards)})  # fmt: skip
    assert r.status_code == 204, r.text
    return job, grant


def test_the_shard_queue_hands_out_each_shard_once_and_retries_failures(
    client, lake, jobs, tables
):  # noqa: F811
    kube, _ = jobs
    _, grant = _queued_run(client, kube, {"mode": "full", "dm": None})

    def call(path, **body):
        return client.post(f"/v1/job-grants/shards{path}", headers=grant, json=body)

    assert client.get("/v1/job-grants/shards/plan", headers=grant).json() == {
        "plan": {"mode": "full", "dm": None}
    }
    a = call("/claim", worker="p0").json()["shard"]
    b = call("/claim", worker="p1").json()["shard"]
    assert (a["index"], a["attempt"], a["keys"]) == (0, 1, ["a"])
    assert (b["index"], b["attempt"]) == (1, 1)
    assert call("/claim", worker="p2").json() == {"shard": None, "settled": False}

    assert call("/0/heartbeat", attempt=1).status_code == 204
    assert call("/0/done", attempt=2, outputs={}).status_code == 409  # not its try
    assert call("/0/done", attempt=1, outputs={"state": "s0"}).status_code == 204
    assert call("/1/fail", attempt=1, error="boom", retry=True).status_code == 204
    again = call("/claim", worker="p2").json()["shard"]
    assert (again["index"], again["attempt"]) == (1, 2)  # queued again
    call("/1/fail", attempt=2, error="boom", retry=True)
    call("/claim", worker="p2")
    call("/1/fail", attempt=3, error="boom again", retry=True)  # the last try
    assert call("/claim", worker="p2").json() == {"shard": None, "settled": True}

    summary = client.get("/v1/job-grants/shards", headers=grant).json()
    assert summary["counts"] == {"queued": 0, "running": 0, "done": 1, "failed": 1}
    assert summary["done"] == [{"index": 0, "outputs": {"state": "s0"}}]
    assert summary["failed"] == [{"index": 1, "error": "boom again"}]


def test_a_shard_whose_pod_stops_is_claimed_again(client, lake, jobs, tables):  # noqa: F811
    kube, _ = jobs
    settings = app.dependency_overrides[get_settings]()
    app.dependency_overrides[get_settings] = lambda: dataclasses.replace(
        settings, shard_heartbeat_lapse_seconds=0
    )
    _, grant = _queued_run(client, kube, shards=[["a"]])

    def call(path, **body):
        return client.post(f"/v1/job-grants/shards{path}", headers=grant, json=body)

    assert call("/claim", worker="p0").json()["shard"]["attempt"] == 1
    # p0's heartbeat has lapsed (instantly, here): p1 takes the shard over.
    taken = call("/claim", worker="p1").json()["shard"]
    assert (taken["index"], taken["attempt"]) == (0, 2)
    assert call("/0/heartbeat", attempt=1).status_code == 409  # p0 learns it lost it
    assert call("/0/done", attempt=1, outputs={}).status_code == 409
    assert call("/0/done", attempt=2, outputs={}).status_code == 204


def test_a_shard_that_fails_once_is_run_again_and_the_run_succeeds(
    client, lake, jobs, tables, monkeypatch
):  # noqa: F811
    from src.jobs.run import ShardWork

    kube, _ = jobs
    body = spec()
    body["aggregate"][0]["metrics"] = ["count", "rmse"]
    saved(client, body)
    whole = run_once(client, kube)
    want = _rows(tables.written[("flood", "results_e_v1")][-1])

    monkeypatch.setenv("RUN_ROWS_PER_SHARD", "1")
    real, tries = ShardWork.__call__, []

    def flaky(self, index, attempt, keys):
        tries.append((index, attempt))
        if (index, attempt) == (1, 1):
            raise RuntimeError("the pod hiccuped")
        return real(self, index, attempt, keys)

    monkeypatch.setattr(ShardWork, "__call__", flaky)
    sharded = run_once(client, kube, {"full": True})
    assert tries == [(0, 1), (1, 1), (1, 2)]
    assert sharded["shards"] == 2 and sharded["state"]["rows"] == whole["state"]["rows"]
    _same(_rows(tables.written[("flood", "results_e_v1")][-1]), want)


def test_a_run_with_enough_shards_gets_more_pods(
    client, lake, jobs, tables, monkeypatch
):  # noqa: F811
    from src.routes.job_grants import run_pods

    kube, _ = jobs
    settings = app.dependency_overrides[get_settings]()
    wide = dataclasses.replace(settings, run_max_pods=4, run_shards_per_pod=1)
    app.dependency_overrides[get_settings] = lambda: wide
    body = spec()
    body["aggregate"][0]["metrics"] = ["count", "rmse"]
    saved(client, body)
    monkeypatch.setenv("RUN_ROWS_PER_SHARD", "1")
    assert run_once(client, kube)["shards"] == 2
    assert kube.scaled == [(f"upload-{len(kube.created) - 1}", 2)]  # a pod a shard

    assert run_pods(dataclasses.replace(wide, run_shards_per_pod=8), 144) == 4
    assert run_pods(dataclasses.replace(wide, run_shards_per_pod=8), 9) == 2
    assert run_pods(settings, 144) == 1  # off by default


def test_another_pod_runs_the_shards_and_pod_zero_commits_them(
    client, lake, jobs, tables, staging, monkeypatch
):  # noqa: F811
    from src.jobs import run as runner

    kube, _ = jobs
    body = spec(output={"pairs": "all"})
    body["aggregate"][0]["metrics"] = ["count", "rmse"]
    saved(client, body)
    whole = run_once(client, kube)
    want = _rows(tables.written[("flood", "results_e_v1")][-1])

    # Pod 1 starts once pod 0 has queued the shards, and runs every one of them.
    put, pod1_ran = runner.ShardQueue.put, []

    def put_then_pod1(self, plan, shards):
        put(self, plan, shards)
        pod1 = GrantClient("http://testserver", self.client.grant, http=client)
        pod1_task = pod1.task()
        real = runner.ShardWork.__call__

        def tracked(work, index, attempt, keys):
            pod1_ran.append(index)
            return real(work, index, attempt, keys)

        monkeypatch.setattr(runner.ShardWork, "__call__", tracked)
        monkeypatch.setenv("HOSTNAME", "pod-1")  # another pod
        runner.work_shards(pod1, pod1_task)
        monkeypatch.delenv("HOSTNAME")
        monkeypatch.setattr(runner.ShardWork, "__call__", real)

    monkeypatch.setattr(runner.ShardQueue, "put", put_then_pod1)
    monkeypatch.setenv("RUN_ROWS_PER_SHARD", "1")
    sharded = run_once(client, kube, {"full": True})
    assert sorted(pod1_ran) == [0, 1]  # pod 0 found the queue done
    # The run's reads include pod 1's: both shards read the observations there.
    assert sharded["reads"]["flood.usgs_obs"]["reads"] == 2  # all pod 1's
    assert sharded["shards"] == 2 and sharded["pairs"]["rows"] == whole["pairs"]["rows"]
    _same(_rows(tables.written[("flood", "results_e_v1")][-1]), want)
    assert not tables.files and not staging.objects  # all committed, work files gone


def test_results_keep_every_runs_rows_and_each_run_reads_its_own(
    client,
    lake,
    jobs,
    tables,  # noqa: F811
):
    kube, _ = jobs
    saved(client)
    ids = []
    for _ in range(2):
        resp = client.post(
            f"{EVALUATIONS}/e/runs", json={"full": True}, headers=ALICE_NOW
        )
        assert resp.status_code == 202, resp.text
        ids.append(resp.json()["id"])
        run_job(client, kube)

    # Each run replaced only its own rows of the version's results table.
    writes = [c for c in tables.calls if c[0] == "write"]
    assert len(writes) == len(tables.writes)
    results_filters = [
        f for (_, _, _, name), (_, f, _) in zip(writes, tables.writes, strict=True)
        if name == "results_e_v1"
    ]  # fmt: skip
    assert results_filters == [EqualTo("run_id", ids[0]), EqualTo("run_id", ids[1])]
    # The table (as the lake holds it): both runs' rows.
    both = pa.concat_tables(tables.written[("flood", "results_e_v1")][-2:])
    lake.put("results_e_v1", both)
    assert set(both["run_id"].to_pylist()) == set(ids)

    first = client.get(f"/v1/projects/flood/runs/{ids[0]}/results", headers=ALICE)
    assert first.status_code == 200, first.text
    rows = first.json()["rows"]
    assert rows and {r["run_id"] for r in rows} == {ids[0]}
    # A filter narrows further, within the run.
    key = rows[0]["primary_series_key"]
    one = client.get(
        f"/v1/projects/flood/runs/{ids[1]}/results",
        params={
            "filter": f"primary_series_key = '{key}'",
            "columns": "primary_series_key,run_id",
        },
        headers=ALICE,
    ).json()["rows"]
    assert one == [{"primary_series_key": key, "run_id": ids[1]}]
    # Read as a dataset, the table shows every run's rows.
    every = client.get("/v1/projects/flood/datasets/results_e_v1/data", headers=ALICE)
    assert {r["run_id"] for r in every.json()["rows"]} == set(ids)
    described = client.get("/v1/projects/flood/datasets/results_e_v1", headers=ALICE)
    assert "every run's rows" in described.json()["description"]


def test_a_runs_results_come_once_it_succeeds(client, lake, jobs):  # noqa: F811
    saved(client)
    run = client.post(f"{EVALUATIONS}/e/runs", headers=ALICE_NOW).json()
    resp = client.get(f"/v1/projects/flood/runs/{run['id']}/results", headers=ALICE)
    assert resp.status_code == 409 and "queued" in resp.json()["detail"]
    missing = client.get(
        f"/v1/projects/flood/runs/{uuid.uuid4()}/results", headers=ALICE
    )
    assert missing.status_code == 404


def test_results_come_with_their_places(client, lake, jobs, tables):  # noqa: F811
    kube, _ = jobs
    _located(client, lake)
    body = spec(primary="obs_located", relation="located_nwm")
    body["aggregate"][0]["group_by"] = ["primary_series_key"]
    body["aggregate"][0]["metrics"] = ["count"]
    body["output"] = {"geometry": "primary_series_key", "attributes": ["huc2"]}
    saved(client, body)
    run_id = client.post(f"{EVALUATIONS}/e/runs", headers=ALICE_NOW).json()["id"]
    run_job(client, kube)
    url = f"/v1/projects/flood/runs/{run_id}/results"

    rows = client.get(url, headers=ALICE).json()["rows"]
    places = {r["primary_series_key"]: (r["huc2"], r["geometry"]) for r in rows}
    a, b = sorted(OBS)  # located at g-a (1, 1) and g-b (5, 5)
    assert places == {
        a: ("02", {"type": "Point", "coordinates": [1.0, 1.0]}),
        b: ("05", {"type": "Point", "coordinates": [5.0, 5.0]}),
    }
    features = client.get(
        url, headers={**ALICE, "Accept": "application/geo+json"}
    ).json()
    assert features["type"] == "FeatureCollection" and len(features["features"]) == 2
    assert {f["properties"]["huc2"] for f in features["features"]} == {"02", "05"}
    arrow = client.get(
        url, headers={**ALICE, "Accept": "application/vnd.apache.arrow.stream"}
    )
    table = pa.ipc.open_stream(arrow.content).read_all()
    assert table.schema.field("geometry").type == pa.binary()
    plain = client.get(url, params={"geometry": "false"}, headers=ALICE).json()["rows"]
    assert "geometry" not in plain[0] and "huc2" not in plain[0]
    # A dataset read of the results table has no places (it's the stored rows).
    stored = client.get("/v1/projects/flood/datasets/results_e_v1/data", headers=ALICE)
    assert "geometry" not in stored.json()["rows"][0]


def test_results_placed_by_a_within_field_get_its_polygons(client, lake, jobs, tables):  # noqa: F811
    kube, _ = jobs
    _located(client, lake)
    body = spec(primary="obs_located", relation="located_nwm",
                fields=[{"name": "basin", "fn": "within", "layer": "basins"}])  # fmt: skip
    body["aggregate"][0]["group_by"] = ["basin"]
    body["aggregate"][0]["metrics"] = ["count"]
    body["output"] = {"geometry": "basin"}
    saved(client, body)
    run_id = client.post(f"{EVALUATIONS}/e/runs", headers=ALICE_NOW).json()["id"]
    run_job(client, kube)
    rows = client.get(f"/v1/projects/flood/runs/{run_id}/results", headers=ALICE).json()
    shapes = {r["basin"]: r["geometry"]["type"] for r in rows["rows"]}
    assert shapes == {"b-overlap": "Polygon", "b-east": "Polygon"}


def test_unknown_output_attributes_say_so(client, lake, jobs, tables):  # noqa: F811
    kube, _ = jobs
    _located(client, lake)
    body = spec(primary="obs_located", relation="located_nwm")
    body["aggregate"][0]["group_by"] = ["primary_series_key"]
    body["aggregate"][0]["metrics"] = ["count"]
    body["output"] = {"geometry": "primary_series_key", "attributes": ["nope"]}
    saved(client, body)
    run_id = client.post(f"{EVALUATIONS}/e/runs", headers=ALICE_NOW).json()["id"]
    run_job(client, kube)
    resp = client.get(f"/v1/projects/flood/runs/{run_id}/results", headers=ALICE)
    assert resp.status_code == 422 and "layer 'gauges' has no nope" in resp.text


def test_a_runs_log_is_kept_and_read_in_order(client, lake, jobs, monkeypatch):  # noqa: F811
    from src import job_logs
    from src.jobs.logship import ship_logs

    kube, _ = jobs
    saved(client)
    run = client.post(f"{EVALUATIONS}/e/runs", headers=ALICE_NOW).json()
    url = f"/v1/projects/flood/runs/{run['id']}/logs"
    before = client.get(url, headers=ALICE).json()
    assert before == {"lines": [], "ended": False}

    job = GrantClient("http://testserver", grant_of(kube.created[-1]), http=client)
    shipped = ship_logs(job, "run-x-0")
    try:
        result = run_evaluation(job, job.task())
    finally:
        shipped.close()
        logging.getLogger("src.jobs").removeHandler(shipped)
    job.finish("succeeded", result=result)

    body = client.get(url, headers=ALICE).json()
    messages = [line["message"] for line in body["lines"]]
    assert body["ended"] is True
    assert any("observed series" in m and "shards" in m for m in messages), messages
    assert any(m.startswith("shard 1/1 (try 1)") for m in messages)
    assert any(m.startswith("wrote results_e_v1") for m in messages)
    assert {line["pod"] for line in body["lines"]} == {"run-x-0"}
    ids = [line["id"] for line in body["lines"]]
    assert ids == sorted(ids)
    # Following: only the lines after one already read.
    rest = client.get(url, params={"after": ids[1]}, headers=ALICE).json()["lines"]
    assert [line["id"] for line in rest] == ids[2:]

    # A job can't flood it: past the limit, one line says so.
    monkeypatch.setattr(job_logs, "MAX_LINES", len(ids) + 1)
    with db() as session:
        job_id = session.get(Run, uuid.UUID(run["id"])).job_id
        kept = job_logs.add(session, job_id, "p", [
            {"at": T0, "level": "INFO", "message": f"line {i}"} for i in range(5)
        ])  # fmt: skip
    assert kept == 1
    tail = client.get(url, params={"after": ids[-1]}, headers=ALICE).json()["lines"]
    assert [line["message"] for line in tail] == ["line 0", job_logs.TRUNCATED]


def test_shipping_logs_keeps_order_and_never_fails_the_job():
    from src.jobs.logship import GrantLogHandler

    sent = []

    class Grant:
        fail = False

        def _grant(self, method, path, json):
            if self.fail:
                raise RuntimeError("control plane away")
            sent.append(json)

    grant = Grant()
    handler = GrantLogHandler(grant, "pod-1", every=3600)
    logger = logging.getLogger("test.logship")
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    try:
        for i in range(450):  # two full batches go as they fill
            logger.info("line %d", i)
        assert [len(b["lines"]) for b in sent] == [200, 200]
        grant.fail = True
        logger.info("lost")
        handler.flush()  # doesn't raise
        grant.fail = False
        logger.warning("after")
        handler.close()
    finally:
        logger.removeHandler(handler)
    messages = [line["message"] for b in sent for line in b["lines"]]
    assert messages[:3] == ["line 0", "line 1", "line 2"] and messages[-1] == "after"
    assert sent[-1]["pod"] == "pod-1" and sent[-1]["lines"][-1]["level"] == "WARNING"
