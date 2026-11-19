"""Loading uploads (Phase 3; ADR-0014): ingest checks the request, starts a
Kubernetes Job that acts as the caller with a job grant, and the job loads the
file through the API's own write path."""

import dataclasses
import time

import pyarrow as pa
import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.config import get_settings
from src.db import get_engine
from src.delegation import DelegationError, Sealer, Tokens, get_delegation, get_sealer
from src.jobs.load import GrantClient, LoadFailed, load_upload
from src.kube import KubeError, get_kube
from src.main import app
from src.models import Job
from tests.conftest import auth, make_token
from tests.test_datasets import ALICE, add_member
from tests.test_datasets import tables as tables  # noqa: F401  (fixture)
from tests.test_uploads import BOB, UPLOADS, create, staged
from tests.test_uploads import staging as staging  # noqa: F401  (fixture)

CSV = b"""site_no,datetime,discharge,qualifiers
01010000,2026-01-01T00:00:00Z,100,P
01010000,2026-01-01T01:00:00Z,200,P
01010500,2026-01-01T00:00:00Z,50,A
"""
OBS = {"name": "obs", "unit": "[cft_i]/s", "role": "observed",
       "variable": "streamflow", "statistic": "inst"}  # fmt: skip


class FakeKube:
    def __init__(self):
        self.created: list[dict] = []
        self.deleted: list[str] = []
        self.jobs: dict[str, dict] = {}
        self.pods: dict[str, list] = {}
        self.fail = False

    def create_job(self, manifest):
        if self.fail:
            raise KubeError("forbidden")
        name = f"upload-{len(self.created)}"
        self.created.append(manifest)
        self.jobs[name] = {"status": {"active": 1}}
        return name

    def get_job(self, name):
        return self.jobs.get(name)

    def job_pods(self, name):
        return self.pods.get(name, [])

    def scale_job(self, name, pods):
        self.scaled = [*getattr(self, "scaled", []), (name, pods)]

    def delete_job(self, name):
        self.deleted.append(name)
        self.jobs.pop(name, None)


class FakeDelegation:
    """Exchanges any token; refreshing gives a real (test-signed) token for the
    caller, so the job's API calls are authenticated as them."""

    def __init__(self):
        self.exchanged: list[str] = []
        self.fail = False
        self.ended = False

    def exchange(self, user_token):
        if self.fail:
            raise DelegationError("no exchange")
        self.exchanged.append(user_token)
        return Tokens("unused", time.time() + 300, "refresh-for-alice")

    def refresh(self, refresh_token):
        if self.ended:
            raise DelegationError("session ended")
        assert refresh_token.startswith("refresh-for-")
        who = refresh_token.removeprefix("refresh-for-")
        return Tokens(make_token(who), time.time() + 300, f"refresh-for-{who}")


@pytest.fixture
def jobs(client, staging):  # noqa: F811
    settings = app.dependency_overrides[get_settings]()
    settings = dataclasses.replace(
        settings, job_image="cp:test", job_namespace="teehr-hub", job_token_key="k"
    )
    app.dependency_overrides[get_settings] = lambda: settings
    kube, delegation = FakeKube(), FakeDelegation()
    app.dependency_overrides[get_kube] = lambda: kube
    app.dependency_overrides[get_delegation] = lambda: delegation
    app.dependency_overrides[get_sealer] = lambda: Sealer("k")
    return kube, delegation


def inspected(client, staging, data=CSV, filename="obs.csv"):
    upload = create(client, filename).json()
    staged(staging, upload, data)
    client.post(f"{UPLOADS}/{upload['id']}/inspect", headers=ALICE)
    return upload["id"]


# A token issued after alice got her project role (as in real use: the job acts
# with the roles this token carries).
ALICE_NOW = auth("alice", roles=("teehr-project-flood-admin",))


def ingest(client, upload_id, dataset=None, mapping=None, headers=ALICE_NOW):
    body = {"dataset": dataset or OBS, "mapping": mapping or {}}
    return client.post(f"{UPLOADS}/{upload_id}/ingest", json=body, headers=headers)


def grant_of(manifest) -> str:
    env = manifest["spec"]["template"]["spec"]["containers"][0]["env"]
    return next(e["value"] for e in env if e["name"] == "JOB_GRANT")


def db():
    return Session(app.dependency_overrides[get_engine]())


# --- ingest -------------------------------------------------------------------


def test_ingest_starts_a_job_acting_as_the_caller(client, staging, jobs):
    kube, delegation = jobs
    upload_id = inspected(client, staging)
    resp = ingest(client, upload_id)
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["status"] == "queued"
    assert body["dataset"]["name"] == "obs" and body["dataset"]["existing"] is False
    assert body["dataset"]["optional_columns"] == ["quality"]
    assert body["mapping"]["series_key"] == "site_no"
    # The caller's own token was exchanged.
    assert delegation.exchanged == [ALICE_NOW["Authorization"].removeprefix("Bearer ")]
    manifest = kube.created[0]
    pod = manifest["spec"]["template"]["spec"]
    assert pod["automountServiceAccountToken"] is False
    assert pod["securityContext"]["runAsNonRoot"] is True
    assert pod["containers"][0]["command"][:4] == ["python", "-m", "src.jobs", "run"]
    assert manifest["spec"]["backoffLimitPerIndex"] == 0  # a failed pod: not retried
    assert grant_of(manifest).startswith("tjg_")
    with db() as session:
        job = session.scalar(select(Job))
        assert job.k8s_name == "upload-0" and job.created_by == "alice"
        # Stored encrypted, and only the grant's hash.
        assert "refresh-for-alice" not in job.refresh_token
        assert grant_of(manifest) not in job.grant_hash


@pytest.mark.parametrize(
    ("dataset", "mapping", "where"),
    [
        ({**OBS, "role": None}, {}, "dataset.role"),
        ({"name": "obs", "unit": "[cft_i]/s"}, {}, "dataset.variable"),
        ({**OBS, "unit": "mm"}, {}, "dataset.unit"),
        ({**OBS, "unit": "furlong"}, {}, "dataset.unit"),
        ({**OBS, "variable": "nope"}, {}, "dataset.variable"),
        ({**OBS, "role": "forecast"}, {}, "mapping.reference_time"),
        (OBS, {"value": "missing_col"}, "mapping.value"),
        (OBS, {"series_key_value": "x"}, "mapping.series_key"),
        (OBS, {"timezone": "Mars/Base"}, "mapping.timezone"),
    ],
)
def test_ingest_reports_what_is_wrong(client, staging, jobs, dataset, mapping, where):
    kube, _ = jobs
    upload_id = inspected(client, staging)
    resp = ingest(client, upload_id, {k: v for k, v in dataset.items() if v}, mapping)
    assert resp.status_code == 422, resp.text
    places = [".".join(e["loc"][1:]) for e in resp.json()["errors"]]
    assert where in places
    assert kube.created == []


def test_inspects_ingest_body_works_once_its_placeholders_are_replaced(
    client, staging, jobs
):
    upload = create(client, "obs.csv").json()
    staged(staging, upload, CSV.replace(b"Z,", b","))
    preview = client.post(f"{UPLOADS}/{upload['id']}/inspect", headers=ALICE).json()[
        "preview"
    ]
    body = preview["ingest"]
    assert body["dataset"]["name"] == "obs"
    assert set(body["mapping"]) == {"timezone"}
    resp = client.post(f"{UPLOADS}/{upload['id']}/ingest", json=body, headers=ALICE_NOW)
    assert resp.status_code == 422
    msg = resp.json()["errors"][0]["msg"]
    for where in ("dataset.unit", "dataset.role", "dataset.variable",
                  "dataset.statistic", "mapping.timezone"):  # fmt: skip
        assert where in msg
    body["dataset"] |= {k: v for k, v in OBS.items() if k != "name"}
    body["mapping"]["timezone"] = "America/New_York"
    resp = client.post(f"{UPLOADS}/{upload['id']}/ingest", json=body, headers=ALICE_NOW)
    assert resp.status_code == 202, resp.text


def test_times_without_a_zone_need_one(client, staging, jobs):
    upload_id = inspected(client, staging, CSV.replace(b"Z,", b","))
    resp = ingest(client, upload_id)
    assert resp.status_code == 422
    assert ingest(client, upload_id, mapping={"timezone": "UTC"}).status_code == 202


def test_loading_into_an_existing_dataset_must_fit_it(client, staging, jobs):
    client.post(
        "/v1/projects/flood/datasets",
        json={"type": "timeseries", "name": "obs", "role": "observed",
              "variable": "streamflow", "statistic": "inst"},
        headers=ALICE,
    )  # fmt: skip
    upload_id = inspected(client, staging)
    resp = ingest(client, upload_id, {"name": "obs", "unit": "[cft_i]/s"})
    assert resp.status_code == 422  # the file has qualifiers; obs has no quality
    resp = ingest(client, upload_id, {"name": "obs", "unit": "[cft_i]/s"},
                  {"quality": None})  # fmt: skip
    assert resp.status_code == 202, resp.text
    assert resp.json()["dataset"]["existing"] is True


def test_readers_cant_ingest(client, staging, jobs):
    upload_id = inspected(client, staging)
    add_member(client, "bob", "read")
    assert ingest(client, upload_id, headers=BOB).status_code == 403


def test_ingest_before_inspect(client, staging, jobs):
    upload = create(client).json()
    assert ingest(client, upload["id"]).status_code == 409


def test_no_token_exchange_no_job(client, staging, jobs):
    kube, delegation = jobs
    delegation.fail = True
    upload_id = inspected(client, staging)
    assert ingest(client, upload_id).status_code == 502
    assert kube.created == []
    assert (
        client.get(f"{UPLOADS}/{upload_id}", headers=ALICE).json()["status"]
        == "inspected"
    )


def test_kubernetes_refusing_leaves_the_upload_as_it_was(client, staging, jobs):
    kube, _ = jobs
    kube.fail = True
    upload_id = inspected(client, staging)
    assert ingest(client, upload_id).status_code == 502
    assert (
        client.get(f"{UPLOADS}/{upload_id}", headers=ALICE).json()["status"]
        == "inspected"
    )
    with db() as session:
        assert session.scalar(select(Job)) is None


def test_jobs_per_project_are_capped(client, staging, jobs):
    for _ in range(2):
        assert ingest(client, inspected(client, staging)).status_code == 202
    assert ingest(client, inspected(client, staging)).status_code == 429


def test_jobs_not_configured(client, staging):  # noqa: F811
    upload_id = inspected(client, staging)
    assert ingest(client, upload_id).status_code == 503


# --- the job's grant ----------------------------------------------------------


def grant_call(client, grant, method, path, **kwargs):
    return client.request(
        method,
        f"/v1/job-grants/{path}",
        headers={"Authorization": f"Bearer {grant}"},
        **kwargs,
    )


def test_a_grant_gets_the_task_and_tokens_until_the_job_ends(client, staging, jobs):
    kube, _ = jobs
    upload_id = inspected(client, staging)
    ingest(client, upload_id)
    grant = grant_of(kube.created[0])
    task = grant_call(client, grant, "GET", "task").json()
    assert task["kind"] == "upload" and task["project"] == "flood"
    assert task["upload"]["download_url"].endswith("?get")
    assert (
        client.get(f"{UPLOADS}/{upload_id}", headers=ALICE).json()["status"]
        == "running"
    )
    token = grant_call(client, grant, "POST", "token").json()
    assert token["expires_in"] > 0
    # The token is the caller's: the API accepts it as alice.
    me = client.get(
        "/v1/projects/flood",
        headers={"Authorization": f"Bearer {token['access_token']}"},
    )
    assert me.status_code == 200
    grant_call(client, grant, "POST", "progress", json={"rows_done": 2})
    assert client.get(f"{UPLOADS}/{upload_id}", headers=ALICE).json()["rows_done"] == 2
    resp = grant_call(
        client,
        grant,
        "POST",
        "finish",
        json={"status": "succeeded", "result": {"rows": 3}},
    )
    assert resp.status_code == 204
    upload = client.get(f"{UPLOADS}/{upload_id}", headers=ALICE).json()
    assert upload["status"] == "succeeded" and upload["result"] == {"rows": 3}
    # A finished job's grant is dead, and its stored token gone.
    assert grant_call(client, grant, "POST", "token").status_code == 401
    with db() as session:
        assert session.scalar(select(Job)).refresh_token is None


def test_unknown_grants_and_user_tokens_are_refused(client, staging, jobs):
    assert grant_call(client, "tjg_nope", "GET", "task").status_code == 401
    user_token = ALICE["Authorization"].removeprefix("Bearer ")
    assert grant_call(client, user_token, "GET", "task").status_code == 401


def test_a_token_without_the_project_role_is_refused_up_front(client, staging, jobs):
    kube, delegation = jobs
    upload_id = inspected(client, staging)
    resp = ingest(client, upload_id, headers=ALICE)  # from before the project
    assert resp.status_code == 403
    assert resp.json()["code"] == "token_refresh_required"
    assert kube.created == [] and delegation.exchanged == []
    assert ingest(client, upload_id).status_code == 202


def test_session_ended(client, staging, jobs):
    kube, delegation = jobs
    ingest(client, inspected(client, staging))
    delegation.ended = True
    resp = grant_call(client, grant_of(kube.created[0]), "POST", "token")
    assert resp.status_code == 401 and resp.json()["code"] == "session_ended"


def test_a_job_whose_pod_died_fails_with_the_reason(client, staging, jobs):
    kube, _ = jobs
    upload_id = inspected(client, staging)
    ingest(client, upload_id)
    kube.jobs["upload-0"] = {"status": {"failed": 1}}
    kube.pods["upload-0"] = [
        {"status": {"containerStatuses": [
            {"state": {"terminated": {"reason": "OOMKilled", "exitCode": 137}}}]}}
    ]  # fmt: skip
    upload = client.get(f"{UPLOADS}/{upload_id}", headers=ALICE).json()
    assert upload["status"] == "failed"
    assert "running out of memory" in upload["error"]
    # Failed loads can be retried.
    assert ingest(client, upload_id).status_code == 202


def test_cancel(client, staging, jobs):
    kube, _ = jobs
    upload_id = inspected(client, staging)
    ingest(client, upload_id)
    resp = client.post(f"{UPLOADS}/{upload_id}/cancel", headers=ALICE)
    assert resp.status_code == 200 and resp.json()["status"] == "cancelled"
    assert kube.deleted == ["upload-0"]
    assert (
        grant_call(client, grant_of(kube.created[0]), "GET", "task").status_code == 401
    )
    assert (
        client.post(f"{UPLOADS}/{upload_id}/cancel", headers=ALICE).status_code == 409
    )


def test_cancel_without_kubernetes_is_refused(client, staging, jobs):
    upload_id = inspected(client, staging)
    ingest(client, upload_id)
    app.dependency_overrides[get_kube] = lambda: None
    resp = client.post(f"{UPLOADS}/{upload_id}/cancel", headers=ALICE)
    assert resp.status_code == 503
    assert client.get(f"{UPLOADS}/{upload_id}", headers=ALICE).json()["status"] in (
        "queued", "running"
    )  # fmt: skip


def test_a_failed_commit_deletes_the_job_it_started(client, staging, jobs, monkeypatch):
    kube, _ = jobs
    upload_id = inspected(client, staging)

    def boom(self):
        raise RuntimeError("database went away")

    monkeypatch.setattr(Session, "commit", boom)
    with pytest.raises(RuntimeError):
        ingest(client, upload_id)
    monkeypatch.undo()
    assert kube.deleted == ["upload-0"]
    assert client.get(f"{UPLOADS}/{upload_id}", headers=ALICE).json()["status"] == (
        "inspected"
    )


# --- the load, end to end (in-process) -------------------------------------------


class InProcess(GrantClient):
    """The job's client, against the app in-process; downloads from fake staging."""

    def __init__(self, client, grant, staging):  # noqa: F811
        super().__init__("http://testserver", grant, http=client)
        self.staging = staging

    def download(self, url, file):
        key = url.removeprefix("https://staging.test/").removesuffix("?get")
        file.write(self.staging.objects[key])


def run_load(client, staging, kube):  # noqa: F811
    job = InProcess(client, grant_of(kube.created[-1]), staging)
    task = job.task()
    try:
        result = load_upload(job, task)
    except LoadFailed as exc:
        job.finish("failed", problems=exc.problems, error=str(exc))
        return None
    job.finish("succeeded", result=result)
    return result


def test_load_registers_and_writes_as_the_caller(client, staging, jobs, tables):  # noqa: F811
    kube, _ = jobs
    upload_id = inspected(client, staging)
    ingest(client, upload_id)
    result = run_load(client, staging, kube)
    assert result == {"dataset": "obs", "created": True, "rows": 3, "series_added": 2,
                      "snapshot_id": result["snapshot_id"]}  # fmt: skip
    dataset = client.get("/v1/projects/flood/datasets/obs", headers=ALICE).json()
    assert dataset["timeseries"]["optional_columns"] == ["quality"]
    # Written through the API: cfs converted to m3/s, keys kept as text.
    mode, _, data = tables.writes[-1]
    assert mode == "upsert"
    assert data.column("series_key").to_pylist() == ["01010000", "01010000", "01010500"]
    assert data.column("value").to_pylist() == pytest.approx(
        [100 * 0.028316846592, 200 * 0.028316846592, 50 * 0.028316846592]
    )
    writes = client.get("/v1/projects/flood/datasets/obs/writes", headers=ALICE).json()
    assert [w["unit"] for w in writes] == ["[cft_i]/s"]
    assert writes[0]["written_by"] == "alice"
    upload = client.get(f"{UPLOADS}/{upload_id}", headers=ALICE).json()
    assert upload["status"] == "succeeded" and upload["rows_done"] == 3


def test_load_checks_the_whole_file_before_writing(client, staging, jobs, tables):  # noqa: F811
    kube, _ = jobs
    bad = CSV + b"01010000,2026-01-01T00:00:00Z,999,P\n"  # a repeated key
    upload_id = inspected(client, staging, bad)
    ingest(client, upload_id)
    assert run_load(client, staging, kube) is None
    upload = client.get(f"{UPLOADS}/{upload_id}", headers=ALICE).json()
    assert upload["status"] == "failed"
    assert "more than once" in upload["problems"][0]
    # Nothing registered, nothing written.
    assert (
        client.get("/v1/projects/flood/datasets/obs", headers=ALICE).status_code == 404
    )
    assert not getattr(tables, "writes", [])


def test_load_refuses_quantiles_past_four_places(client, staging, jobs, tables):  # noqa: F811
    kube, _ = jobs
    data = (b"location_id,issue_time,valid_time,quantile,value\n"
            b"A,2026-01-01T00:00:00Z,2026-01-01T01:00:00Z,0.12345,1.0\n")  # fmt: skip
    upload_id = inspected(client, staging, data, "q.csv")
    resp = ingest(
        client, upload_id, {**OBS, "name": "q", "role": "forecast", "unit": "m3/s"}
    )
    assert resp.status_code == 202, resp.text
    assert run_load(client, staging, kube) is None
    problems = client.get(f"{UPLOADS}/{upload_id}", headers=ALICE).json()["problems"]
    assert problems == ["column 'quantile': values must have at most 4 decimal places"]


def test_load_one_series_from_parquet(client, staging, jobs, tables):  # noqa: F811
    import io

    import pyarrow.parquet as pq

    kube, _ = jobs
    times = pa.array([0, 3_600_000_000], pa.timestamp("us", tz="UTC"))
    sink = io.BytesIO()
    pq.write_table(pa.table({"value_time": times, "flow_cms": [1.0, 2.0]}), sink)
    upload_id = inspected(client, staging, sink.getvalue(), "g.parquet")
    resp = ingest(client, upload_id, {**OBS, "name": "g", "unit": "m3/s"},
                  {"series_key_value": "gauge-1"})  # fmt: skip
    assert resp.status_code == 202, resp.text
    assert run_load(client, staging, kube)["rows"] == 2
    _, _, data = tables.writes[-1]
    assert data.column("series_key").to_pylist() == ["gauge-1", "gauge-1"]


def test_load_sets_each_series_location(client, staging, jobs, tables):  # noqa: F811
    kube, _ = jobs
    client.post(
        "/v1/projects/flood/datasets",
        json={"type": "layer", "name": "gauges"},
        headers=ALICE,
    )
    client.post(
        "/v1/projects/flood/datasets/gauges/data",
        json=[{"location_key": "01010000", "geometry": "POINT (-69.7 46.7)"}],
        headers=ALICE,
    )
    upload_id = inspected(client, staging)
    resp = ingest(
        client,
        upload_id,
        {**OBS, "location_layer": "gauges"},
        {"location_key": "site_no"},  # the site number is both keys
    )
    assert resp.status_code == 202, resp.text
    result = run_load(client, staging, kube)
    assert result["locations_set"] == 2
    assert result["notes"] == ["1 of 2 location keys aren't in gauges: 01010500"]
    series = tables.rows[("flood", "obs_series")]
    assert series["01010000"]["location_key"] == "01010000"
    assert series["01010000"]["location_layer"] == "gauges"


def test_location_keys_need_a_layer(client, staging, jobs):
    upload_id = inspected(client, staging)
    resp = ingest(client, upload_id, mapping={"location_key": "site_no"})
    assert resp.status_code == 422
    places = [".".join(e["loc"][1:]) for e in resp.json()["errors"]]
    assert "mapping.location_key" in places
    resp = ingest(
        client,
        upload_id,
        {**OBS, "location_layer": "nope"},
        {"location_key": "site_no"},
    )
    places = [".".join(e["loc"][1:]) for e in resp.json()["errors"]]
    assert "dataset.location_layer" in places


def test_exchange_uses_its_own_client_when_configured():
    import dataclasses

    from src.config import Settings
    from src.delegation import KeycloakDelegation

    base = Settings(database_url="sqlite://", keycloak_token_url="http://kc/token",
                    client_id="cp", client_secret="s1")  # fmt: skip
    assert KeycloakDelegation(base)._client == ("cp", "s1")
    split = dataclasses.replace(
        base, exchange_client_id="cp-jobs", exchange_client_secret="s2"
    )
    assert KeycloakDelegation(split)._client == ("cp-jobs", "s2")


# --- the shared upload contract: wide files, time_format, GeoArrow ---------------

WIDE = b"""datetime,01010000,01010500
2026-01-01T00:00:00Z,100,50
2026-01-01T01:00:00Z,110,
"""


def test_inspect_proposes_wide_for_value_columns_without_a_key(client, staging, jobs):
    upload = create(client, "wide.csv").json()
    staged(staging, upload, WIDE)
    preview = client.post(f"{UPLOADS}/{upload['id']}/inspect", headers=ALICE).json()[
        "preview"
    ]
    mapping = preview["mapping"]
    assert mapping["layout"] == "wide"
    assert mapping["series_columns"] == ["01010000", "01010500"]
    assert mapping["series_key"] is None and mapping["value"] is None
    assert not any("series_key" in n or n == "mapping.value" for n in preview["needed"])


def test_load_a_wide_file(client, staging, jobs, tables):  # noqa: F811
    kube, _ = jobs
    upload_id = inspected(client, staging, WIDE, "wide.csv")
    resp = ingest(client, upload_id, {**OBS, "unit": "m3/s"})
    assert resp.status_code == 202, resp.text
    result = run_load(client, staging, kube)
    assert result["rows"] == 4 and result["series_added"] == 2
    _, _, data = tables.writes[-1]
    rows = sorted(zip(data["series_key"].to_pylist(), data["value"].to_pylist(),
                      strict=True), key=lambda r: (r[0], r[1] is None))  # fmt: skip
    assert rows == [("01010000", 100.0), ("01010000", 110.0),
                    ("01010500", 50.0), ("01010500", None)]  # fmt: skip


def test_wide_mapping_problems(client, staging, jobs):
    upload_id = inspected(client, staging, WIDE, "wide.csv")
    resp = ingest(
        client,
        upload_id,
        mapping={"series_columns": ["01010000", "nope"], "location_key": "datetime"},
    )
    places = [".".join(e["loc"][1:]) for e in resp.json()["errors"]]
    assert "mapping.series_columns.1" in places and "mapping.location_key" in places


NON_ISO = b"""site_no,datetime,discharge
A,01/02/2026 13:00,1.0
A,01/02/2026 14:00,2.0
"""


def test_non_iso_times_need_a_time_format(client, staging, jobs, tables):  # noqa: F811
    kube, _ = jobs
    upload = create(client, "obs.csv").json()
    staged(staging, upload, NON_ISO)
    preview = client.post(f"{UPLOADS}/{upload['id']}/inspect", headers=ALICE).json()[
        "preview"
    ]
    assert "mapping.time_format" in preview["needed"]
    mapping = {"timezone": "UTC"}
    resp = ingest(client, upload["id"], OBS, mapping)
    assert "mapping.time_format" in [
        ".".join(e["loc"][1:]) for e in resp.json()["errors"]
    ]
    resp = ingest(client, upload["id"], OBS, {**mapping, "time_format": "%Y-%m-%d"})
    assert resp.status_code == 422 and "doesn't parse 'datetime'" in resp.text
    resp = ingest(
        client, upload["id"], OBS, {**mapping, "time_format": "%m/%d/%Y %H:%M"}
    )
    assert resp.status_code == 202, resp.text
    run_load(client, staging, kube)
    _, _, data = tables.writes[-1]
    assert [t.isoformat() for t in data["value_time"].to_pylist()] == [
        "2026-01-02T13:00:00+00:00", "2026-01-02T14:00:00+00:00"
    ]  # fmt: skip


def test_a_set_shard_size_reaches_the_job_pod():
    from src.config import Settings
    from src.kube import job_manifest

    def env(settings):
        pod = job_manifest(settings, "run", "j" * 36, "tjg_x")["spec"]["template"]
        return {e["name"]: e["value"] for e in pod["spec"]["containers"][0]["env"]}

    base = Settings(database_url="sqlite://")
    assert "RUN_ROWS_PER_SHARD" not in env(base)  # sized from the pod's memory
    sized = dataclasses.replace(base, run_rows_per_shard=5_000_000)
    assert env(sized)["RUN_ROWS_PER_SHARD"] == "5000000"


def test_job_pod_sizing_settings_reach_the_pod():
    from src.config import Settings
    from src.kube import job_manifest

    def container(settings):
        pod = job_manifest(settings, "run", "j" * 36, "tjg_x")["spec"]["template"]
        return pod["spec"]["containers"][0]

    base = Settings(database_url="sqlite://")
    assert "cpu" not in container(base)["resources"]["requests"]
    assert "JOB_THREADS" not in {e["name"] for e in container(base)["env"]}
    sized = container(dataclasses.replace(base, job_cpu_request="2", job_threads=8))
    assert sized["resources"]["requests"]["cpu"] == "2"
    assert {e["name"]: e["value"] for e in sized["env"]}["JOB_THREADS"] == "8"


def test_a_runs_job_is_pod_zeros_other_pods_dont_end_it():
    from src.kube import job_state

    class Kube:
        def __init__(self, status):
            self.job = {"spec": {"completionMode": "Indexed"}, "status": status}

        def get_job(self, name):
            return self.job

        def job_pods(self, name):
            return []

    def phase(**status):
        return job_state(Kube(status), "run-x").phase

    assert phase(active=3) == "active"
    assert phase(completedIndexes="1-2", succeeded=2, active=1) == "active"
    assert phase(failedIndexes="2", failed=1, active=2) == "active"
    assert phase(completedIndexes="0", succeeded=1, active=2) == "succeeded"
    assert phase(failedIndexes="0,3", failed=2) == "failed"
    conditions = [{"type": "Failed", "status": "True"}]
    assert phase(conditions=conditions, failedIndexes="1") == "failed"


def test_a_job_pod_is_indexed_and_starts_alone():
    from src.config import Settings
    from src.kube import job_manifest

    spec = job_manifest(Settings(database_url="sqlite://"), "run", "j" * 36, "tjg_x")
    spec = spec["spec"]
    assert (spec["completionMode"], spec["completions"], spec["parallelism"]) == (
        "Indexed", 1, 1
    )  # fmt: skip
    assert spec["backoffLimitPerIndex"] == 0 and "backoffLimit" not in spec
