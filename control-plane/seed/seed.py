"""Seed a local (KinD) control plane with data to test against. Local only.

Through the API, as the local ``admin`` user (password = username, as every local
user), so seeding exercises the same paths a client uses:

- ``public``: a ``usgs_gages`` layer, ``usgs_inst_streamflow`` observations,
  ``nwm31_short_range`` and ``nwm31_medium_range_mem1`` forecasts (one series
  table) and a ``usgs_nwm31`` crosswalk. The data is teehr-core's hydro test fixture:
  three USGS gages and their NWM 3.1 reaches, a few days in October 2026.
- ``demo``: a project with ``user`` as a writer and two saved evaluations of the
  public datasets, each run once so results are there to look at.

Safe to run again: datasets are created when missing and written only while empty
(so a redeploy doesn't touch them, and runs after it stay incremental); saving an
unchanged evaluation saves nothing; runs only start for evaluations without
results.
"""

import base64
import json
import os
import sys
import time
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

HERE = Path(__file__).parent
API = os.environ.get("API_URL", "http://teehr-control-plane:8000") + "/v1"
TOKEN_URL = os.environ.get(
    "TOKEN_URL",
    "http://keycloak-service:8080/realms/teehr/protocol/openid-connect/token",
)
CLIENT_ID = os.environ.get("CLIENT_ID", "jupyterhub")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET", "local-jupyterhub-client-secret")
ARROW = "application/vnd.apache.arrow.stream"
SOURCE = "teehr-core hydro test fixture (USGS NWIS, NOAA NWM 3.1)"


def log(msg: str) -> None:
    print(msg, flush=True)


def sign_in(user: str) -> str:
    resp = httpx.post(
        TOKEN_URL,
        data={"grant_type": "password", "client_id": CLIENT_ID,
              "client_secret": CLIENT_SECRET, "username": user, "password": user},
        timeout=30,
    )  # fmt: skip
    resp.raise_for_status()
    return resp.json()["access_token"]


def subject(token: str) -> str:
    payload = token.split(".")[1]
    return json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))[
        "sub"
    ]


class Api:
    def __init__(self, user: str):
        self.user = user
        self.http = httpx.Client(base_url=API, timeout=300)
        self.sign_in()

    def sign_in(self) -> None:
        """A new token (e.g. to carry a project role just granted)."""
        self.http.headers["Authorization"] = f"Bearer {sign_in(self.user)}"

    def call(self, method: str, path: str, ok=(200, 201, 202, 204), **kwargs):
        resp = self.http.request(method, path, **kwargs)
        if resp.status_code not in ok:
            sys.exit(f"{method} {path}: HTTP {resp.status_code}: {resp.text[:2000]}")
        return resp

    def exists(self, path: str) -> bool:
        return self.call("GET", path, ok=(200, 404)).status_code == 200

    def empty(self, project: str, dataset: str) -> bool:
        resp = self.call("GET", f"/projects/{project}/datasets/{dataset}/data",
                         params={"limit": 1})  # fmt: skip
        return not resp.json()["rows"]

    def write_arrow(
        self, project: str, dataset: str, table: pa.Table, **params
    ) -> bool:
        """Write ``table`` if the dataset is empty; whether it did."""
        if not self.empty(project, dataset):
            return False
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        self.call(
            "POST", f"/projects/{project}/datasets/{dataset}/data",
            params=params, content=sink.getvalue().to_pybytes(),
            headers={"Content-Type": ARROW},
        )  # fmt: skip
        log(f"  wrote {table.num_rows} rows to {project}.{dataset}")
        return True

    def write_json(self, project: str, dataset: str, rows: list[dict]) -> None:
        """Write ``rows`` (upserted) as they are."""
        self.call("POST", f"/projects/{project}/datasets/{dataset}/data", json=rows)
        log(f"  wrote {len(rows)} rows to {project}.{dataset}")


def ensure_dataset(api: Api, project: str, body: dict) -> None:
    if api.exists(f"/projects/{project}/datasets/{body['name']}"):
        return
    api.call("POST", f"/projects/{project}/datasets", json=body)
    log(f"  registered {project}.{body['name']}")


def ready(api: Api, project: str) -> None:
    """Provision ``project`` until it's ready."""
    for _ in range(10):
        status = api.call("GET", f"/projects/{project}").json()["status"]
        if status == "ready":
            return
        api.call("POST", f"/projects/{project}/provision", ok=(200, 202, 409))
        time.sleep(3)
    sys.exit(f"project {project} isn't ready")


def seed_public(api: Api) -> None:
    log("public:")
    ready(api, "public")
    series = pq.read_table(HERE / "usgs_inst_streamflow__series.parquet")

    ensure_dataset(api, "public", {
        "type": "layer", "name": "usgs_gages", "source": SOURCE,
        "description": "USGS stream gages",
        "attributes": [{"name": "state_code", "type": "string"},
                       {"name": "drainage_area_mi2", "type": "float64"}],
    })  # fmt: skip
    gages = [
        {"location_key": r["location_key"], "name": r["name"],
         "geometry": {"type": "Point", "coordinates": [r["longitude"], r["latitude"]]},
         "state_code": r["state_code"], "drainage_area_mi2": r["drainage_area_mi2"]}
        for r in series.to_pylist()
    ]  # fmt: skip
    if api.empty("public", "usgs_gages"):
        api.write_json("public", "usgs_gages", gages)

    ensure_dataset(api, "public", {
        "type": "timeseries", "name": "usgs_inst_streamflow", "role": "observed",
        "variable": "streamflow", "statistic": "inst", "optional_columns": ["approval_status"],
        "location_layer": "usgs_gages", "source": SOURCE,
        "description": "USGS instantaneous streamflow",
    })  # fmt: skip
    obs = pq.read_table(HERE / "usgs_inst_streamflow.parquet")
    written = api.write_arrow("public", "usgs_inst_streamflow",
                              obs.select(["series_key", "value_time", "value", "approval_status"]),
                              unit="[cft_i]/s")  # fmt: skip
    if written:  # its series' locations, once the writes have added the series
        api.write_json("public", "usgs_inst_streamflow_series", [
            {"series_key": r["series_key"], "location_key": r["location_key"],
             "resolution": r["resolution"]} for r in series.to_pylist()
        ])  # fmt: skip

    nwm_series = [{"name": "feature_id", "type": "int64"}]
    ensure_dataset(api, "public", {
        "type": "timeseries", "name": "nwm31_short_range", "role": "forecast",
        "variable": "streamflow", "statistic": "inst", "source": SOURCE,
        "series_attributes": nwm_series, "description": "NWM 3.1 short-range",
    })  # fmt: skip
    ensure_dataset(api, "public", {
        "type": "timeseries", "name": "nwm31_medium_range_mem1", "role": "forecast",
        "variable": "streamflow", "statistic": "inst", "source": SOURCE,
        "series_table": "nwm31_short_range_series",
        "description": "NWM 3.1 medium-range, member 1",
    })  # fmt: skip
    written = False
    for name in ("nwm31_short_range", "nwm31_medium_range_mem1"):
        fc = pq.read_table(HERE / f"{name}.parquet")
        columns = ["series_key", "reference_time", "value_time", "value"]
        written = api.write_arrow("public", name, fc.select(columns)) or written
    if written:
        reaches = pq.read_table(HERE / "nwm31__series.parquet").to_pylist()
        api.write_json("public", "nwm31_short_range_series", reaches)

    ensure_dataset(api, "public", {
        "type": "crosswalk", "name": "usgs_nwm31", "left": "usgs_inst_streamflow",
        "right": "nwm31_short_range", "source": "USGS NLDI",
        "description": "USGS gages to their NWM 3.1 reaches",
    })  # fmt: skip
    links = pq.read_table(HERE / "usgs_nwm31_crosswalk.parquet")
    api.write_arrow("public", "usgs_nwm31", links.select(["left_key", "right_key"]))


def seed_demo(api: Api) -> None:
    log("demo:")
    if not api.exists("/projects/demo"):
        api.call("POST", "/projects", json={
            "name": "demo", "display_name": "Demo",
            "description": "Seeded evaluations of the public NWM 3.1 and USGS datasets",
        })  # fmt: skip
        log("  created demo")
    ready(api, "demo")
    user = subject(sign_in("user"))
    api.call("PUT", f"/projects/demo/members/{user}", json={"role": "write"})
    api.sign_in()  # carries the demo project role now
    for path in sorted(HERE.glob("evaluation_*.json")):
        slug = path.stem.removeprefix("evaluation_")
        saved = api.call("PUT", f"/projects/demo/evaluations/{slug}",
                         json=json.loads(path.read_text())).json()  # fmt: skip
        log(f"  {slug}: version {saved['version']}"
            + (" (saved)" if saved["changed"] else ""))  # fmt: skip
        results = f"/projects/demo/datasets/results_{slug}_v{saved['version']}"
        runs = api.call("GET", f"/projects/demo/evaluations/{slug}/runs").json()
        going = [r for r in runs if r["status"] in ("queued", "running")]
        if not api.exists(results) and not going:
            run = api.call("POST", f"/projects/demo/evaluations/{slug}/runs").json()
            log(f"  {slug}: started run {run['id']}")


def main() -> None:
    api = Api("admin")
    seed_public(api)
    seed_demo(api)
    log("seeded")


if __name__ == "__main__":
    main()
