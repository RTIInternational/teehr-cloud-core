#!/usr/bin/env python3
"""
Integration test: Phase 2 gate - data through the control-plane API (#148, #180).

Every caller is a real Keycloak user with their own token; the API reads and writes
Iceberg as that caller. Validates:
- A member registers a typed timeseries (checked against the domains) and writes
  and reads it through the API; values are converted to the dataset's unit and new
  series keys reach its series table.
- Reads are paged and pinned to one snapshot; JSON and Arrow give the same rows.
- Every write has a lineage record, naming its snapshot.
- A non-member is denied; a read-only member reads but can't write or register.
- Untyped datasets are refused to members; members can't register in `public`.
- Today's access is unchanged: the legacy `teehr` namespace (what Spark/Jupyter
  use) and teehr-api.

Leaves nothing behind: datasets and the project are removed at the end.
"""

import json
import sys
import time
import uuid

import pyarrow as pa
import requests

KEYCLOAK_TOKEN_URL = (
    "http://keycloak-service:8080/realms/teehr/protocol/openid-connect/token"
)
BASE = "http://teehr-control-plane:8000/v1"
POLARIS = "http://polaris:8181/api/catalog/v1/teehr"
TEEHR_API = "http://teehr-api:8000"
REALM = "teehr"
ARROW = "application/vnd.apache.arrow.stream"
CFS_TO_CMS = 0.028316846592
MAX_RETRIES = 30
RETRY_DELAY = 2

results: list[bool] = []


def check(label, ok, detail=""):
    results.append(bool(ok))
    print(f"  {'✅' if ok else '❌'} {label}" + (f"  [{detail}]" if detail else ""))


def user_token(username):
    resp = requests.post(
        KEYCLOAK_TOKEN_URL,
        data={
            "grant_type": "password",
            "client_id": "jupyterhub",
            "client_secret": "local-jupyterhub-client-secret",
            "username": username,
            "password": username,
            "scope": "openid",
        },
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def api(username, method, path, **kwargs):
    """Call the control plane as ``username`` (fresh token each time)."""
    headers = {"Authorization": f"Bearer {user_token(username)}"}
    headers.update(kwargs.pop("headers", {}))
    return requests.request(method, BASE + path, headers=headers, timeout=120, **kwargs)


def write(username, path, rows, **params):
    return api(
        username,
        "POST",
        path,
        params=params,
        data=json.dumps(rows),
        headers={"Content-Type": "application/json"},
    )


def wait_for_control_plane():
    for attempt in range(MAX_RETRIES):
        try:
            if requests.get(BASE.rsplit("/v1", 1)[0] + "/ready", timeout=5).ok:
                return
        except requests.RequestException:
            pass
        print(f"  Attempt {attempt + 1}/{MAX_RETRIES}: control plane not ready...")
        time.sleep(RETRY_DELAY)
    raise RuntimeError("control plane never became ready")


def rows_for(series, hours, value):
    return [
        {
            "series_key": series,
            "value_time": f"2026-01-01T{h:02d}:00:00Z",
            "value": value + h,
        }
        for h in hours
    ]


def domains_gate():
    print("\n📚 Domains")
    units = {u["symbol"] for u in api("user", "GET", "/domains/units").json()}
    check("public units are listed", {"m3/s", "[cft_i]/s"} <= units)
    periods = {p["name"] for p in api("user", "GET", "/domains/periods").json()}
    check(
        "the five shared periods are there",
        periods
        >= {"instant", "hour_starting", "hour_ending", "day_starting", "day_ending"},
    )
    r = api(
        "user",
        "POST",
        "/domains/units",
        json={"symbol": "gate_unit", "dimension": "x", "to_si": 1},
    )
    check("a user can't add a public row", r.status_code == 403, str(r.status_code))


def register_gate(p):
    print("\n🗂️  Typed registration")
    r = api(
        "user",
        "POST",
        f"/projects/{p}/datasets",
        json={
            "type": "timeseries",
            "name": "obs",
            "role": "observed",
            "variable": "streamflow",
            "statistic": "inst",
            "location_layer": "usgs_gauges",
            "series_attributes": [{"name": "basin", "type": "string"}],
        },
    )
    body = r.json()
    check(
        "a member registers a timeseries",
        r.status_code == 201 and body.get("kind") == "timeseries",
        f"{r.status_code} {r.text[:120]}",
    )
    check(
        "stored in the variable's canonical unit, keyed by series and time",
        (body.get("timeseries") or {}).get("unit") == "m3/s"
        and body.get("key_columns") == ["series_key", "value_time"],
    )
    r = api("user", "GET", f"/projects/{p}/datasets/obs_series")
    check("its series table is registered", r.ok and r.json()["kind"] == "series")
    r = api(
        "user",
        "POST",
        f"/projects/{p}/datasets",
        json={"type": "timeseries", "name": "bad", "role": "observed",
              "variable": "no_such_variable", "statistic": "inst"},
    )  # fmt: skip
    check("an unknown variable is refused", r.status_code == 422, str(r.status_code))
    r = api(
        "user",
        "POST",
        f"/projects/{p}/datasets",
        json={"name": "plain", "columns": [{"name": "x", "type": "int32"}]},
    )
    check(
        "untyped datasets are refused to members",
        r.status_code == 403,
        str(r.status_code),
    )
    r = api(
        "user",
        "POST",
        "/projects/public/datasets",
        json={"type": "timeseries", "name": "gate_obs", "role": "observed",
              "variable": "streamflow", "statistic": "inst"},
    )  # fmt: skip
    check("a member can't register in public", r.status_code == 403, str(r.status_code))


def write_read_gate(p):
    print("\n✍️  Writes and reads as the caller")
    path = f"/projects/{p}/datasets/obs/data"
    r = write("user", path, rows_for("usgs-01", range(24), 100.0), unit="[cft_i]/s")
    check(
        "a member writes in cfs",
        r.status_code == 201 and r.json().get("series_added") == 1,
        f"{r.status_code} {r.text[:150]}",
    )
    r = api("user", "GET", path, params={"limit": 1000})
    first = r.json()["rows"][0] if r.ok and r.json()["rows"] else {}
    check(
        "values are stored in m3/s",
        abs(first.get("value", 0) - 100.0 * CFS_TO_CMS) < 1e-9,
        first.get("value"),
    )

    r = write("user", path, rows_for("usgs-01", range(12), 0.0))
    check("an upsert replaces 12 values", r.status_code == 201, str(r.status_code))
    r = write("user", path, rows_for("usgs-02", range(24), 50.0))
    check("a new series is added", r.ok and r.json().get("series_added") == 1)
    full = api("user", "GET", path, params={"limit": 1000, "count": "true"}).json()
    check("48 rows in all", full.get("total") == 48, full.get("total"))
    by_hour = {
        int(x["value_time"][11:13]): x["value"]
        for x in full["rows"]
        if x["series_key"] == "usgs-01"
    }
    expected = {h: float(h) if h < 12 else (100.0 + h) * CFS_TO_CMS for h in range(24)}
    check(
        "the upsert replaced hours 0-11 and kept 12-23",
        all(abs(by_hour.get(h, -1) - v) < 1e-9 for h, v in expected.items()),
        {h: by_hour.get(h) for h in (0, 11, 12, 23)},
    )

    r = write("user", f"/projects/{p}/datasets/obs_series/data",
              [{"series_key": "usgs-01", "location_key": "01010000", "basin": "upper"}])  # fmt: skip
    check("a member writes series attributes", r.status_code == 201, str(r.status_code))
    write("user", path, rows_for("usgs-01", range(1), 7.0))
    series = {
        x["series_key"]: x
        for x in api("user", "GET", f"/projects/{p}/datasets/obs_series/data").json()[
            "rows"
        ]
    }
    check(
        "later writes leave series attributes alone",
        series.get("usgs-01", {}).get("basin") == "upper",
        series.get("usgs-01"),
    )
    check(
        "new series get the dataset's layer",
        series.get("usgs-02", {}).get("location_layer") == "usgs_gauges",
    )

    paged, cursor, snapshots = [], None, set()
    while True:
        params = {"limit": 10, **({"cursor": cursor} if cursor else {})}
        r = api("user", "GET", path, params=params)
        snapshots.add(r.headers.get("X-Snapshot-Id"))
        paged += r.json()["rows"]
        if len(snapshots) == 1 and len(paged) == 20:  # a write lands mid-read
            write("user", path, rows_for("usgs-03", range(1), 1.0))
        cursor = r.json()["next_cursor"]
        if not cursor:
            break
    check("pages of 10 give all 48 rows", len(paged) == 48, len(paged))
    check("every page read one snapshot despite a write mid-read", len(snapshots) == 1)

    r = api("user", "GET", path, params={"limit": 1000}, headers={"Accept": ARROW})
    table = pa.ipc.open_stream(pa.BufferReader(r.content)).read_all() if r.ok else None
    check("Arrow gives the same rows", table is not None and table.num_rows == 49,
          table.num_rows if table is not None else r.status_code)  # fmt: skip

    writes = api("user", "GET", f"/projects/{p}/datasets/obs/writes").json()
    check("every write has a lineage record", len(writes) == 5, len(writes))
    check(
        "the latest write names the snapshot reads see",
        writes[0]["snapshot_id"] == r.headers.get("X-Snapshot-Id"),
        f"{writes[0]['snapshot_id']} vs {r.headers.get('X-Snapshot-Id')}",
    )
    check(
        "lineage records the arriving unit",
        [w["unit"] for w in writes][-1] == "[cft_i]/s",
    )


def access_gate(p):
    print("\n🚫 Access")
    path = f"/projects/{p}/datasets/obs"
    r = api("noaccess", "GET", path + "/data")
    check("a non-member can't read", r.status_code == 404, str(r.status_code))
    r = write("noaccess", path + "/data", rows_for("x", range(1), 1.0))
    check("a non-member can't write", r.status_code == 404, str(r.status_code))
    r = api("user", "POST", f"/projects/{p}/members",
            json={"username": "noaccess", "role": "read"})  # fmt: skip
    check("the owner adds a reader", r.status_code == 201, str(r.status_code))
    r = api("noaccess", "GET", path + "/data", params={"limit": 5})
    check("the reader reads", r.status_code == 200, str(r.status_code))
    r = write("noaccess", path + "/data", rows_for("x", range(1), 1.0))
    check("the reader can't write", r.status_code == 403, str(r.status_code))
    r = api("noaccess", "POST", f"/projects/{p}/datasets",
            json={"type": "timeseries", "name": "mine", "role": "observed",
                  "variable": "streamflow", "statistic": "inst"})  # fmt: skip
    check("nor register", r.status_code == 403, str(r.status_code))


def legacy_gate():
    print("\n🏛️  Today's access is unchanged")
    headers = {
        "Authorization": f"Bearer {user_token('user')}",
        "X-Polaris-Realm": REALM,
    }
    r = requests.get(f"{POLARIS}/namespaces/teehr/tables", headers=headers, timeout=30)
    check(
        "a teehr reader still lists the teehr namespace",
        r.status_code == 200,
        str(r.status_code),
    )
    r = requests.post(
        f"{POLARIS}/namespaces/teehr/tables",
        headers=headers,
        json={"name": f"gate_{uuid.uuid4().hex[:6]}",
              "schema": {"type": "struct", "schema-id": 0, "fields": []}},
        timeout=30,
    )  # fmt: skip
    check("and still can't write it", r.status_code == 403, str(r.status_code))
    r = requests.get(f"{TEEHR_API}/health", timeout=10)
    check("teehr-api is healthy", r.status_code == 200, str(r.status_code))


def cleanup(p):
    print("\n🧹 Cleanup")
    r = api("user", "DELETE", f"/projects/{p}/datasets/obs")
    check("the owner deletes the dataset", r.status_code == 204, str(r.status_code))
    left = api("user", "GET", f"/projects/{p}/datasets").json()
    check("its series table went with it", left == [], [d["name"] for d in left])
    r = api("user", "DELETE", f"/projects/{p}")
    check("the owner deletes the project", r.status_code == 204, str(r.status_code))


def main():
    print("🧪 Phase 2 gate: data through the control-plane API")
    wait_for_control_plane()
    project = f"gate_d_{uuid.uuid4().hex[:8]}"
    r = api("user", "POST", "/projects", json={"name": project})
    if r.status_code != 201 or r.json().get("status") != "ready":
        print(f"  ❌ couldn't create a project: {r.status_code} {r.text[:200]}")
        return 1
    try:
        domains_gate()
        register_gate(project)
        write_read_gate(project)
        access_gate(project)
        legacy_gate()
    finally:
        cleanup(project)
    passed = sum(results)
    print(f"\n{passed}/{len(results)} checks passed")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
