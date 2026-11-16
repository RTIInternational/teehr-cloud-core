#!/usr/bin/env python3
"""
Integration test: Phase 3 gate - user data upload and registration (#148).

A real Keycloak user stages files with presigned URLs, inspects them and loads
them; every load is a Kubernetes Job acting as that user (ADR-0014). Validates:
- Timeseries: inspect proposes a mapping and an ingest body; the load converts
  units, adds series and sets each series' location in a layer.
- Layers from GeoJSON, a zipped Shapefile in another coordinate system
  (reprojected), one layer of a two-layer GeoPackage, and CSV points (codes keep
  their leading zeros); read back as GeoJSON.
- A crosswalk between two timeseries; keys not in a series table are noted.
- Bad files fail before anything is written: repeated keys, projected coordinates
  in a "longitude/latitude" file.
- Ingest refuses an unfinished body (placeholders, missing fields) all at once.
- Loads write as the caller (write history); a load can be cancelled; a project
  can't be deleted while one of its uploads is loading.
- A reader can list uploads but not create, inspect or load them; a non-member
  can't see them.

Sample files are built here (same data as control-plane/examples/uploads).
Leaves nothing behind: datasets and the project are removed at the end.
"""

import os
import sys
import tempfile
import time
import uuid
import zipfile
from urllib.parse import urlsplit, urlunsplit

import pyarrow as pa
import pyogrio
import pyproj
import requests
import shapely

KEYCLOAK_TOKEN_URL = (
    "http://keycloak-service:8080/realms/teehr/protocol/openid-connect/token"
)
BASE = "http://teehr-control-plane:8000/v1"
# Upload URLs are signed for the public S3 address; in the cluster the same
# request goes to the store directly, with the signed Host header.
STAGING_INTERNAL = "http://local-s3:9000"
CFS_TO_CMS = 0.028316846592
MAX_RETRIES = 30
RETRY_DELAY = 2

GAUGES = [
    ("01010000", "St. John River at Ninemile Bridge, ME", -69.7156, 46.7006),
    ("01010500", "St. John River at Dickey, ME", -69.0884, 47.1131),
    ("02087500", "Neuse River near Clayton, NC", -78.4058, 35.6473),
]
HUCS = ["01010001", "01010002", "03020201"]

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


def put(url: str, data: bytes) -> requests.Response:
    parts = urlsplit(url)
    internal = urlsplit(STAGING_INTERNAL)
    target = urlunsplit((internal.scheme, internal.netloc, parts.path, parts.query, ""))
    return requests.put(target, data=data, headers={"Host": parts.netloc}, timeout=60)


# --- sample files ------------------------------------------------------------------


def timeseries_csv(hours=4) -> bytes:
    lines = ["site_no,datetime,discharge"]
    for k, (site, *_) in enumerate(GAUGES):
        for h in range(hours):
            lines.append(f"{site},2026-01-01T{h:02d}:00:00Z,{100 + 10 * k + h}")
    return ("\n".join(lines) + "\n").encode()


def big_csv(rows=300_000) -> bytes:
    lines = ["site_no,datetime,discharge"]
    for i in range(rows):
        lines.append(f"S{i % 50:04d},2026-01-{1 + (i // 50) % 28:02d}T"
                     f"{(i // 1400) % 24:02d}:{(i // 50) % 60:02d}:00Z,{i}")  # fmt: skip
    return ("\n".join(lines) + "\n").encode()


def gauges_table(crs="EPSG:4326") -> pa.Table:
    xy = [(lon, lat) for _, _, lon, lat in GAUGES]
    if crs != "EPSG:4326":
        move = pyproj.Transformer.from_crs("EPSG:4326", crs, always_xy=True)
        xy = [move.transform(x, y) for x, y in xy]
    return pa.table(
        {
            "site_no": [g[0] for g in GAUGES],
            "station_nm": [g[1] for g in GAUGES],
            "huc_cd": HUCS,
            "geometry": pa.array([shapely.Point(x, y).wkb for x, y in xy]),
        }
    )


def geo_file(kind: str) -> bytes:
    with tempfile.TemporaryDirectory() as d:
        if kind == "geojson":
            path = os.path.join(d, "g.geojson")
            pyogrio.write_arrow(gauges_table(), path, driver="GeoJSON",
                                geometry_name="geometry", geometry_type="Point",
                                crs="EPSG:4326")  # fmt: skip
        elif kind == "shapefile":
            pyogrio.write_arrow(gauges_table("EPSG:5070"), os.path.join(d, "g.shp"),
                                geometry_name="geometry", geometry_type="Point",
                                crs="EPSG:5070")  # fmt: skip
            path = os.path.join(d, "g.zip")
            with zipfile.ZipFile(path, "w") as z:
                for ext in ("shp", "shx", "dbf", "prj", "cpg"):
                    part = os.path.join(d, f"g.{ext}")
                    if os.path.exists(part):
                        z.write(part, f"g.{ext}")
        else:
            path = os.path.join(d, "g.gpkg")
            pyogrio.write_arrow(gauges_table(), path, layer="gauges",
                                geometry_name="geometry", geometry_type="Point",
                                crs="EPSG:4326")  # fmt: skip
            basins = pa.table({
                "huc8": HUCS,
                "geometry": pa.array([
                    shapely.box(lon - 0.5, lat - 0.3, lon + 0.5, lat + 0.3).wkb
                    for _, _, lon, lat in GAUGES]),
            })  # fmt: skip
            pyogrio.write_arrow(basins, path, layer="basins", geometry_name="geometry",
                                geometry_type="Polygon", crs="EPSG:4326")  # fmt: skip
        with open(path, "rb") as f:
            return f.read()


POINTS_CSV = (
    "site_no,station_nm,dec_lat_va,dec_long_va,huc_cd\n"
    + "".join(f'{g[0]},"{g[1]}",{g[3]},{g[2]},{h}\n' for g, h in zip(GAUGES, HUCS))
).encode()
CROSSWALK_CSV = (
    b"primary_location_id,secondary_location_id,weight\n"
    b"01010000,724696,1.0\n01010500,724838,1.0\n02087500,8731016,1.0\n"
)


# --- steps -------------------------------------------------------------------------


def stage(p, filename, kind, data, user="user"):
    r = api(user, "POST", f"/projects/{p}/uploads",
            json={"filename": filename, "kind": kind})  # fmt: skip
    if r.status_code != 201:
        return None, r
    body = r.json()
    put(body["upload_url"], data).raise_for_status()
    return body["id"], r


def inspect(p, upload_id, **params):
    return api("user", "POST", f"/projects/{p}/uploads/{upload_id}/inspect",
               params=params)  # fmt: skip


def ingest(p, upload_id, body):
    return api("user", "POST", f"/projects/{p}/uploads/{upload_id}/ingest", json=body)


def wait(p, upload_id, timeout=240):
    end = time.time() + timeout
    while time.time() < end:
        u = api("user", "GET", f"/projects/{p}/uploads/{upload_id}").json()
        if u["status"] in ("succeeded", "failed", "cancelled"):
            return u
        time.sleep(3)
    return u


def load(p, filename, kind, data, body, **inspect_params):
    upload_id, _ = stage(p, filename, kind, data)
    preview = inspect(p, upload_id, **inspect_params).json()["preview"]
    r = ingest(p, upload_id, body)
    if r.status_code != 202:
        return preview, {"status": f"ingest {r.status_code}", "error": r.text[:300]}
    return preview, wait(p, upload_id)


def rows(p, name, **params):
    return api("user", "GET", f"/projects/{p}/datasets/{name}/data",
               params={"limit": 1000, **params}).json()["rows"]  # fmt: skip


def layers_gate(p):
    print("\n🗺️  Layers")
    preview, u = load(p, "gauges.geojson", "layer", geo_file("geojson"),
                      {"dataset": {"name": "gauges"}})  # fmt: skip
    m = preview["mapping"]
    check("GeoJSON: inspect maps key, name, geometry, EPSG:4326",
          (m["location_key"], m["name"], m["crs"]) == ("site_no", "station_nm",
                                                       "EPSG:4326"), m)  # fmt: skip
    check("…and loads", u["status"] == "succeeded", u.get("problems") or u.get("error"))

    preview, u = load(p, "gauges.zip", "layer", geo_file("shapefile"),
                      {"dataset": {"name": "gauges_albers"}})  # fmt: skip
    check("zipped Shapefile in EPSG:5070: inspected in place",
          preview["mapping"]["crs"] == "EPSG:5070", preview["mapping"].get("crs"))
    check("…loads reprojected", u["status"] == "succeeded", u.get("problems"))
    got = {r["location_key"]: r["geometry"]["coordinates"]
           for r in rows(p, "gauges_albers")}  # fmt: skip
    lon, lat = got.get("01010000", (0, 0))
    check("…back to longitude/latitude",
          abs(lon + 69.7156) < 1e-4 and abs(lat - 46.7006) < 1e-4, (lon, lat))

    preview, u = load(p, "g.gpkg", "layer", geo_file("geopackage"),
                      {"dataset": {"name": "basins"},
                       "mapping": {"source_layer": "basins"}},
                      source_layer="basins")  # fmt: skip
    check("GeoPackage: both layers listed",
          preview.get("layers") == ["gauges", "basins"], preview.get("layers"))
    check("…its basins layer loads", u["status"] == "succeeded", u.get("problems"))
    fc = api("user", "GET", f"/projects/{p}/datasets/basins/data",
             headers={"Accept": "application/geo+json"}).json()  # fmt: skip
    check("…read back as GeoJSON polygons",
          {f["geometry"]["type"] for f in fc.get("features", [])} == {"Polygon"})

    preview, u = load(p, "points.csv", "layer", POINTS_CSV,
                      {"dataset": {"name": "gauge_points"}})  # fmt: skip
    check("CSV points: loads", u["status"] == "succeeded", u.get("problems"))
    hucs = sorted(r["huc_cd"] for r in rows(p, "gauge_points"))
    check("…HUC codes keep their leading zeros", hucs[:1] == ["01010001"], hucs)

    bad = b"site_no,lat,lon\n01,46.7,-69.7\n02,2860000.0,2040000.0\n"
    _, u = load(p, "projected.csv", "layer", bad, {"dataset": {"name": "bad_points"}})
    check("projected coordinates fail the load",
          u["status"] == "failed" and "EPSG:4326" in (u["problems"] or [""])[0],
          u.get("problems"))
    r = api("user", "GET", f"/projects/{p}/datasets/bad_points")
    check("…before anything is registered", r.status_code == 404, str(r.status_code))


def timeseries_gate(p):
    print("\n📈 Timeseries")
    upload_id, _ = stage(p, "obs.csv", "timeseries", timeseries_csv())
    preview = inspect(p, upload_id).json()["preview"]
    check("inspect proposes the mapping",
          (preview["mapping"]["series_key"], preview["mapping"]["value"])
          == ("site_no", "discharge"), preview["mapping"])
    body = preview["ingest"]
    r = ingest(p, upload_id, body)
    msg = " ".join(e["msg"] for e in r.json().get("errors", []))
    check("its ingest body, unedited, is refused (placeholders)",
          r.status_code == 422 and "placeholders" in msg, msg[:120])
    r = ingest(p, upload_id, {"dataset": {"name": "obs", "unit": "[cft_i]/s"}})
    where = {".".join(e["loc"][1:]) for e in r.json().get("errors", [])}
    check("a new dataset without role/variable/statistic: all reported",
          {"dataset.role", "dataset.variable", "dataset.statistic"} <= where, where)
    r = ingest(p, upload_id, {
        "dataset": {"name": "obs", "unit": "[cft_i]/s", "role": "observed",
                    "variable": "streamflow", "statistic": "inst",
                    "location_layer": "gauges"},
        "mapping": {"location_key": "site_no"}})  # fmt: skip
    check("ingest starts the load", r.status_code == 202, r.text[:120])
    u = wait(p, upload_id)
    result = u.get("result") or {}
    check("the load succeeds", u["status"] == "succeeded", u.get("problems"))
    check("…12 rows, 3 new series, each located in the layer",
          (result.get("rows"), result.get("series_added"), result.get("locations_set"))
          == (12, 3, 3), result)  # fmt: skip
    first = min(rows(p, "obs"), key=lambda r: (r["series_key"], r["value_time"]))
    check("cfs converted to m3/s",
          abs(first["value"] - 100 * CFS_TO_CMS) < 1e-9, first["value"])
    series = {r["series_key"]: r for r in rows(p, "obs_series")}
    check("series point at their location",
          series.get("01010000", {}).get("location_key") == "01010000"
          and series["01010000"].get("location_layer") == "gauges")
    writes = api("user", "GET", f"/projects/{p}/datasets/obs/writes").json()
    check("written by the caller, through the API",
          bool(writes) and all(w["written_by"] for w in writes)
          and writes[0]["unit"] == "[cft_i]/s", writes[:1])

    dup = b"site_no,datetime,discharge\n01,2026-01-01T00:00:00Z,1\n" \
          b"01,2026-01-01T00:00:00Z,2\n"  # fmt: skip
    _, u = load(p, "dup.csv", "timeseries", dup, {"dataset": {"name": "obs",
                                                              "unit": "m3/s"}})
    check("repeated keys fail the load",
          u["status"] == "failed" and "more than once" in (u["problems"] or [""])[0],
          u.get("problems"))
    count = len(rows(p, "obs"))
    check("…and write nothing", count == 12, count)


def crosswalk_gate(p):
    print("\n🔗 Crosswalk")
    r = api("user", "POST", f"/projects/{p}/datasets", json={
        "type": "timeseries", "name": "nwm", "role": "simulated",
        "variable": "streamflow", "statistic": "inst"})  # fmt: skip
    check("a second timeseries", r.status_code == 201, r.text[:120])
    preview, u = load(p, "xw.csv", "crosswalk", CROSSWALK_CSV,
                      {"dataset": {"name": "usgs_nwm", "left": "obs",
                                   "right": "nwm"}})  # fmt: skip
    check("inspect maps teehr's crosswalk columns",
          (preview["mapping"]["left_key"], preview["mapping"]["right_key"])
          == ("primary_location_id", "secondary_location_id"), preview["mapping"])
    check("the crosswalk loads", u["status"] == "succeeded", u.get("problems"))
    notes = (u.get("result") or {}).get("notes") or []
    check("NWM keys not in its series table are noted, not refused",
          notes == ["3 of 3 right_key values aren't in nwm_series: "
                    "724696, 724838, 8731016"], notes)  # fmt: skip


def cancel_gate(p):
    print("\n⏹️  Cancel")
    upload_id, _ = stage(p, "big.csv", "timeseries", big_csv())
    inspect(p, upload_id)
    r = ingest(p, upload_id, {"dataset": {"name": "big", "unit": "m3/s",
                                          "role": "observed", "variable": "streamflow",
                                          "statistic": "inst"}})  # fmt: skip
    check("a big load starts", r.status_code == 202, r.text[:120])
    r = api("user", "DELETE", f"/projects/{p}")
    check("the project can't be deleted while it loads",
          r.status_code == 409 and "loading" in r.text, r.text[:120])
    r = api("user", "POST", f"/projects/{p}/uploads/{upload_id}/cancel")
    check("it's cancelled", r.status_code == 200 and r.json()["status"] == "cancelled",
          r.text[:120])


def access_gate(p):
    print("\n🚫 Access")
    r = api("noaccess", "GET", f"/projects/{p}/uploads")
    check("a non-member can't see uploads", r.status_code == 404, str(r.status_code))
    r = api("user", "POST", f"/projects/{p}/members",
            json={"username": "noaccess", "role": "read"})  # fmt: skip
    check("the owner adds a reader", r.status_code == 201, str(r.status_code))
    r = api("noaccess", "GET", f"/projects/{p}/uploads")
    check("the reader lists uploads", r.status_code == 200, str(r.status_code))
    upload_id = r.json()[0]["id"] if r.ok and r.json() else uuid.uuid4()
    _, r = stage(p, "mine.csv", "timeseries", timeseries_csv(), user="noaccess")
    check("but can't create one", r.status_code == 403, str(r.status_code))
    r = api("noaccess", "POST", f"/projects/{p}/uploads/{upload_id}/inspect")
    check("…or inspect one", r.status_code == 403, str(r.status_code))
    r = api("noaccess", "POST", f"/projects/{p}/uploads/{upload_id}/ingest",
            json={"dataset": {"name": "x", "unit": "m3/s"}})  # fmt: skip
    check("…or load one", r.status_code == 403, str(r.status_code))


def cleanup(p):
    print("\n🧹 Cleanup")
    for name in ("usgs_nwm", "obs", "nwm", "big", "gauges", "gauges_albers", "basins",
                 "gauge_points"):  # fmt: skip
        api("user", "DELETE", f"/projects/{p}/datasets/{name}")
    left = api("user", "GET", f"/projects/{p}/datasets").json()
    check("its datasets are deleted", left == [], [d["name"] for d in left])
    r = api("user", "DELETE", f"/projects/{p}")
    check("the owner deletes the project (and its staged files)",
          r.status_code == 204, f"{r.status_code} {r.text[:120]}")


def main():
    print("🧪 Phase 3 gate: user data upload and registration")
    wait_for_control_plane()
    project = f"gate_u_{uuid.uuid4().hex[:8]}"
    r = api("user", "POST", "/projects", json={"name": project})
    if r.status_code != 201 or r.json().get("status") != "ready":
        print(f"  ❌ couldn't create a project: {r.status_code} {r.text[:200]}")
        return 1
    try:
        layers_gate(project)
        timeseries_gate(project)
        crosswalk_gate(project)
        cancel_gate(project)
        access_gate(project)
    finally:
        cleanup(project)
    passed = sum(results)
    print(f"\n{passed}/{len(results)} checks passed")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
