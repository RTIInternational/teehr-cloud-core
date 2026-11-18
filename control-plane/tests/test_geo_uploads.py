"""Layer and crosswalk uploads (layers-and-crosswalks.md, build step 2): inspect
GeoJSON / zipped Shapefile / GeoPackage / GeoParquet / CSV, ingest checks, and the
load job end to end."""

import io
import os
import tempfile
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
import pytest
import shapely

from tests.conftest import auth
from tests.test_datasets import ALICE
from tests.test_datasets import tables as tables  # noqa: F401  (fixture)
from tests.test_ingest import jobs as jobs  # noqa: F401  (fixture)
from tests.test_ingest import run_load
from tests.test_uploads import UPLOADS, staged
from tests.test_uploads import staging as staging  # noqa: F401  (fixture)

DATASETS = "/v1/projects/flood/datasets"
GAUGES = pa.table(
    {
        "site_no": ["01010000", "01010500"],
        "station_nm": ["St. John R", "Allagash R"],
        "drain_km2": [1.5, 2.0],
        "geometry": pa.array(
            [shapely.Point(-69.7, 46.7).wkb, shapely.Point(-68.6, 47.1).wkb]
        ),
    }
)


def geo_bytes(fmt: str, table=GAUGES, crs="EPSG:4326", layers=("gauges",)) -> bytes:
    """A GeoJSON, zipped Shapefile or GeoPackage of ``table``."""
    d = tempfile.mkdtemp()
    if fmt == "geojson":
        path = os.path.join(d, "g.geojson")
        pyogrio.write_arrow(table, path, driver="GeoJSON", geometry_name="geometry",
                            geometry_type="Point", crs=crs)  # fmt: skip
    elif fmt == "geopackage":
        path = os.path.join(d, "g.gpkg")
        for layer in layers:
            pyogrio.write_arrow(table, path, layer=layer, geometry_name="geometry",
                                geometry_type="Point", crs=crs)  # fmt: skip
    else:
        pyogrio.write_arrow(table, os.path.join(d, "g.shp"), geometry_name="geometry",
                            geometry_type="Point", crs=crs)  # fmt: skip
        path = os.path.join(d, "g.zip")
        with zipfile.ZipFile(path, "w") as z:
            for ext in ("shp", "shx", "dbf", "prj", "cpg"):
                part = os.path.join(d, f"g.{ext}")
                if os.path.exists(part) and not (ext == "prj" and crs is None):
                    z.write(part, f"g.{ext}")
    with open(path, "rb") as f:
        return f.read()


def upload(client, staging, filename, data, kind="layer", **extra):  # noqa: F811
    resp = client.post(
        UPLOADS, json={"filename": filename, "kind": kind, **extra}, headers=ALICE
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    staged(staging, body, data)
    return body["id"]


def inspect(client, upload_id, **params):
    resp = client.post(f"{UPLOADS}/{upload_id}/inspect", headers=ALICE, params=params)
    assert resp.status_code == 200, resp.text
    return resp.json()["preview"]


# A token that carries alice's project role, as a real one issued after the
# project was created does.
ALICE_NOW = auth("alice", roles=("teehr-project-flood-admin",))


def ingest(client, upload_id, body):
    return client.post(f"{UPLOADS}/{upload_id}/ingest", json=body, headers=ALICE_NOW)


def problems_at(resp) -> list[str]:
    assert resp.status_code == 422, resp.text
    return [".".join(e["loc"][1:]) for e in resp.json()["errors"]]


# --- create ---------------------------------------------------------------------


def test_formats_follow_the_kind(client, staging):  # noqa: F811
    for filename, kind, ok in (
        ("g.geojson", "layer", True),
        ("g.zip", "layer", True),
        ("g.gpkg", "layer", True),
        ("g.csv", "layer", True),
        ("g.geojson", "timeseries", False),
        ("xw.gpkg", "crosswalk", False),
        ("xw.csv", "crosswalk", True),
    ):
        resp = client.post(
            UPLOADS, json={"filename": filename, "kind": kind}, headers=ALICE
        )
        assert (resp.status_code == 201) == ok, (filename, kind, resp.text)
    resp = client.post(UPLOADS, json={"filename": "g.zip", "kind": "layer"},
                       headers=ALICE)  # fmt: skip
    assert resp.json()["format"] == "shapefile"


# --- inspect --------------------------------------------------------------------


def test_inspect_geojson(client, staging):  # noqa: F811
    preview = inspect(client, upload(client, staging, "gauges.geojson",
                                     geo_bytes("geojson")))  # fmt: skip
    mapping = preview["mapping"]
    assert mapping["location_key"] == "site_no"
    assert mapping["name"] == "station_nm"
    assert mapping["geometry"] == "geometry" and mapping["crs"] == "EPSG:4326"
    assert mapping["attributes"] == [
        {"column": "drain_km2", "name": "drain_km2", "type": "float64"}
    ]
    assert preview["rows"] == 2 and preview["needed"] == []
    assert preview["sample"][0]["geometry"] == "POINT (-69.7 46.7)"
    assert preview["ingest"] == {"dataset": {"name": "gauges"}}


def test_inspect_a_utm_shapefile(client, staging):  # noqa: F811
    utm = GAUGES.set_column(
        3, "geometry", pa.array([shapely.Point(500000, 5170000).wkb] * 2)
    )
    preview = inspect(client, upload(client, staging, "g.zip",
                                     geo_bytes("shapefile", utm, "EPSG:26919")))  # fmt: skip
    assert preview["mapping"]["crs"] == "EPSG:26919"
    assert any("reprojected to EPSG:4326" in n for n in preview["notes"])


def test_a_shapefile_without_prj_needs_a_crs(client, staging):  # noqa: F811
    preview = inspect(client, upload(client, staging, "g.zip",
                                     geo_bytes("shapefile", crs=None)))  # fmt: skip
    assert "mapping.crs" in preview["needed"]
    assert preview["ingest"]["mapping"]["crs"].startswith("<")


def test_inspect_a_geopackage_layer(client, staging):  # noqa: F811
    data = geo_bytes("geopackage", layers=("gauges", "basins"))
    upload_id = upload(client, staging, "g.gpkg", data)
    preview = inspect(client, upload_id)
    assert preview["layers"] == ["gauges", "basins"]
    assert preview["mapping"]["source_layer"] == "gauges"
    preview = inspect(client, upload_id, source_layer="basins")
    assert preview["mapping"]["source_layer"] == "basins"
    resp = client.post(f"{UPLOADS}/{upload_id}/inspect", headers=ALICE,
                       params={"source_layer": "nope"})  # fmt: skip
    assert resp.status_code == 422 and "no layer 'nope'" in resp.text


def test_inspect_a_csv_of_points(client, staging):  # noqa: F811
    csv = b"site_no,dec_lat_va,dec_long_va,huc_cd,Drainage Area\n" \
          b"01010000,46.7,-69.7,01010001,1.5\n"  # fmt: skip
    preview = inspect(client, upload(client, staging, "sites.csv", csv))
    mapping = preview["mapping"]
    assert (mapping["longitude"], mapping["latitude"]) == ("dec_long_va", "dec_lat_va")
    assert mapping["geometry"] is None and mapping["crs"] == "EPSG:4326"
    assert mapping["attributes"] == [
        {"column": "huc_cd", "name": "huc_cd", "type": "string"},  # leading zero
        {"column": "Drainage Area", "name": "Drainage_Area", "type": "float64"},
    ]


def test_inspect_geoparquet(client, staging):  # noqa: F811
    from src.geometry import with_geoparquet

    sink = io.BytesIO()
    pq.write_table(with_geoparquet(GAUGES), sink)
    preview = inspect(client, upload(client, staging, "g.parquet", sink.getvalue()))
    assert preview["mapping"]["geometry"] == "geometry"
    assert preview["mapping"]["crs"] == "EPSG:4326"
    # Plain Parquet with a WKB column: the coordinate system isn't known.
    sink = io.BytesIO()
    pq.write_table(GAUGES, sink)
    preview = inspect(client, upload(client, staging, "p.parquet", sink.getvalue()))
    assert "mapping.crs" in preview["needed"]


def test_inspect_a_crosswalk(client, staging):  # noqa: F811
    csv = b"primary_location_id,secondary_location_id,weight\nusgs-01,nwm-1,1.0\n"
    preview = inspect(client, upload(client, staging, "xw.csv", csv, "crosswalk"))
    assert preview["mapping"]["left_key"] == "primary_location_id"
    assert preview["mapping"]["right_key"] == "secondary_location_id"
    assert preview["mapping"]["attributes"] == [
        {"column": "weight", "name": "weight", "type": "float64"}
    ]
    assert preview["needed"] == ["dataset.left", "dataset.right"]
    assert preview["ingest"]["dataset"]["left"].startswith("<")


# --- ingest checks --------------------------------------------------------------


def test_layer_ingest_problems(client, staging, jobs):  # noqa: F811
    upload_id = upload(client, staging, "g.zip", geo_bytes("shapefile", crs=None))
    inspect(client, upload_id)
    resp = ingest(client, upload_id, {
        "dataset": {"name": "gauges", "unit": "m3/s"},
        "mapping": {"timezone": "UTC", "attributes": [{"column": "nope"}]},
    })  # fmt: skip
    places = problems_at(resp)
    for where in ("dataset.unit", "mapping.timezone", "mapping.crs",
                  "mapping.attributes.0"):  # fmt: skip
        assert where in places, places


def test_geopackage_layer_must_be_the_inspected_one(client, staging, jobs):  # noqa: F811
    data = geo_bytes("geopackage", layers=("gauges", "basins"))
    upload_id = upload(client, staging, "g.gpkg", data)
    inspect(client, upload_id)
    resp = ingest(client, upload_id, {"dataset": {"name": "g"},
                                      "mapping": {"source_layer": "basins"}})  # fmt: skip
    assert "mapping.source_layer" in problems_at(resp)


def test_crosswalk_sides_must_be_timeseries(client, staging, jobs):  # noqa: F811
    upload_id = upload(client, staging, "xw.csv", b"left_key,right_key\na,b\n",
                       "crosswalk")  # fmt: skip
    inspect(client, upload_id)
    resp = ingest(client, upload_id, {"dataset": {"name": "xw", "left": "nope"}})
    places = problems_at(resp)
    assert "dataset.left" in places and "dataset.right" in places


# --- loads ----------------------------------------------------------------------


def load(client, staging, jobs, upload_id, body):  # noqa: F811
    kube, _ = jobs
    resp = ingest(client, upload_id, body)
    assert resp.status_code == 202, resp.text
    result = run_load(client, staging, kube)
    status = client.get(f"{UPLOADS}/{upload_id}", headers=ALICE).json()
    return result, status


def test_load_a_utm_shapefile_reprojected(client, staging, jobs, tables):  # noqa: F811
    utm = GAUGES.set_column(
        3, "geometry", pa.array([shapely.Point(500000, 5170000).wkb] * 2)
    )
    upload_id = upload(client, staging, "g.zip", geo_bytes("shapefile", utm,
                                                           "EPSG:26919"))  # fmt: skip
    inspect(client, upload_id)
    result, status = load(
        client, staging, jobs, upload_id, {"dataset": {"name": "gauges"}}
    )
    assert status["status"] == "succeeded", status
    assert result["created"] is True and result["rows"] == 2
    dataset = client.get(f"{DATASETS}/gauges", headers=ALICE).json()
    assert [c["name"] for c in dataset["columns"]][-1] == "drain_km2"
    data = tables.written[("flood", "gauges")][-1]
    point = shapely.from_wkb(data["geometry"][0].as_py())
    # UTM 19N, 500000 E = the zone's central meridian, 69 W.
    assert point.x == pytest.approx(-69.0) and point.y == pytest.approx(
        46.684, abs=0.001
    )
    assert data["location_key"].to_pylist() == ["01010000", "01010500"]


def test_load_points_from_csv_into_an_existing_layer(client, staging, jobs, tables):  # noqa: F811
    client.post(DATASETS, headers=ALICE, json={
        "type": "layer", "name": "sites",
        "attributes": [{"name": "huc_cd", "type": "string"}],
    })  # fmt: skip
    csv = b"site_no,lat,lon,huc_cd\n01010000,46.7,-69.7,01010001\n"
    upload_id = upload(client, staging, "sites.csv", csv)
    inspect(client, upload_id)
    result, status = load(client, staging, jobs, upload_id, {
        "dataset": {"name": "sites"},
        "mapping": {"attributes": [{"column": "huc_cd", "type": "string"}]},
    })  # fmt: skip
    assert status["status"] == "succeeded", status
    data = tables.written[("flood", "sites")][-1]
    assert data["huc_cd"].to_pylist() == ["01010001"]  # leading zero kept
    assert data["bbox_xmin"].to_pylist() == [-69.7]


def test_existing_layer_must_have_the_attributes(client, staging, jobs, tables):  # noqa: F811
    client.post(DATASETS, headers=ALICE, json={"type": "layer", "name": "sites"})
    upload_id = upload(client, staging, "sites.csv",
                       b"site_no,lat,lon,huc_cd\n01,46.7,-69.7,x\n")  # fmt: skip
    inspect(client, upload_id)
    resp = ingest(client, upload_id, {"dataset": {"name": "sites"}})
    assert "mapping.attributes" in problems_at(resp)


def test_bad_geometry_fails_the_load_before_writing(client, staging, jobs, tables):  # noqa: F811
    csv = b"site_no,lat,lon\n01,46.7,-69.7\n02,4649776,500000\n"
    upload_id = upload(client, staging, "sites.csv", csv)
    inspect(client, upload_id)
    result, status = load(client, staging, jobs, upload_id, {"dataset": {"name": "s"}})
    assert status["status"] == "failed"
    assert "EPSG:4326" in status["problems"][0] and "02" in status["problems"][0]
    assert client.get(f"{DATASETS}/s", headers=ALICE).status_code == 404


def test_load_a_crosswalk_with_notes(client, staging, jobs, tables):  # noqa: F811
    for name in ("usgs_obs", "nwm"):
        client.post(DATASETS, headers=ALICE, json={
            "type": "timeseries", "name": name, "role": "observed",
            "variable": "streamflow", "statistic": "inst"})  # fmt: skip
    csv = b"primary_location_id,secondary_location_id,weight\n" \
          b"usgs-01,nwm-1,1.0\nusgs-02,nwm-2,0.5\n"  # fmt: skip
    upload_id = upload(client, staging, "xw.csv", csv, "crosswalk")
    inspect(client, upload_id)
    result, status = load(client, staging, jobs, upload_id, {
        "dataset": {"name": "usgs_nwm", "left": "usgs_obs", "right": "nwm"}})  # fmt: skip
    assert status["status"] == "succeeded", status
    assert result["rows"] == 2 and "series_added" not in result
    assert result["notes"][0].startswith("2 of 2 left_key values aren't in")
    dataset = client.get(f"{DATASETS}/usgs_nwm", headers=ALICE).json()
    assert dataset["crosswalk"]["left"] == "usgs_obs"
    assert [c["name"] for c in dataset["columns"]] == [
        "left_key",
        "right_key",
        "weight",
    ]
