"""Layer and crosswalk datasets (layers-and-crosswalks.md): registration, writes
with geometry checks and key notes, reads as GeoJSON / GeoParquet, references."""

import io
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import shapely

from src.tables import ReadResult
from tests.test_datasets import ALICE, ROOT
from tests.test_datasets import tables as tables  # noqa: F401  (fixture)

DATASETS = "/v1/projects/flood/datasets"
PARQUET = "application/vnd.apache.parquet"
ARROW = "application/vnd.apache.arrow.stream"


def create(client, body, project="flood", headers=ALICE):
    return client.post(f"/v1/projects/{project}/datasets", json=body, headers=headers)


def layer(client, name="gauges", **fields):
    resp = create(client, {"type": "layer", "name": name, **fields})
    assert resp.status_code == 201, resp.text
    return resp.json()


def timeseries(client, name, **fields):
    body = {
        "type": "timeseries",
        "name": name,
        "role": "observed",
        "variable": "streamflow",
        "statistic": "inst",
        **fields,
    }
    resp = create(client, body)
    assert resp.status_code == 201, resp.text
    return resp.json()


def write(client, name, rows, **params):
    return client.post(
        f"{DATASETS}/{name}/data", json=rows, headers=ALICE, params=params
    )


def write_arrow(client, name, table):
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return client.post(
        f"{DATASETS}/{name}/data",
        content=sink.getvalue().to_pybytes(),
        headers={**ALICE, "Content-Type": ARROW},
    )


def last_write(tables, name):  # noqa: F811
    return tables.written[("flood", name)][-1]


# --- layers -------------------------------------------------------------------


def test_register_a_layer(client, tables):  # noqa: F811
    body = layer(client, attributes=[{"name": "state", "type": "string"}],
                 source="USGS")  # fmt: skip
    assert body["kind"] == "layer" and body["layer"] == {"source": "USGS"}
    assert [c["name"] for c in body["columns"]] == [
        "location_key", "name", "geometry",
        "bbox_xmin", "bbox_ymin", "bbox_xmax", "bbox_ymax", "state",
    ]  # fmt: skip
    assert body["key_columns"] == ["location_key"]
    key, layout = tables.layouts["gauges"]
    assert layout.sort == ("location_key",) and layout.partition is None
    assert layout.properties["teehr.type"] == "layer"
    assert layout.properties["teehr.crs"] == "EPSG:4326"


def test_layer_attributes_cant_reuse_layer_columns(client, tables):  # noqa: F811
    resp = create(
        client,
        {"type": "layer", "name": "g", "attributes": [{"name": "geometry",
                                                       "type": "string"}]},
    )  # fmt: skip
    assert resp.status_code == 422


def test_layer_writes_take_geojson_or_wkt_and_compute_bounds(client, tables):  # noqa: F811
    layer(client, attributes=[{"name": "drainage_km2", "type": "float64"}])
    rows = [
        {"location_key": "01010000", "name": "St. John",
         "geometry": {"type": "Point", "coordinates": [-69.7, 46.7]},
         "drainage_km2": 1.5},
        {"location_key": "basin-1",
         "geometry": "POLYGON ((-70 45, -69 45, -69 46, -70 46, -70 45))"},
    ]  # fmt: skip
    resp = write(client, "gauges", rows)
    assert resp.status_code == 201, resp.text
    data = last_write(tables, "gauges")
    assert data["location_key"].to_pylist() == ["01010000", "basin-1"]
    assert data["drainage_km2"].to_pylist() == [1.5, None]
    point = shapely.from_wkb(data["geometry"][0].as_py())
    assert (point.x, point.y) == (-69.7, 46.7)
    assert data["bbox_xmin"].to_pylist() == [-69.7, -70.0]
    assert data["bbox_ymax"].to_pylist() == [46.7, 46.0]
    mode, _, _ = tables.writes[-1]
    assert mode == "upsert"


def test_layer_writes_take_wkb_in_arrow(client, tables):  # noqa: F811
    layer(client)
    table = pa.table(
        {
            "location_key": ["a"],
            "geometry": pa.array([shapely.Point(10, 20).wkb], pa.binary()),
        }
    )
    assert write_arrow(client, "gauges", table).status_code == 201
    assert last_write(tables, "gauges")["bbox_xmax"].to_pylist() == [10.0]


def test_every_geometry_problem_is_reported(client, tables):  # noqa: F811
    layer(client)
    rows = [
        {"location_key": "ok", "geometry": "POINT (1 1)"},
        {"location_key": "bad", "geometry": "POINT (oops)"},
        {"location_key": "empty", "geometry": "POINT EMPTY"},
        {"location_key": "bowtie",
         "geometry": "POLYGON ((0 0, 1 1, 1 0, 0 1, 0 0))"},
        {"location_key": "utm", "geometry": "POINT (500000 4649776)"},
        {"location_key": "none", "geometry": None},
    ]  # fmt: skip
    resp = write(client, "gauges", rows)
    assert resp.status_code == 422
    detail = resp.json()["detail"]
    for key, why in (
        ("bad", "isn't valid"),
        ("empty", "empty geometry"),
        ("bowtie", "invalid geometry"),
        ("utm", "EPSG:4326"),
        ("none", "no geometry"),
    ):
        assert why in detail and key in detail
    assert ("flood", "gauges") not in getattr(tables, "written", {})


def test_repeated_location_keys_are_refused(client, tables):  # noqa: F811
    layer(client)
    row = {"location_key": "a", "geometry": "POINT (1 1)"}
    resp = write(client, "gauges", [row, row])
    assert resp.status_code == 422 and "more than once" in resp.text


@pytest.fixture
def stored(client, tables):  # noqa: F811
    """A layer with two locations; reads return what was written last."""
    layer(client)
    write(
        client,
        "gauges",
        [
            {"location_key": "a", "name": "A", "geometry": "POINT (1 2)"},
            {"location_key": "b", "geometry": "LINESTRING (0 0, 3 4)"},
        ],
    )

    def read(token, ns, name, *, columns, row_filter, snapshot_id, limit):
        tables.read_filter = row_filter
        data = last_write(tables, name)
        if columns:
            data = data.select(list(columns))
        return ReadResult(data.slice(0, limit), 7)

    tables.read = read
    return tables


def test_json_reads_give_geojson_geometry(client, stored):
    rows = client.get(f"{DATASETS}/gauges/data", headers=ALICE).json()["rows"]
    assert rows[0]["geometry"] == {"type": "Point", "coordinates": [1.0, 2.0]}
    assert rows[1]["geometry"]["type"] == "LineString"
    assert rows[0]["bbox_xmax"] == 1.0


def test_geojson_reads_give_a_feature_collection(client, stored):
    resp = client.get(
        f"{DATASETS}/gauges/data",
        headers={**ALICE, "Accept": "application/geo+json"},
        params={"columns": "location_key,name"},
    )
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("application/geo+json")
    body = resp.json()
    assert body["type"] == "FeatureCollection"
    first = body["features"][0]
    assert first["id"] == "a" and first["properties"] == {
        "location_key": "a",
        "name": "A",
    }
    assert first["geometry"]["type"] == "Point"


def test_parquet_reads_carry_geoparquet_metadata(client, stored):
    resp = client.get(f"{DATASETS}/gauges/data", headers={**ALICE, "Accept": PARQUET})
    table = pq.read_table(io.BytesIO(resp.content))
    geo = json.loads(table.schema.metadata[b"geo"])
    assert geo["primary_column"] == "geometry"
    assert geo["columns"]["geometry"]["encoding"] == "WKB"


def test_bbox_filters_on_the_bounds(client, stored):
    resp = client.get(
        f"{DATASETS}/gauges/data", headers=ALICE, params={"bbox": "0.5,1,2,3"}
    )
    assert resp.status_code == 200
    text = str(stored.read_filter)
    for column in ("bbox_xmin", "bbox_xmax", "bbox_ymin", "bbox_ymax"):
        assert column in text
    bad = client.get(f"{DATASETS}/gauges/data", headers=ALICE, params={"bbox": "1,2"})
    assert bad.status_code == 422


def test_bbox_and_geojson_are_for_layers_only(client, tables):  # noqa: F811
    timeseries(client, "obs")
    resp = client.get(f"{DATASETS}/obs/data", headers=ALICE, params={"bbox": "0,0,1,1"})
    assert resp.status_code == 422


# --- crosswalks ---------------------------------------------------------------


def test_register_a_crosswalk(client, tables):  # noqa: F811
    timeseries(client, "usgs_obs")
    timeseries(client, "nwm_sr", role="forecast")
    timeseries(client, "nwm_mr", role="forecast", series_table="nwm_sr_series")
    resp = create(
        client,
        {"type": "crosswalk", "name": "usgs_nwm", "left": "usgs_obs",
         "right": "nwm_mr", "extra_columns": [{"name": "weight",
                                                "type": "float64"}]},
    )  # fmt: skip
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["crosswalk"] == {
        "left": "usgs_obs",
        "right": "nwm_mr",
        "left_series_table": "usgs_obs_series",
        "right_series_table": "nwm_sr_series",  # the family's shared one
        "source": None,
    }
    assert body["key_columns"] == ["left_key", "right_key"]
    assert [c["name"] for c in body["columns"]] == ["left_key", "right_key", "weight"]


def test_crosswalk_sides_must_be_timeseries(client, tables):  # noqa: F811
    layer(client)
    timeseries(client, "usgs_obs")
    for left, right in (("nope", "usgs_obs"), ("usgs_obs", "gauges")):
        resp = create(client, {"type": "crosswalk", "name": "x", "left": left,
                               "right": right})  # fmt: skip
        assert resp.status_code == 422, (left, right)


def test_crosswalk_keys_not_in_their_series_table_are_noted(client, tables):  # noqa: F811
    timeseries(client, "usgs_obs")
    timeseries(client, "nwm", role="forecast")
    create(client, {"type": "crosswalk", "name": "xw", "left": "usgs_obs",
                    "right": "nwm"})  # fmt: skip
    write(client, "usgs_obs", [{"series_key": "usgs-01", "value": 1.0,
                                "value_time": "2026-01-01T00:00:00Z"}])  # fmt: skip
    resp = write(
        client,
        "xw",
        [
            {"left_key": "usgs-01", "right_key": "nwm-1"},
            {"left_key": "usgs-99", "right_key": "nwm-2"},
        ],
    )
    assert resp.status_code == 201, resp.text
    notes = resp.json()["notes"]
    assert notes == [
        "1 of 2 left_key values aren't in usgs_obs_series: usgs-99",
        "2 of 2 right_key values aren't in nwm_series: nwm-1, nwm-2",
    ]
    history = client.get(f"{DATASETS}/xw/writes", headers=ALICE).json()
    assert history[0]["notes"] == notes


def test_repeated_crosswalk_pairs_are_refused(client, tables):  # noqa: F811
    timeseries(client, "a")
    timeseries(client, "b")
    create(client, {"type": "crosswalk", "name": "xw", "left": "a", "right": "b"})
    row = {"left_key": "1", "right_key": "2"}
    assert write(client, "xw", [row, row]).status_code == 422


# --- series locations ---------------------------------------------------------


def test_series_location_keys_not_in_the_layer_are_noted(client, tables):  # noqa: F811
    layer(client)
    write(client, "gauges", [{"location_key": "01", "geometry": "POINT (1 1)"}])
    timeseries(client, "obs", location_layer="gauges")
    resp = write(
        client,
        "obs_series",
        [{"series_key": "s1", "location_key": "01"},
         {"series_key": "s2", "location_key": "02"}],
    )  # fmt: skip
    assert resp.status_code == 201, resp.text
    # The rows didn't name a layer: the timeseries' default is used.
    assert resp.json()["notes"] == ["1 of 2 location keys aren't in gauges: 02"]


def test_series_rows_must_name_a_layer(client, tables):  # noqa: F811
    timeseries(client, "obs")
    resp = write(
        client,
        "obs_series",
        [{"series_key": "s1", "location_key": "01", "location_layer": "nope"}],
    )
    assert resp.status_code == 422 and "'nope' is not a layer" in resp.text


# --- references ---------------------------------------------------------------


def test_deleting_what_others_refer_to_is_refused(client, tables):  # noqa: F811
    layer(client)
    timeseries(client, "obs", location_layer="gauges")
    timeseries(client, "sim", role="simulated")
    create(client, {"type": "crosswalk", "name": "xw", "left": "obs", "right": "sim"})
    resp = client.delete(f"{DATASETS}/gauges", headers=ALICE)
    assert resp.status_code == 409 and "flood.obs" in resp.text
    resp = client.delete(f"{DATASETS}/sim", headers=ALICE)
    assert resp.status_code == 409 and "flood.xw" in resp.text
    assert client.delete(f"{DATASETS}/xw", headers=ALICE).status_code == 204
    assert client.delete(f"{DATASETS}/sim", headers=ALICE).status_code == 204


@pytest.fixture
def public(client, tables, db_url):  # noqa: F811
    from sqlalchemy.orm import Session

    from src.db import make_engine
    from src.provisioning import ensure_system_projects

    with Session(make_engine(db_url)) as session:
        ensure_system_projects(session)
    client.post("/v1/projects/public/provision", headers=ROOT)
    resp = create(client, {"type": "layer", "name": "usgs_gages"}, "public", ROOT)
    assert resp.status_code == 201, resp.text
    return tables


def test_public_layers_are_referred_to_as_public_name(client, public):
    body = timeseries(client, "obs", location_layer="public.usgs_gages")
    assert body["timeseries"]["location_layer"] == "public.usgs_gages"
    # A bare name means this project's own, which doesn't have one.
    resp = create(client, {"type": "timeseries", "name": "obs2", "role": "observed",
                           "variable": "streamflow", "statistic": "inst",
                           "location_layer": "usgs_gages"})  # fmt: skip
    assert resp.status_code == 422
    # Public's layer can't be deleted while a project uses it.
    resp = client.delete("/v1/projects/public/datasets/usgs_gages", headers=ROOT)
    assert resp.status_code == 409 and "flood.obs" in resp.text


def test_other_projects_cant_be_referred_to_yet(client, tables):  # noqa: F811
    resp = create(client, {"type": "crosswalk", "name": "xw",
                           "left": "other.obs", "right": "obs"})  # fmt: skip
    assert resp.status_code == 422


def test_arrow_reads_mark_geometry_as_geoarrow(client, stored):
    resp = client.get(f"{DATASETS}/gauges/data", headers={**ALICE, "Accept": ARROW})
    table = pa.ipc.open_stream(resp.content).read_all()
    meta = table.schema.field("geometry").metadata
    assert meta[b"ARROW:extension:name"] == b"geoarrow.wkb"
    # ...and a GeoArrow-marked column is accepted on write.
    assert write_arrow(client, "gauges", table.select(["location_key", "geometry"])
                       ).status_code == 201  # fmt: skip
