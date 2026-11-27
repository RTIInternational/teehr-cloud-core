"""Attribute columns added to an existing layer or series table: optional only,
the table's schema evolved as the caller and the record kept in step."""

import pyarrow as pa
import pytest

from src import tables as t
from src.tables import TableForbidden, TableMissing
from tests.test_datasets import ALICE, BOB
from tests.test_datasets import tables as tables  # noqa: F401  (fixture)
from tests.test_layers import layer, timeseries, write

DATASETS = "/v1/projects/flood/datasets"
SOURCE_ID = {"name": "source_id", "type": "string", "doc": "USGS time_series_id"}


def add(client, name, *columns, headers=ALICE):
    return client.post(
        f"{DATASETS}/{name}/columns", json={"columns": list(columns)}, headers=headers
    )


def names(body):
    return [c["name"] for c in body["columns"]]


def test_a_series_table_gains_a_column_that_writes_fill(client, tables):
    layer(client, "gauges")
    timeseries(client, "usgs_obs", location_layer="gauges")
    rows = [
        {"series_key": "usgs-01", "value_time": "2026-01-01T00:00:00Z", "value": 1.0}
    ]
    assert write(client, "usgs_obs", rows).status_code == 201

    resp = add(client, "usgs_obs_series", SOURCE_ID)
    assert resp.status_code == 200, resp.text
    assert names(resp.json())[-1] == "source_id"
    assert tables.tables[("flood", "usgs_obs_series")][-1]["name"] == "source_id"
    assert [(op, ns, n) for op, _, ns, n in tables.calls if op == "add_columns"] == [
        ("add_columns", "flood", "usgs_obs_series")
    ]
    assert (
        names(client.get(f"{DATASETS}/usgs_obs_series", headers=ALICE).json())[-1]
        == "source_id"
    )

    pin = [{"series_key": "usgs-01", "source_id": "f5dc2874"}]
    assert write(client, "usgs_obs_series", pin).status_code == 201
    row = tables.rows[("flood", "usgs_obs_series")]["usgs-01"]
    assert row["source_id"] == "f5dc2874" and row["location_layer"] == "gauges"


def test_a_layer_gains_attributes(client, tables):
    layer(client, "gauges")
    resp = add(client, "gauges", {"name": "huc2", "type": "string"},
               {"name": "drainage_area", "type": "float64"})  # fmt: skip
    assert resp.status_code == 200, resp.text
    assert names(resp.json())[-2:] == ["huc2", "drainage_area"]


@pytest.mark.parametrize(
    "columns, detail",
    [
        ([{"name": "source_id", "type": "string", "required": True}], "required"),
        ([SOURCE_ID, {**SOURCE_ID, "name": "SOURCE_ID"}], "duplicate"),
        ([], "at least 1"),
        ([{"name": "x", "type": "nope"}], "type"),
    ],
)
def test_invalid_columns(client, tables, columns, detail):
    timeseries(client, "usgs_obs")
    resp = add(client, "usgs_obs_series", *columns)
    assert resp.status_code == 422 and detail in resp.text.lower(), resp.text


def test_a_column_it_has_is_a_conflict(client, tables):
    timeseries(client, "usgs_obs")
    resp = add(client, "usgs_obs_series", {"name": "Location_Key", "type": "string"})
    assert resp.status_code == 409 and "Location_Key" in resp.json()["detail"]


def test_only_layers_and_series_tables(client, tables):
    timeseries(client, "usgs_obs")
    resp = add(client, "usgs_obs", SOURCE_ID)
    assert resp.status_code == 422 and "timeseries" in resp.json()["detail"]


def test_readers_cant_and_strangers_dont_see_it(client, tables):
    timeseries(client, "usgs_obs")
    assert add(client, "usgs_obs_series", SOURCE_ID, headers=BOB).status_code in (
        403,
        404,
    )
    assert add(client, "missing", SOURCE_ID).status_code == 404


@pytest.mark.parametrize(
    "error, code", [(TableForbidden("no"), 403), (TableMissing("gone"), 409)]
)
def test_catalog_refusals(client, tables, error, code):
    timeseries(client, "usgs_obs")
    tables.fail_with = error
    resp = add(client, "usgs_obs_series", SOURCE_ID)
    assert resp.status_code == code, resp.text
    tables.fail_with = None
    body = client.get(f"{DATASETS}/usgs_obs_series", headers=ALICE).json()
    assert "source_id" not in names(body)  # the record isn't changed


def test_the_iceberg_table_gains_the_column_and_keeps_its_rows(tmp_path):
    """Against a real (SQL) catalog: schema evolution, rows untouched, retry-safe."""
    real = t.PolarisTables.at({
        "type": "sql", "uri": f"sqlite:///{tmp_path}/c.db",
        "warehouse": f"file://{tmp_path}/w",
    })  # fmt: skip
    real._catalog("token").create_namespace("p")
    columns = [{"name": "series_key", "type": "string", "required": True},
               {"name": "location_key", "type": "string"}]  # fmt: skip
    real.create_table("token", "p", "s", columns, ["series_key"])
    table = real._catalog("token").load_table(("p", "s"))
    table.append(pa.table({"series_key": ["usgs-01"], "location_key": ["usgs-01"]},
                          schema=table.schema().as_arrow()))  # fmt: skip

    real.add_columns("token", "p", "s", [SOURCE_ID])
    real.add_columns("token", "p", "s", [SOURCE_ID])  # again: nothing to do
    table = real._catalog("token").load_table(("p", "s"))
    field = table.schema().find_field("source_id")
    assert not field.required and field.doc == "USGS time_series_id"
    assert table.scan().to_arrow().to_pylist() == [
        {"series_key": "usgs-01", "location_key": "usgs-01", "source_id": None}
    ]
