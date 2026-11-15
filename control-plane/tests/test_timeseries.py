"""Typed timeseries datasets (#182, warehouse data model v1): registration
builds the columns, key, layout and series table from the metadata."""

import dataclasses
import json

import pyarrow as pa
import pytest

from src.config import get_settings
from src.main import app
from src.tables import conform, duplicate_keys
from tests.test_datasets import ALICE, ROOT
from tests.test_datasets import tables as tables  # noqa: F401  (fixture)

DATASETS = "/v1/projects/flood/datasets"


def register(client, name="usgs_obs", headers=ALICE, **fields):
    body = {
        "type": "timeseries",
        "name": name,
        "role": "observed",
        "variable": "streamflow",
        "statistic": "inst",
        **fields,
    }
    return client.post(DATASETS, json=body, headers=headers)


def names(columns):
    return [c["name"] for c in columns]


def test_observed_instant_streamflow(client, tables):
    resp = register(client, source="USGS NWIS")
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["kind"] == "timeseries"
    assert body["timeseries"] == {
        "role": "observed",
        "variable": "streamflow",
        "unit": "m3/s",
        "statistic": "inst",
        "period": "instant",
        "optional_columns": [],
        "series_table": "usgs_obs_series",
        "location_layer": None,
        "time_partition": "month",
        "source": "USGS NWIS",
    }
    assert names(body["columns"]) == ["series_key", "value_time", "value"]
    assert body["key_columns"] == ["series_key", "value_time"]

    key, layout = tables.layouts["usgs_obs"]
    assert layout.partition == ("value_time", "month")
    assert layout.sort == ("series_key", "value_time")
    assert layout.properties["teehr.type"] == "timeseries"
    assert layout.properties["teehr.unit"] == "m3/s"

    # Its series table was created first and registered too.
    assert [c[3] for c in tables.calls] == ["usgs_obs_series", "usgs_obs"]
    series = client.get(f"{DATASETS}/usgs_obs_series", headers=ALICE).json()
    assert series["kind"] == "series" and series["key_columns"] == ["series_key"]
    assert names(series["columns"])[:5] == [
        "series_key",
        "location_key",
        "location_layer",
        "period",
        "resolution",
    ]


def test_ensemble_quantile_forecast(client, tables):
    resp = register(
        client,
        "nwm_medium_range",
        role="forecast",
        optional_columns=["member", "quantile", "quality"],
        extra_columns=[{"name": "lead_hours", "type": "int32"}],
        time_partition="year",
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert names(body["columns"]) == [
        "series_key",
        "reference_time",
        "value_time",
        "member",
        "quantile",
        "value",
        "quality",
        "lead_hours",
    ]
    assert body["key_columns"] == [
        "series_key",
        "reference_time",
        "value_time",
        "member",
        "quantile",
    ]
    # reference_time is on for every forecast, without asking.
    assert body["timeseries"]["optional_columns"][0] == "reference_time"
    quantile = next(c for c in body["columns"] if c["name"] == "quantile")
    assert quantile == quantile | {"type": "decimal(5,4)", "required": True}
    _, layout = tables.layouts["nwm_medium_range"]
    assert layout.partition == ("reference_time", "year")
    assert layout.sort == ("series_key", "reference_time", "value_time")


def test_absolute_period_adds_window_columns(client, tables):
    client.post(
        "/v1/domains/periods",
        json={"project": "flood", "name": "storm", "form": "absolute"},
        headers=ALICE,
    )
    resp = register(client, statistic="sum", period="storm", variable="precipitation")
    assert resp.status_code == 201, resp.text
    assert names(resp.json()["columns"])[-2:] == ["period_start", "period_end"]
    assert resp.json()["timeseries"]["unit"] == "mm"


def test_a_family_shares_one_series_table(client, tables):
    register(client, "short_range", role="forecast")
    resp = register(
        client, "medium_range", role="forecast", series_table="short_range_series"
    )
    assert resp.status_code == 201
    assert resp.json()["timeseries"]["series_table"] == "short_range_series"
    assert "medium_range_series" not in {c[3] for c in tables.calls}

    # The shared table stays until its last user goes.
    assert (
        client.delete(f"{DATASETS}/short_range_series", headers=ALICE).status_code
        == 409
    )
    assert client.delete(f"{DATASETS}/short_range", headers=ALICE).status_code == 204
    assert ("flood", "short_range_series") in tables.tables
    assert client.delete(f"{DATASETS}/medium_range", headers=ALICE).status_code == 204
    assert ("flood", "short_range_series") not in tables.tables
    listed = client.get(DATASETS, headers=ALICE).json()
    assert listed == []


def test_shared_series_table_must_be_one(client, tables):
    client.post(
        DATASETS,
        json={"name": "plain", "columns": [{"name": "x", "type": "int32"}]},
        headers=ALICE,
    )
    resp = register(client, series_table="plain")
    assert resp.status_code == 422
    resp = register(client, series_table="nope")
    assert resp.status_code == 422


@pytest.mark.parametrize(
    ("fields", "detail"),
    [
        ({"variable": "discharge"}, "unknown variable"),
        ({"unit": "[cft_i]/s"}, "stored in m3/s"),
        ({"statistic": "mean"}, "needs a period"),
        ({"statistic": "mean", "period": "instant"}, "instant period"),
        ({"period": "hour_ending"}, "instant period"),
        ({"period": "nope"}, "unknown period"),
    ],
)
def test_metadata_is_checked_against_the_domains(client, tables, fields, detail):
    resp = register(client, **fields)
    assert resp.status_code == 422
    assert detail in resp.json()["detail"]
    assert tables.calls == []  # nothing created


@pytest.mark.parametrize(
    "fields",
    [
        {"extra_columns": [{"name": "value", "type": "float64"}]},
        {"series_attributes": [{"name": "location_key", "type": "string"}]},
        {
            "series_table": "x_series",
            "series_attributes": [{"name": "basin", "type": "string"}],
        },
        {"optional_columns": ["member", "member"]},
        {"role": "predicted"},
        {"time_partition": "day"},
        {"columns": [{"name": "x", "type": "int32"}]},
    ],
)
def test_invalid_registrations(client, tables, fields):
    assert register(client, **fields).status_code == 422


def test_name_conflicts(client, tables):
    register(client)
    assert register(client).status_code == 409
    # The default series table's name is taken by another dataset.
    client.post(
        DATASETS,
        json={"name": "other_series", "columns": [{"name": "x", "type": "int32"}]},
        headers=ALICE,
    )
    assert register(client, "other").status_code == 409


def test_failed_timeseries_create_drops_its_new_series_table(client, tables):
    tables.tables[("flood", "usgs_obs")] = []  # an unregistered table in the way
    resp = register(client)
    assert resp.status_code == 409
    assert ("flood", "usgs_obs_series") not in tables.tables
    assert client.get(f"{DATASETS}/usgs_obs_series", headers=ALICE).status_code == 404


def test_members_register_typed_datasets_without_the_untyped_switch(client, tables):
    settings = app.dependency_overrides[get_settings]()
    strict = dataclasses.replace(settings, untyped_datasets_for_members=False)
    app.dependency_overrides[get_settings] = lambda: strict
    assert register(client).status_code == 201
    plain = {"name": "plain", "columns": [{"name": "x", "type": "int32"}]}
    assert client.post(DATASETS, json=plain, headers=ALICE).status_code == 403
    assert client.post(DATASETS, json=plain, headers=ROOT).status_code == 201


# --- writes -----------------------------------------------------------------


def write(client, rows, name="usgs_obs", **params):
    return client.post(
        f"{DATASETS}/{name}/data",
        params=params,
        content=json.dumps(rows).encode(),
        headers={**ALICE, "Content-Type": "application/json"},
    )


ROWS = [
    {"series_key": "usgs-01", "value_time": "2026-01-01T00:00:00Z", "value": 1.5},
    {"series_key": "usgs-01", "value_time": "2026-01-01T01:00:00Z", "value": 2.5},
]


def test_timeseries_writes_upsert_by_default(client, tables):
    register(client)
    assert write(client, ROWS).status_code == 201
    assert tables.writes[-1][0] == "upsert"
    assert write(client, ROWS, mode="overwrite").status_code == 201
    resp = write(client, ROWS, mode="append")
    assert resp.status_code == 422 and "upsert or overwrite" in resp.json()["detail"]


def test_duplicate_keys_in_a_batch_are_refused(client, tables):
    register(client)
    resp = write(client, [*ROWS, {**ROWS[0], "value": 9.9}])
    assert resp.status_code == 422
    assert "1 key values" in resp.json()["detail"]


def test_upsert_needs_key_columns(client, tables):
    client.post(
        DATASETS,
        json={"name": "plain", "columns": [{"name": "x", "type": "int32"}]},
        headers=ALICE,
    )
    resp = write(client, [{"x": 1}], name="plain", mode="upsert")
    assert resp.status_code == 422
    assert write(client, [{"x": 1}], name="plain").status_code == 201  # append


def test_quantiles_keep_four_decimal_places(client, tables):
    register(client, "q", role="forecast", optional_columns=["quantile"])
    row = {
        "series_key": "a",
        "reference_time": "2026-01-01T00:00:00Z",
        "value_time": "2026-01-01T06:00:00Z",
        "value": 1.0,
    }
    assert write(client, [row | {"quantile": 0.25}], name="q").status_code == 201
    resp = write(client, [row | {"quantile": 0.12345}], name="q")
    assert resp.status_code == 422


def test_conform_refuses_rounding_float_quantiles():
    columns = [{"name": "quantile", "type": "decimal(5,4)", "required": True}]
    ok = conform(pa.table({"quantile": [0.1, 0.25, 0.9999]}), columns)
    assert [str(v) for v in ok.column(0).to_pylist()] == ["0.1000", "0.2500", "0.9999"]
    with pytest.raises(ValueError, match="4 decimal places"):
        conform(pa.table({"quantile": [0.5, 0.12345]}), columns)


def test_duplicate_keys_counts_repeated_key_values():
    data = pa.table({"k": ["a", "a", "b", "c", "c", "c"], "v": [1, 2, 3, 4, 5, 6]})
    assert duplicate_keys(data, ["k"]) == 2
    assert duplicate_keys(data, ["k", "v"]) == 0


def test_json_rows_give_quantiles_as_numbers():
    from decimal import Decimal

    from src.formats import json_rows

    table = pa.table({"quantile": pa.array([Decimal("0.2500")], pa.decimal128(5, 4))})
    assert json.loads(json.dumps(json_rows(table))) == [{"quantile": 0.25}]
