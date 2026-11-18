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


# --- units and series on write ------------------------------------------------


def written_values(tables):
    return tables.writes[-1][2].column("value").to_pylist()


def test_values_are_converted_to_the_dataset_unit(client, tables):
    register(client)  # streamflow, stored in m3/s
    resp = write(client, ROWS, unit="[cft_i]/s")
    assert resp.status_code == 201, resp.text
    assert resp.json()["unit"] == "[cft_i]/s"
    assert written_values(tables) == pytest.approx(
        [1.5 * 0.028316846592, 2.5 * 0.028316846592]
    )
    # No unit: already in the dataset's unit, recorded as such.
    resp = write(client, ROWS)
    assert resp.json()["unit"] == "m3/s" and written_values(tables) == [1.5, 2.5]
    history = client.get(f"{DATASETS}/usgs_obs/writes", headers=ALICE).json()
    assert [w["unit"] for w in history] == ["m3/s", "[cft_i]/s"]


def test_affine_conversion(client, tables):
    register(client, "temp", variable="air_temperature")  # stored in Cel
    rows = [{**ROWS[0], "value": 32.0}, {**ROWS[1], "value": 212.0}]
    assert write(client, rows, name="temp", unit="[degF]").status_code == 201
    assert written_values(tables) == pytest.approx([0.0, 100.0])
    assert write(client, rows[:1], name="temp", unit="K").status_code == 201
    assert written_values(tables) == pytest.approx([32.0 - 273.15])


def test_nulls_stay_null_through_conversion(client, tables):
    register(client)
    rows = [ROWS[0], {**ROWS[1], "value": None}]
    assert write(client, rows, unit="L/s").status_code == 201
    assert written_values(tables) == pytest.approx([0.0015, None])


@pytest.mark.parametrize(
    ("unit", "detail"),
    [("mm", "doesn't convert"), ("furlong", "unknown unit")],
)
def test_unconvertible_units_are_refused(client, tables, unit, detail):
    register(client)
    resp = write(client, ROWS, unit=unit)
    assert resp.status_code == 422 and detail in resp.json()["detail"]
    assert not getattr(tables, "writes", [])


def test_a_projects_own_unit_converts(client, tables):
    client.post(
        "/v1/domains/units",
        json={"project": "flood", "symbol": "[gal_us]/min", "dimension": "volume_flow",
              "to_si": 0.003785411784 / 60},
        headers=ALICE,
    )  # fmt: skip
    register(client)
    assert write(client, ROWS[:1], unit="[gal_us]/min").status_code == 201
    assert written_values(tables) == pytest.approx([1.5 * 0.003785411784 / 60])


def test_unit_is_for_timeseries_only(client, tables):
    client.post(
        DATASETS,
        json={"name": "plain", "columns": [{"name": "value", "type": "float64"}]},
        headers=ALICE,
    )
    resp = write(client, [{"value": 1.0}], name="plain", unit="m")
    assert resp.status_code == 422


def register_layer(client, name):
    resp = client.post(DATASETS, json={"type": "layer", "name": name}, headers=ALICE)
    assert resp.status_code == 201, resp.text


def test_new_series_keys_are_added_to_the_series_table(client, tables):
    register_layer(client, "usgs_gauges")
    register(client, location_layer="usgs_gauges")
    resp = write(client, ROWS)
    assert resp.json()["series_added"] == 1
    series = tables.rows[("flood", "usgs_obs_series")]
    assert series["usgs-01"]["location_layer"] == "usgs_gauges"
    assert series["usgs-01"]["location_key"] is None
    # Known keys aren't added (or touched) again; new ones are.
    more = [{**ROWS[0], "series_key": "usgs-02"}, ROWS[1]]
    assert write(client, more).json()["series_added"] == 1
    assert sorted(series) == ["usgs-01", "usgs-02"]
    # The series table is written before the data.
    ops = [c[0] for c in tables.calls]
    assert ops[-2:] == ["insert_missing", "write"]


def test_location_layer_must_be_a_layer(client, tables):
    resp = register(client, location_layer="nope")
    assert resp.status_code == 422 and "'nope' is not a layer" in resp.text


def test_family_writes_share_the_series_table(client, tables):
    register(client, "short_range", role="forecast")
    register(client, "medium_range", role="forecast", series_table="short_range_series")
    row = {**ROWS[0], "reference_time": "2026-01-01T00:00:00Z"}
    assert write(client, [row], name="short_range").json()["series_added"] == 1
    assert write(client, [row], name="medium_range").json()["series_added"] == 0


def test_series_table_writes_dont_add_series(client, tables):
    register(client)
    resp = write(
        client, [{"series_key": "a", "location_key": "01"}], name="usgs_obs_series"
    )
    assert resp.status_code == 201
    assert resp.json()["series_added"] is None and resp.json()["unit"] is None


# --- complete records; series attributes merge ------------------------------


def test_timeseries_writes_must_send_every_column(client, tables):
    register(client, optional_columns=["quality"])
    resp = write(client, ROWS)  # no quality column
    assert resp.status_code == 422
    assert "missing: quality" in resp.json()["detail"]
    rows = [{**ROWS[0], "quality": "P"}, {**ROWS[1], "quality": None}]
    assert write(client, rows).status_code == 201


def test_a_revised_quality_code_replaces_the_old_one(client, tables):
    register(client, optional_columns=["quality"])
    write(client, [{**ROWS[0], "quality": "P"}])
    assert write(client, [{**ROWS[0], "quality": "A"}]).status_code == 201
    mode, _, data = tables.writes[-1]
    assert mode == "upsert" and data.column("quality").to_pylist() == ["A"]


def test_series_writes_keep_columns_left_out(client, tables):
    register_layer(client, "gauges")
    register(client, location_layer="gauges",
             series_attributes=[{"name": "basin", "type": "string"}])  # fmt: skip
    write(client, ROWS)  # adds usgs-01 with the dataset's layer
    resp = write(
        client, [{"series_key": "usgs-01", "basin": "upper"}], name="usgs_obs_series"
    )
    assert resp.status_code == 201
    row = tables.rows[("flood", "usgs_obs_series")]["usgs-01"]
    assert row["location_layer"] == "gauges" and row["basin"] == "upper"
    key_columns, provided, _ = tables.merged
    assert key_columns == ["series_key"] and provided == ["series_key", "basin"]


def test_keep_unprovided_fills_from_stored_rows():
    from src.tables import keep_unprovided

    schema = pa.schema(
        [pa.field("series_key", pa.string(), nullable=False),
         pa.field("layer", pa.string()), pa.field("basin", pa.string())]
    )  # fmt: skip
    data = pa.table(
        {
            "series_key": ["b", "new", "a"],
            "layer": [None] * 3,
            "basin": ["B", "N", "A"],
        },
        schema=schema,
    )
    current = pa.table(
        {"series_key": ["a", "b"], "layer": ["la", "lb"], "basin": ["old", "old"]},
        schema=schema,
    )
    out = keep_unprovided(data, current, ["series_key"], ["series_key", "basin"])
    assert out.schema == schema
    assert out.to_pylist() == [
        {"series_key": "b", "layer": "lb", "basin": "B"},
        {"series_key": "new", "layer": None, "basin": "N"},
        {"series_key": "a", "layer": "la", "basin": "A"},
    ]
    # Everything provided: unchanged.
    assert keep_unprovided(data, current, ["series_key"], schema.names) is data
