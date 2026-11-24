"""Reads in another unit (teehr-shared-eval-spec ``data-api.md``, ``units.md``):
values converted, filters in that unit, the response's unit said."""

from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pytest
from pyiceberg.expressions import GreaterThan

from src.expressions import NotConvertible, in_units
from tests.test_datasets import ALICE
from tests.test_datasets import tables as tables  # noqa: F401  (fixture)
from tests.test_reads import Snapshots
from tests.test_timeseries import register

URL = "/v1/projects/flood/datasets/usgs_obs/data"
CFS = 0.028316846592  # m3/s per ft3/s


@pytest.fixture
def stored(client, tables):  # noqa: F811
    assert register(client).status_code == 201  # streamflow, stored in m3/s
    lake = Snapshots(tables)
    base = datetime(2026, 1, 1, tzinfo=UTC)
    lake.commit(pa.table({
        "series_key": ["usgs-01"] * 3,
        "value_time": pa.array([base + timedelta(hours=h) for h in range(3)],
                               pa.timestamp("us", tz="UTC")),
        "value": [CFS * 10, CFS * 20, None],
    }))  # fmt: skip
    return lake


def read(client, **params):
    return client.get(URL, params=params, headers=ALICE)


def test_values_come_in_the_unit_asked_for(client, stored):
    resp = read(client, unit="[cft_i]/s")
    assert resp.status_code == 200, resp.text
    assert resp.headers["X-Unit"] == "[cft_i]/s" and resp.json()["unit"] == "[cft_i]/s"
    values = [r["value"] for r in resp.json()["rows"]]
    assert values[:2] == pytest.approx([10.0, 20.0]) and values[2] is None


def test_without_a_unit_the_stored_unit_is_said(client, stored):
    resp = read(client)
    assert resp.headers["X-Unit"] == "m3/s" and resp.json()["unit"] == "m3/s"
    assert resp.json()["rows"][0]["value"] == pytest.approx(CFS * 10)


def test_a_filters_numbers_are_in_the_unit_and_still_pushed_down(client, stored):
    assert read(client, unit="[cft_i]/s", filter="value > 15").status_code == 200
    pushed = stored.reads[-1]["filter"]
    assert isinstance(pushed, GreaterThan) and pushed.term.name == "value"
    assert pushed.literal.value == pytest.approx(15 * CFS)


def test_arithmetic_in_a_filter_sees_converted_values(client, stored):
    resp = read(client, unit="[cft_i]/s", filter="value - 15 > 0")
    assert [r["value"] for r in resp.json()["rows"]] == pytest.approx([20.0])


@pytest.mark.parametrize("unit, says", [("[degF]", "isn't a unit of these values"),
                                        ("furlong/s", "unknown unit")])  # fmt: skip
def test_a_unit_that_doesnt_fit_is_refused(client, stored, unit, says):
    resp = read(client, unit=unit)
    assert resp.status_code == 422 and says in resp.json()["detail"]
    assert resp.json()["errors"][0]["loc"] == ["query", "unit"]


def test_unit_is_for_timeseries_and_results(client, tables):  # noqa: F811
    from tests.test_datasets import register as register_table
    from tests.test_reads import COLUMNS

    register_table(client, columns=COLUMNS)
    Snapshots(tables)
    resp = client.get("/v1/projects/flood/datasets/obs/data", params={"unit": "m3/s"},
                      headers=ALICE)  # fmt: skip
    assert resp.status_code == 422 and "timeseries" in resp.json()["detail"]


def test_in_units_rewrites_only_converted_columns():
    factors = {"value": (2.0, 1.0), "nse": (1.0, 0.0), "rb": None}
    assert in_units("value > 5", factors) == "value > 2.0"
    assert in_units("value BETWEEN 3 AND 7", factors) == "value BETWEEN 1.0 AND 3.0"
    assert in_units("value IN (1, 3)", factors) == "value IN (0.0, 1.0)"
    assert in_units("-1 < value", factors) == "-1.0 < value"
    assert in_units("value * 2 > 5", factors) == "(2.0 * value + (1.0)) * 2 > 5"
    assert in_units("nse > 0.5 AND series_key = 'a'", factors) == (
        "nse > 0.5 AND series_key = 'a'"
    )
    with pytest.raises(NotConvertible) as caught:
        in_units("rb > 1", factors)
    assert caught.value.columns == ["rb"]
