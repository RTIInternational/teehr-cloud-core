"""Domains (#182): units, periods and variables - public rows plus each
project's own, one definition per name."""

import pytest

from tests.conftest import auth
from tests.test_datasets import ALICE, BOB, ROOT, add_member
from tests.test_datasets import tables as tables  # noqa: F401  (fixture)

CAROL = auth("carol")


def test_public_rows_are_seeded(client):
    units = {
        u["symbol"]: u for u in client.get("/v1/domains/units", headers=ALICE).json()
    }
    assert units["[cft_i]/s"]["dimension"] == "volume_flow"
    assert units["[cft_i]/s"]["to_si"] == pytest.approx(0.028316846592)
    assert units["Cel"]["offset"] == 273.15
    # 32 degF is 0 Cel is 273.15 K
    f = units["[degF]"]
    assert 32 * f["to_si"] + f["offset"] == pytest.approx(273.15)
    assert all(u["project"] is None for u in units.values())

    periods = {
        p["name"]: p for p in client.get("/v1/domains/periods", headers=ALICE).json()
    }
    assert set(periods) == {
        "instant",
        "hour_starting",
        "hour_ending",
        "day_starting",
        "day_ending",
    }
    assert periods["hour_ending"] | {"description": None} == {
        "name": "hour_ending",
        "form": "duration",
        "length": "PT1H",
        "value_time_at": "end",
        "start": None,
        "end": None,
        "description": None,
        "project": None,
    }

    variables = {
        v["name"]: v for v in client.get("/v1/domains/variables", headers=ALICE).json()
    }
    assert variables["streamflow"]["canonical_unit"] == "m3/s"


def test_public_rows_are_added_by_platform_admins_only(client):
    unit = {"symbol": "kW", "dimension": "power", "to_si": 1000}
    assert client.post("/v1/domains/units", json=unit, headers=ALICE).status_code == 403
    resp = client.post("/v1/domains/units", json=unit, headers=ROOT)
    assert resp.status_code == 201 and resp.json()["project"] is None
    assert client.post("/v1/domains/units", json=unit, headers=ROOT).status_code == 409


def test_project_rows_are_visible_in_that_project_only(client, tables):
    period = {
        "project": "flood",
        "name": "apr_jul_volume",
        "form": "calendar",
        "start": "--04-01",
        "end": "--07-31",
    }
    resp = client.post("/v1/domains/periods", json=period, headers=ALICE)
    assert resp.status_code == 201 and resp.json()["project"] == "flood"

    mine = client.get("/v1/domains/periods?project=flood", headers=ALICE).json()
    assert "apr_jul_volume" in {p["name"] for p in mine}
    public = client.get("/v1/domains/periods", headers=ALICE).json()
    assert "apr_jul_volume" not in {p["name"] for p in public}
    # Not a member: the project doesn't exist for them.
    resp = client.get("/v1/domains/periods?project=flood", headers=CAROL)
    assert resp.status_code == 404


def test_readers_list_writers_add(client, tables):
    add_member(client, "bob", "read")
    var = {"project": "flood", "name": "inflow", "canonical_unit": "m3/s"}
    resp = client.get("/v1/domains/variables?project=flood", headers=BOB)
    assert resp.status_code == 200
    assert (
        client.post("/v1/domains/variables", json=var, headers=BOB).status_code == 403
    )
    add_member(client, "bob", "write")
    assert (
        client.post("/v1/domains/variables", json=var, headers=BOB).status_code == 201
    )


def test_a_name_is_defined_once(client, tables):
    # A project can't shadow a public name...
    resp = client.post(
        "/v1/domains/variables",
        json={"project": "flood", "name": "streamflow", "canonical_unit": "m"},
        headers=ALICE,
    )
    assert resp.status_code == 409
    # ...and a public row can't take a name a project already uses.
    client.post(
        "/v1/domains/units",
        json={"project": "flood", "symbol": "[acr_us]", "dimension": "area",
              "to_si": 4046.873},
        headers=ALICE,
    )  # fmt: skip
    resp = client.post(
        "/v1/domains/units",
        json={"symbol": "[acr_us]", "dimension": "area", "to_si": 4046.873},
        headers=ROOT,
    )
    assert resp.status_code == 409


def test_variable_needs_a_known_unit(client, tables):
    resp = client.post(
        "/v1/domains/variables",
        json={"project": "flood", "name": "load", "canonical_unit": "MW"},
        headers=ALICE,
    )
    assert resp.status_code == 422
    # A project's own unit works for its own variable.
    client.post(
        "/v1/domains/units",
        json={"project": "flood", "symbol": "MW", "dimension": "power",
              "to_si": 1e6},
        headers=ALICE,
    )  # fmt: skip
    resp = client.post(
        "/v1/domains/variables",
        json={"project": "flood", "name": "load", "canonical_unit": "MW"},
        headers=ALICE,
    )
    assert resp.status_code == 201


@pytest.mark.parametrize(
    "period",
    [
        {"form": "duration", "length": "PT1H"},  # no value_time_at
        {"form": "duration", "length": "P", "value_time_at": "end"},
        {"form": "duration", "length": "PT", "value_time_at": "end"},
        {"form": "duration", "length": "1 hour", "value_time_at": "end"},
        {"form": "calendar", "start": "--04-01"},  # no end
        {"form": "calendar", "start": "--02-30", "end": "--03-01"},
        {"form": "calendar", "start": "--13-01", "end": "--03-01"},
        {"form": "instant", "length": "PT1H"},  # instant takes nothing
        {"form": "absolute", "start": "--01-01", "end": "--12-31"},
    ],
)
def test_period_rows_fill_exactly_their_forms_fields(client, tables, period):
    body = {"project": "flood", "name": "p", **period}
    assert (
        client.post("/v1/domains/periods", json=body, headers=ALICE).status_code == 422
    )


def test_leap_day_and_year_spanning_calendar_periods(client, tables):
    for name, start, end in (
        ("feb", "--02-01", "--02-29"),
        ("wy", "--10-01", "--09-30"),
    ):
        body = {"project": "flood", "name": name, "form": "calendar",
                "start": start, "end": end}  # fmt: skip
        assert (
            client.post("/v1/domains/periods", json=body, headers=ALICE).status_code
            == 201
        )
