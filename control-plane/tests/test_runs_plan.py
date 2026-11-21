"""Planning an evaluation spec against a project's datasets (POST .../runs/plan)."""

import pytest

from tests.test_datasets import ALICE, BOB, ROOT
from tests.test_datasets import tables as tables  # noqa: F401  (fixture)
from tests.test_layers import create, timeseries
from tests.test_layers import public as public  # noqa: F401  (fixture)

URL = "/v1/projects/flood/runs/plan"
CFS = "[cft_i]/s"


def crosswalk(client, name, left, right, project="flood", headers=ALICE):
    resp = create(client, {"type": "crosswalk", "name": name, "left": left,
                           "right": right}, project, headers)  # fmt: skip
    assert resp.status_code == 201, resp.text


def spec(primary="usgs_obs", secondary=("nwm_sr",), relation="usgs_nwm", **extra):
    return {
        "version": 1,
        "name": "t",
        "inputs": {
            "primary": {"dataset": primary},
            "secondary": [{"dataset": s} for s in secondary],
        },
        "pair": {"relation": relation},
        "aggregate": [
            {
                "group_by": ["primary_series_key", "secondary_dataset"],
                "metrics": [
                    "count",
                    "rmse",
                    "nse",
                    {"name": "hits", "threshold": {"value": 1000, "unit": CFS}},
                ],
            }
        ],
        **extra,
    }


@pytest.fixture
def datasets(client, tables):  # noqa: F811
    timeseries(client, "usgs_obs")
    timeseries(client, "nwm_sr", role="forecast")
    timeseries(client, "nwm_mr", role="forecast", series_table="nwm_sr_series")
    crosswalk(client, "usgs_nwm", "usgs_obs", "nwm_sr")
    return tables


def test_a_spec_plans_against_the_datasets(client, datasets):
    resp = client.post(URL, json=spec(secondary=("nwm_sr", "nwm_mr"),
                                      output={"unit": CFS}), headers=ALICE)  # fmt: skip
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["unit"] == CFS
    assert [(i["dataset"], i["side"], i["role"]) for i in body["inputs"]] == [
        ("usgs_obs", "primary", "observed"),
        ("nwm_sr", "secondary", "forecast"),
        ("nwm_mr", "secondary", "forecast"),  # shares nwm_sr's series: same crosswalk
    ]
    # Stored in m3/s, evaluated in cfs.
    assert body["inputs"][0]["unit"] == "m3/s"
    assert body["inputs"][0]["conversion"]["scale"] == pytest.approx(1 / 0.028316846592)
    assert body["relation"] == {"dataset": "usgs_nwm", "reversed": False}
    assert body["stages"] == [
        {
            "group_by": ["primary_series_key", "secondary_dataset"],
            "columns": ["count", "root_mean_square_error",
                        "nash_sutcliffe_efficiency", "hits"],
        }
    ]  # fmt: skip


def test_a_crosswalk_may_run_either_way(client, datasets):
    crosswalk(client, "nwm_usgs", "nwm_sr", "usgs_obs")
    resp = client.post(URL, json=spec(relation="nwm_usgs"), headers=ALICE)
    assert resp.status_code == 200, resp.text
    assert resp.json()["relation"] == {"dataset": "nwm_usgs", "reversed": True}


def problems(resp):
    assert resp.status_code == 422, resp.text
    return resp.json()["errors"]


def test_the_relation_must_link_the_inputs(client, datasets):
    timeseries(client, "other_obs")
    crosswalk(client, "other_nwm", "other_obs", "nwm_sr")
    [error] = problems(client.post(URL, json=spec(relation="other_nwm"), headers=ALICE))
    assert error["loc"] == ["body", "pair", "relation"]
    assert "not the primary input's series" in error["msg"]
    [error] = problems(client.post(URL, json=spec(relation="nwm_mr"), headers=ALICE))
    assert "is a timeseries, not a crosswalk" in error["msg"]


def test_every_unknown_dataset_is_reported(client, datasets):
    errors = problems(
        client.post(
            URL, json=spec(primary="nope", relation="public.gone"), headers=ALICE
        )
    )
    assert [e["msg"] for e in errors] == [
        "no dataset 'nope' in flood",
        "no dataset 'public.gone' in the public project",
    ]


def test_spec_shape_problems_point_at_the_field(client, datasets):
    bad = spec()
    bad["aggregate"][0]["metrics"] = [{"name": "hits", "threshold": {"value": 1000}}]
    errors = problems(client.post(URL, json=bad, headers=ALICE))
    assert errors[0]["loc"][:3] == ["body", "aggregate", 0]
    errors = problems(client.post(URL, json={**spec(), "version": 2}, headers=ALICE))
    assert errors[0]["loc"] == ["body", "version"]


def test_what_the_engine_refuses_is_reported(client, datasets):
    # A forecast can't be the primary (observed) side.
    [error] = problems(client.post(URL, json=spec(primary="nwm_sr", secondary=("usgs_obs",)),
                                   headers=ALICE))  # fmt: skip
    assert error["type"] == "plan" and "role 'forecast'" in error["msg"]
    # Units must convert to the evaluation unit.
    [error] = problems(
        client.post(URL, json=spec(output={"unit": "Cel"}), headers=ALICE)
    )
    assert error["type"] == "plan" and "Cel" in error["msg"]


def test_public_datasets_are_public_name(client, public):  # noqa: F811
    resp = create(client, {"type": "timeseries", "name": "usgs", "role": "observed",
                           "variable": "streamflow", "statistic": "inst"},
                  "public", ROOT)  # fmt: skip
    assert resp.status_code == 201, resp.text
    timeseries(client, "nwm_sr", role="forecast")
    crosswalk(client, "usgs_nwm", "public.usgs", "nwm_sr")
    resp = client.post(URL, json=spec(primary="public.usgs"), headers=ALICE)
    assert resp.status_code == 200, resp.text
    assert resp.json()["inputs"][0]["dataset"] == "public.usgs"


def test_readers_can_plan_and_strangers_cant_see_the_project(client, datasets):
    from tests.test_datasets import add_member

    assert client.post(URL, json=spec(), headers=BOB).status_code == 404
    add_member(client, "bob", "read")
    assert client.post(URL, json=spec(), headers=BOB).status_code == 200


def test_location_attributes_need_the_layer_to_have_them(client, datasets):
    from tests.test_layers import layer

    field = {"name": "state", "fn": "attribute", "attribute": "state", "of": "location"}
    body = spec(fields=[field])
    body["aggregate"][0]["group_by"].append("state")
    [error] = problems(client.post(URL, json=body, headers=ALICE))
    assert "names no location layer" in error["msg"]

    layer(client, "gauges", attributes=[{"name": "huc2", "type": "string"}])
    timeseries(client, "obs_located", location_layer="gauges")
    crosswalk(client, "located_nwm", "obs_located", "nwm_sr")
    located = spec(primary="obs_located", relation="located_nwm", fields=[field])
    located["aggregate"][0]["group_by"].append("state")
    [error] = problems(client.post(URL, json=located, headers=ALICE))
    assert "layer 'gauges' has no attribute 'state'" in error["msg"]
    field["attribute"] = "huc2"
    located["aggregate"][0]["group_by"][-1] = "state"
    resp = client.post(URL, json=located, headers=ALICE)
    assert resp.status_code == 200, resp.text
    assert resp.json()["stages"][0]["group_by"][-1] == "state"


def test_features_teehr_cant_run_yet_are_refused_with_why(client, datasets):
    body = spec(
        secondary=("nwm_sr",),
        fields=[
            {"name": "p90", "fn": "percentile", "from": "usgs_p", "percentile": 90},
            {"name": "scale", "fn": "naive_scale", "from": "usgs_ns", "lag": "P1D"},
        ],
    )
    body["inputs"]["secondary"].append({"generate": "climatology", "from": "usgs_clim"})
    resp = client.post(URL, json=body, headers=ALICE)
    assert resp.status_code == 422, resp.text
    errors = resp.json()["errors"]
    assert [(e["loc"][1:], e["type"]) for e in errors] == [
        (["inputs", "secondary", 1], "unsupported"),
        (["fields", 0], "unsupported"),
        (["fields", 1], "unsupported"),
    ]
    assert all("isn't supported in TEEHR yet" in e["msg"] and "#251" in e["msg"]
               for e in errors)  # fmt: skip
    assert "climatology dataset" in errors[0]["msg"]
    assert "percentiles dataset" in errors[1]["msg"]


def test_a_plan_warns_what_a_spec_does_that_it_may_not_expect(client, datasets):
    plain = client.post(URL, json=spec(), headers=ALICE).json()
    assert plain["warnings"] == []
    body = spec()
    body["aggregate"][0]["metrics"].append("spearman_correlation")
    warned = client.post(URL, json=body, headers=ALICE)
    assert warned.status_code == 200, warned.text
    [full] = warned.json()["warnings"]
    assert full.startswith("every run is full") and "spearman_correlation" in full
    # Saving shows them too, in its plan.
    saved = client.put("/v1/projects/flood/evaluations/w", json=body, headers=ALICE)
    assert saved.json()["plan"]["warnings"] == [full]
