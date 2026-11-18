"""Saved evaluations: planned on save, versioned when the spec changes."""

from tests.test_datasets import ALICE, BOB, add_member
from tests.test_datasets import tables as tables  # noqa: F401  (fixture)
from tests.test_runs_plan import datasets as datasets  # noqa: F401  (fixture)
from tests.test_runs_plan import spec

URL = "/v1/projects/flood/evaluations"


def test_save_plans_and_versions_the_spec(client, datasets):  # noqa: F811
    resp = client.put(f"{URL}/nwm_vs_usgs", json=spec(), headers=ALICE)
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert (body["slug"], body["version"], body["changed"]) == ("nwm_vs_usgs", 1, True)
    assert body["plan"]["relation"] == {"dataset": "usgs_nwm", "reversed": False}

    # The same meaning (an alias, keys in another order) saves nothing new.
    same = spec()
    same["aggregate"][0]["metrics"][1] = "root_mean_square_error"
    same = dict(reversed(list(same.items())))
    resp = client.put(f"{URL}/nwm_vs_usgs", json=same, headers=ALICE)
    assert resp.status_code == 200, resp.text
    assert (resp.json()["version"], resp.json()["changed"]) == (1, False)

    changed = spec(output={"unit": "[cft_i]/s"})
    resp = client.put(f"{URL}/nwm_vs_usgs", json=changed, headers=ALICE)
    assert (resp.json()["version"], resp.json()["changed"]) == (2, True)
    assert [v["version"] for v in resp.json()["versions"]] == [1, 2]

    got = client.get(f"{URL}/nwm_vs_usgs", headers=ALICE).json()
    assert got["version"] == 2 and got["spec"] == changed
    assert client.get(f"{URL}/nwm_vs_usgs/versions/1", headers=ALICE).json() == spec()
    assert client.get(f"{URL}/nwm_vs_usgs/versions/3", headers=ALICE).status_code == 404
    listed = client.get(URL, headers=ALICE).json()
    assert [(e["slug"], e["version"]) for e in listed] == [("nwm_vs_usgs", 2)]


def test_a_spec_that_doesnt_plan_isnt_saved(client, datasets):  # noqa: F811
    resp = client.put(f"{URL}/broken", json=spec(primary="nope"), headers=ALICE)
    assert resp.status_code == 422
    assert resp.json()["errors"][0]["msg"] == "no dataset 'nope' in flood"
    assert client.get(f"{URL}/broken", headers=ALICE).status_code == 404


def test_slugs_fit_the_results_table_name(client, datasets):  # noqa: F811
    assert (
        client.put(f"{URL}/{'a' * 46}", json=spec(), headers=ALICE).status_code == 422
    )
    assert client.put(f"{URL}/Bad-Name", json=spec(), headers=ALICE).status_code == 422


def test_roles(client, datasets):  # noqa: F811
    client.put(f"{URL}/e", json=spec(), headers=ALICE)
    add_member(client, "bob", "read")
    assert client.get(f"{URL}/e", headers=BOB).status_code == 200
    assert client.put(f"{URL}/e", json=spec(), headers=BOB).status_code == 403
    add_member(client, "bob", "write")
    assert client.put(f"{URL}/e", json=spec(), headers=BOB).status_code == 200
    assert client.delete(f"{URL}/e", headers=BOB).status_code == 403
    assert client.delete(f"{URL}/e", headers=ALICE).status_code == 204
    assert client.get(f"{URL}/e", headers=ALICE).status_code == 404
