"""``GET /v1/me``: who the caller is, their platform roles and memberships."""

import dataclasses

from src.config import get_settings
from src.main import app
from tests.conftest import ADMIN_ROLE, auth
from tests.test_datasets import tables as tables  # noqa: F401  (fixture)
from tests.test_ingest import jobs as jobs  # noqa: F401  (fixture)
from tests.test_projects import ALICE, BOB, add_member, create
from tests.test_tokens import bearer, sign_in
from tests.test_tokens import device as device  # noqa: F401  (fixture)
from tests.test_uploads import staging as staging  # noqa: F401  (fixture)


def test_me_lists_memberships_with_roles(client):
    create(client, name="beta")
    create(client, name="alpha")
    create(client, name="gamma", headers=BOB)
    add_member(client, "alice", "read", project="gamma", headers=BOB)
    me = client.get("/v1/me", headers=ALICE)
    assert me.status_code == 200, me.text
    body = me.json()
    assert (body["subject"], body["username"]) == ("alice", "alice")
    assert body["projects"] == [
        {"project": "alpha", "role": "admin"},
        {"project": "beta", "role": "admin"},
        {"project": "gamma", "role": "read"},
    ]
    assert body["platform"] == {"admin": False, "viewer": False,
                                "can_create_projects": True}  # fmt: skip
    assert body["token"] is None


def test_me_says_platform_roles(client):
    settings = dataclasses.replace(
        app.dependency_overrides[get_settings](),
        viewer_roles=("viewer",), project_creator_roles=("creator",),
    )  # fmt: skip
    app.dependency_overrides[get_settings] = lambda: settings

    def platform(**kw):
        return client.get("/v1/me", headers=auth("x", **kw)).json()["platform"]

    assert platform() == {"admin": False, "viewer": False, "can_create_projects": False}
    assert platform(roles=("creator",))["can_create_projects"] is True
    assert platform(roles=("viewer",))["viewer"] is True
    assert platform(roles=(ADMIN_ROLE,)) == {"admin": True, "viewer": False,
                                             "can_create_projects": True}  # fmt: skip
    assert (
        client.get("/v1/me", headers=auth("x", roles=(ADMIN_ROLE,))).json()["projects"]
        == []
    )  # an admin sees every project, but is a member of none


def test_me_shows_the_personal_token_used(client, device):  # noqa: F811
    _, made = sign_in(client, device, scopes=["read"], projects=["flood"])
    body = client.get("/v1/me", headers=bearer(made["token"])).json()
    assert body["token"]["name"] == "laptop"
    assert body["token"]["scopes"] == ["read"]
    assert body["token"]["projects"] == ["flood"]


def test_me_needs_a_sign_in(client):
    assert client.get("/v1/me").status_code == 401
