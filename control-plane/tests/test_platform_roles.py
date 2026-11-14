"""Platform-wide roles: viewers see everything and change nothing; system-project
grants follow the configuration exactly."""

import dataclasses

import pytest

from src.config import get_settings
from src.db import make_engine
from src.main import app
from src.models import Role
from src.polaris import catalog_role_name
from src.provisioning import reconcile
from tests.conftest import auth
from tests.test_system_projects import SETTINGS

ALICE = auth("alice")
VIEWER_ROLE = "control-plane-viewer"


def _auth(sub, *roles):
    return auth(sub, roles=roles)


@pytest.fixture
def viewer_client(client):
    settings = dataclasses.replace(
        app.dependency_overrides[get_settings](), viewer_roles=(VIEWER_ROLE,)
    )
    app.dependency_overrides[get_settings] = lambda: settings
    client.post("/v1/projects", json={"name": "flood"}, headers=ALICE)
    return client


def test_viewer_lists_and_reads_every_project(viewer_client):
    viewer = _auth("vera", VIEWER_ROLE)
    listed = viewer_client.get("/v1/projects", headers=viewer).json()
    assert [p["name"] for p in listed] == ["flood"]
    assert listed[0]["my_role"] is None
    assert viewer_client.get("/v1/projects/flood", headers=viewer).status_code == 200
    members = viewer_client.get("/v1/projects/flood/members", headers=viewer).json()
    assert [m["subject"] for m in members] == ["alice"]


@pytest.mark.parametrize(
    "method, path, body",
    [
        ("patch", "/v1/projects/flood", {"description": "x"}),
        ("delete", "/v1/projects/flood", None),
        ("post", "/v1/projects/flood/provision", None),
        ("put", "/v1/projects/flood/members/vera", {"role": "admin"}),
        ("post", "/v1/projects/flood/members", {"username": "vera", "role": "read"}),
        ("delete", "/v1/projects/flood/members/alice", None),
    ],
)
def test_viewer_changes_nothing(viewer_client, method, path, body):
    kwargs = {"json": body} if body is not None else {}
    resp = getattr(viewer_client, method)(
        path, headers=_auth("vera", VIEWER_ROLE), **kwargs
    )
    assert resp.status_code == 403
    assert (
        viewer_client.get("/v1/projects/flood", headers=ALICE).json()["description"]
        is None
    )


def test_without_the_viewer_role_a_non_member_still_gets_404(viewer_client):
    bob = _auth("bob")
    assert viewer_client.get("/v1/projects/flood", headers=bob).status_code == 404
    assert viewer_client.get("/v1/projects", headers=bob).json() == []


def test_system_grants_follow_the_configuration(client, polaris, db_url):
    engine = make_engine(db_url)
    reconcile(engine, polaris.admin(), settings=SETTINGS)
    write = catalog_role_name("public", Role.WRITE)
    assert write in polaris.assignments["teehr-read-write"]

    narrowed = dataclasses.replace(SETTINGS, system_writer_roles=("platform-write",))
    reconcile(engine, polaris.admin(), settings=narrowed)
    assert write in polaris.assignments["platform-write"]
    assert write not in polaris.assignments["teehr-read-write"]
    # The project's own role keeps it, and readers are unchanged.
    assert write in polaris.assignments["teehr-project-public-write"]
    read = catalog_role_name("public", Role.READ)
    assert read in polaris.assignments["teehr-read-only"]


def test_tenant_grants_are_never_pruned(client, polaris):
    """Only system projects converge; a tenant's catalog roles keep any extra
    assignment (none are made today, but nothing should remove them silently)."""
    client.post("/v1/projects", json={"name": "flood"}, headers=ALICE)
    polaris.assignments["someone-else"].add(catalog_role_name("flood", Role.READ))
    assert client.post("/v1/projects/flood/provision", headers=ALICE).status_code == 200
    assert catalog_role_name("flood", Role.READ) in polaris.assignments["someone-else"]
