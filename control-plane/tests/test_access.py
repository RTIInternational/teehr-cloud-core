"""Membership becomes data access: Keycloak realm roles + Polaris principal bindings."""

import pytest

from tests.conftest import auth

ALICE = auth("alice")

READ, WRITE, ADMIN = (f"teehr-project-flood-{r}" for r in ("read", "write", "admin"))


@pytest.fixture
def project(client, polaris, keycloak):
    body = client.post("/projects", json={"name": "flood"}, headers=ALICE).json()
    assert body["status"] == "ready"
    return body


def add(client, username, role):
    return client.post(
        "/projects/flood/members",
        json={"username": username, "role": role},
        headers=ALICE,
    )


def test_creator_gets_admin_access(project, polaris, keycloak):
    assert set(keycloak.roles) == {READ, WRITE, ADMIN}
    assert keycloak.user_roles["alice"] == {ADMIN}
    assert polaris.bindings["alice"] == {ADMIN}


def test_add_member_by_username(client, project, polaris, keycloak):
    resp = add(client, "bob", "read")
    assert resp.status_code == 201
    assert resp.json()["subject"] == "bob"
    assert resp.json()["username"] == "bob"
    assert keycloak.user_roles["bob"] == {READ}
    assert polaris.bindings["bob"] == {READ}


def test_unknown_username_is_404(client, project):
    assert add(client, "ghost", "read").status_code == 404


def test_role_change_swaps_access(client, project, polaris, keycloak):
    add(client, "bob", "read")
    assert add(client, "bob", "write").status_code == 200
    assert keycloak.user_roles["bob"] == {WRITE}
    assert polaris.bindings["bob"] == {WRITE}


def test_removed_member_loses_access_in_both(client, project, polaris, keycloak):
    add(client, "bob", "write")
    assert (
        client.delete("/projects/flood/members/bob", headers=ALICE).status_code == 204
    )
    assert keycloak.user_roles["bob"] == set()
    assert polaris.bindings["bob"] == set()


def test_member_deleted_from_keycloak_still_loses_polaris_access(
    client, project, polaris, keycloak
):
    add(client, "bob", "write")
    del keycloak.users["bob"]
    assert (
        client.delete("/projects/flood/members/bob", headers=ALICE).status_code == 204
    )
    assert polaris.bindings["bob"] == set()


def test_other_projects_roles_are_left_alone(client, project, polaris, keycloak):
    client.post("/projects", json={"name": "drought"}, headers=ALICE)
    add(client, "bob", "read")
    client.post(
        "/projects/drought/members",
        json={"username": "bob", "role": "write"},
        headers=ALICE,
    )
    client.delete("/projects/flood/members/bob", headers=ALICE)
    assert keycloak.user_roles["bob"] == {"teehr-project-drought-write"}
    assert polaris.bindings["bob"] == {"teehr-project-drought-write"}


def test_deleting_project_removes_roles_everywhere(client, project, polaris, keycloak):
    add(client, "bob", "read")
    assert client.delete("/projects/flood", headers=ALICE).status_code == 204
    assert not set(keycloak.roles) & {READ, WRITE, ADMIN}
    assert keycloak.user_roles["bob"] == set()
    assert polaris.bindings["bob"] == set()
    assert polaris.bindings["alice"] == set()


def test_access_failure_saves_membership_and_marks_project_failed(
    client, project, polaris, keycloak
):
    keycloak.fail_paths.add("/role-mappings/")
    resp = add(client, "bob", "read")
    assert resp.status_code == 502
    assert "Retry" in resp.json()["detail"]
    members = client.get("/projects/flood/members", headers=ALICE).json()
    assert "bob" in {m["subject"] for m in members}
    assert client.get("/projects/flood", headers=ALICE).json()["status"] == "failed"

    keycloak.fail_paths.clear()
    body = client.post("/projects/flood/provision", headers=ALICE).json()
    assert body["status"] == "ready"
    assert keycloak.user_roles["bob"] == {READ}
    assert polaris.bindings["bob"] == {READ}


def test_without_keycloak_membership_is_recorded_only(client, polaris):
    client.post("/projects", json={"name": "flood"}, headers=ALICE)
    resp = client.put(
        "/projects/flood/members/bob", json={"role": "read"}, headers=ALICE
    )
    assert resp.status_code == 201
    assert polaris.bindings == {}


def test_add_by_username_needs_keycloak(client, polaris):
    client.post("/projects", json={"name": "flood"}, headers=ALICE)
    assert add(client, "bob", "read").status_code == 503
