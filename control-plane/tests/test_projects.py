"""Project CRUD and membership rules."""

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.db import make_engine
from src.models import Membership
from tests.conftest import ADMIN_ROLE, auth

ALICE, BOB, CAROL = auth("alice"), auth("bob"), auth("carol")
PLATFORM_ADMIN = auth("root", roles=(ADMIN_ROLE,))


def create(client, name="flood_study", headers=ALICE, **body):
    return client.post("/v1/projects", json={"name": name, **body}, headers=headers)


def add_member(client, subject, role, project="flood_study", headers=ALICE):
    return client.put(
        f"/v1/projects/{project}/members/{subject}",
        json={"role": role},
        headers=headers,
    )


# --- create ---------------------------------------------------------------


def test_create_makes_caller_admin_and_starts_pending(client):
    resp = create(client, display_name="Flood study")
    assert resp.status_code == 201
    body = resp.json()
    assert body["name"] == "flood_study"
    assert body["kind"] == "tenant"
    assert body["status"] == "pending"
    assert body["created_by"] == "alice"
    assert body["my_role"] == "admin"


def test_duplicate_name_is_409(client):
    create(client)
    assert create(client, headers=BOB).status_code == 409


@pytest.mark.parametrize("name", ["public", "platform", "teehr"])
def test_reserved_names_are_409(client, name):
    assert create(client, name=name).status_code == 409


@pytest.mark.parametrize("name", ["Flood", "1flood", "flood-study", "", "x" * 61])
def test_invalid_names_are_422(client, name):
    assert create(client, name=name).status_code == 422


# --- read / list ------------------------------------------------------------


def test_list_shows_only_my_projects(client):
    create(client, name="alpha")
    create(client, name="beta", headers=BOB)
    names = [p["name"] for p in client.get("/v1/projects", headers=ALICE).json()]
    assert names == ["alpha"]


def test_platform_admin_lists_every_project(client):
    create(client, name="alpha")
    create(client, name="beta", headers=BOB)
    projects = client.get("/v1/projects", headers=PLATFORM_ADMIN).json()
    assert [p["name"] for p in projects] == ["alpha", "beta"]
    assert all(p["my_role"] is None for p in projects)


def test_non_member_gets_404(client):
    create(client)
    assert client.get("/v1/projects/flood_study", headers=BOB).status_code == 404
    assert (
        client.get("/v1/projects/flood_study/members", headers=BOB).status_code == 404
    )


def test_missing_project_is_404(client):
    assert client.get("/v1/projects/nope", headers=ALICE).status_code == 404


def test_reader_can_read(client):
    create(client)
    add_member(client, "bob", "read")
    resp = client.get("/v1/projects/flood_study", headers=BOB)
    assert resp.status_code == 200
    assert resp.json()["my_role"] == "read"


# --- update / delete --------------------------------------------------------


def test_only_admin_can_update(client):
    create(client)
    add_member(client, "bob", "write")
    patch = {"description": "new"}
    assert (
        client.patch("/v1/projects/flood_study", json=patch, headers=BOB).status_code
        == 403
    )
    resp = client.patch("/v1/projects/flood_study", json=patch, headers=ALICE)
    assert resp.status_code == 200
    assert resp.json()["description"] == "new"


def test_platform_admin_can_update_without_membership(client):
    create(client)
    resp = client.patch(
        "/v1/projects/flood_study", json={"display_name": "X"}, headers=PLATFORM_ADMIN
    )
    assert resp.status_code == 200


def test_delete_removes_project_and_memberships(client, db_url):
    create(client)
    add_member(client, "bob", "write")
    assert client.delete("/v1/projects/flood_study", headers=BOB).status_code == 403
    assert client.delete("/v1/projects/flood_study", headers=ALICE).status_code == 204
    assert client.get("/v1/projects/flood_study", headers=ALICE).status_code == 404
    with Session(make_engine(db_url)) as session:
        assert session.scalar(select(func.count()).select_from(Membership)) == 0


# --- membership -------------------------------------------------------------


def test_add_then_change_member_role(client):
    create(client)
    assert add_member(client, "bob", "read").status_code == 201
    resp = add_member(client, "bob", "write")
    assert resp.status_code == 200
    assert resp.json()["role"] == "write"
    members = client.get("/v1/projects/flood_study/members", headers=BOB).json()
    assert {(m["subject"], m["role"]) for m in members} == {
        ("alice", "admin"),
        ("bob", "write"),
    }


def test_writer_cannot_manage_members(client):
    create(client)
    add_member(client, "bob", "write")
    assert add_member(client, "carol", "read", headers=BOB).status_code == 403


def test_last_admin_cannot_be_demoted_or_removed(client):
    create(client)
    assert add_member(client, "alice", "write").status_code == 409
    assert (
        client.delete(
            "/v1/projects/flood_study/members/alice", headers=ALICE
        ).status_code
        == 409
    )


def test_admin_can_step_down_once_another_admin_exists(client):
    create(client)
    add_member(client, "carol", "admin")
    assert add_member(client, "alice", "read").status_code == 200


def test_member_can_leave(client):
    create(client)
    add_member(client, "bob", "read")
    assert (
        client.delete("/v1/projects/flood_study/members/bob", headers=BOB).status_code
        == 204
    )
    assert client.get("/v1/projects/flood_study", headers=BOB).status_code == 404


def test_member_cannot_remove_someone_else(client):
    create(client)
    add_member(client, "bob", "write")
    add_member(client, "carol", "read")
    assert (
        client.delete("/v1/projects/flood_study/members/carol", headers=BOB).status_code
        == 403
    )


def test_removing_unknown_member_is_404(client):
    create(client)
    assert (
        client.delete(
            "/v1/projects/flood_study/members/ghost", headers=ALICE
        ).status_code
        == 404
    )
