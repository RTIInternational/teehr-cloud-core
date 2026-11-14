"""The system projects (public, platform): recorded and provisioned at startup,
readable by everyone, written by the platform, never deleted."""

import dataclasses
import uuid

import pytest
from sqlalchemy.orm import Session

from src.config import Settings, get_settings
from src.db import make_engine
from src.main import app
from src.models import Project, ProjectKind, ProjectStatus, Role
from src.polaris import catalog_role_name
from src.provisioning import SYSTEM_PROJECTS, reconcile
from tests.conftest import ISSUER, auth
from tests.fake_polaris import FakePolaris

ALICE = auth("alice")
ROOT = auth("root", roles=("admin",))
SETTINGS = Settings(
    database_url=None,
    keycloak_issuer_url=ISSUER,
    system_reader_roles=("teehr-read-only",),
    system_writer_roles=("teehr-read-write",),
)


@pytest.fixture
def system(client, polaris, db_url) -> FakePolaris:
    """Reconcile once, as the service does at startup, with system roles set."""
    reconcile(make_engine(db_url), polaris.admin(), settings=SETTINGS)
    return polaris


def _statuses(db_url) -> dict[str, tuple[ProjectKind, ProjectStatus]]:
    with Session(make_engine(db_url)) as session:
        return {p.name: (p.kind, p.status) for p in session.query(Project)}


def test_startup_records_and_provisions_both(system, db_url):
    assert _statuses(db_url) == dict.fromkeys(
        SYSTEM_PROJECTS, (ProjectKind.SYSTEM, ProjectStatus.READY)
    )
    assert {"public", "platform"} <= system.namespaces
    # Fresh namespaces under their own prefixes, not the legacy `teehr` one.
    assert "teehr" not in system.namespaces
    assert system.namespace_props["public"]["location"] == "s3://warehouse/public/"


def test_everyone_reads_platform_writes(system):
    for name in SYSTEM_PROJECTS:
        read, write = (catalog_role_name(name, r) for r in (Role.READ, Role.WRITE))
        assert read in system.assignments["teehr-read-only"]
        assert write in system.assignments["teehr-read-write"]
        # Neither shared role gets more than that.
        assert write not in system.assignments["teehr-read-only"]
        assert (
            catalog_role_name(name, Role.ADMIN)
            not in system.assignments["teehr-read-write"]
        )


def test_without_polaris_they_are_still_recorded(db_url):
    assert reconcile(make_engine(db_url), None) == 0
    assert {n for n, (_, s) in _statuses(db_url).items()} == set(SYSTEM_PROJECTS)
    assert all(s == ProjectStatus.PENDING for _, s in _statuses(db_url).values())


def test_reconcile_twice_is_idempotent(system, db_url):
    reconcile(make_engine(db_url), system.admin(), settings=SETTINGS)
    assert len(_statuses(db_url)) == 2
    assert all(s == ProjectStatus.READY for _, s in _statuses(db_url).values())


def test_any_user_lists_and_reads_them(client, system):
    client.post("/v1/projects", json={"name": "flood"}, headers=ALICE)
    listed = client.get("/v1/projects", headers=auth("bob")).json()
    assert {p["name"] for p in listed} == set(SYSTEM_PROJECTS)
    assert all(p["kind"] == "system" and p["my_role"] is None for p in listed)
    assert client.get("/v1/projects/public", headers=auth("bob")).status_code == 200
    members = client.get("/v1/projects/public/members", headers=auth("bob"))
    assert members.status_code == 200


@pytest.mark.parametrize(
    "method, path, body",
    [
        ("patch", "/v1/projects/public", {"description": "mine now"}),
        ("post", "/v1/projects/public/provision", None),
        ("put", "/v1/projects/public/members/bob", {"role": "write"}),
        ("delete", "/v1/projects/public", None),
    ],
)
def test_users_cannot_change_them(client, system, method, path, body):
    kwargs = {"json": body} if body is not None else {}
    resp = getattr(client, method)(path, headers=ALICE, **kwargs)
    assert resp.status_code == 403


def test_nobody_can_delete_them(client, system):
    assert client.delete("/v1/projects/public", headers=ROOT).status_code == 409
    assert "public" in system.namespaces


def test_reserved_names_cannot_be_created(client, system):
    for name in SYSTEM_PROJECTS:
        resp = client.post("/v1/projects", json={"name": name}, headers=ROOT)
        assert resp.status_code == 409


def test_platform_admin_reprovisions_with_system_grants(client, system):
    settings = dataclasses.replace(
        app.dependency_overrides[get_settings](),
        system_reader_roles=SETTINGS.system_reader_roles,
        system_writer_roles=SETTINGS.system_writer_roles,
    )
    app.dependency_overrides[get_settings] = lambda: settings
    system.assignments["teehr-read-only"].clear()  # e.g. revoked by hand
    resp = client.post("/v1/projects/public/provision", headers=ROOT)
    assert resp.status_code == 200 and resp.json()["status"] == "ready"
    assert (
        catalog_role_name("public", Role.READ) in system.assignments["teehr-read-only"]
    )


def test_a_tenant_never_gets_the_system_grants(client, system):
    client.post("/v1/projects", json={"name": "flood"}, headers=ALICE)
    for role in Role:
        held = (
            system.assignments["teehr-read-only"]
            | system.assignments["teehr-read-write"]
        )
        assert catalog_role_name("flood", role) not in held


def test_existing_tenant_named_like_a_system_project_is_left_alone(db_url):
    """A row named 'public' that isn't a system project isn't hijacked."""
    with Session(make_engine(db_url)) as session:
        session.add(
            Project(
                id=uuid.uuid4(),
                name="platform",
                kind=ProjectKind.TENANT,
                status=ProjectStatus.READY,
                created_by="x",
            )
        )
        session.commit()
    reconcile(make_engine(db_url), None)
    assert _statuses(db_url)["platform"][0] == ProjectKind.TENANT
