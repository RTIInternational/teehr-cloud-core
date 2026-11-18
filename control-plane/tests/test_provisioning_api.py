"""Provisioning through the API: create, retry, delete, reconcile."""

import uuid

from sqlalchemy.orm import Session

from src.db import make_engine
from src.models import Project, ProjectKind, ProjectStatus
from src.provisioning import reconcile
from tests.conftest import auth
from tests.fake_polaris import FakePolaris

ALICE = auth("alice")


def create(client, name="flood"):
    return client.post("/v1/projects", json={"name": name}, headers=ALICE)


def test_create_provisions_and_reports_ready(client, polaris):
    body = create(client).json()
    assert body["status"] == "ready"
    assert body["status_detail"] is None
    assert "flood" in polaris.namespaces
    assert len(polaris.principal_roles) == 3


def test_failed_provisioning_is_reported_then_retried(client, polaris):
    polaris.fail_paths.add("/grants")
    body = create(client).json()
    assert body["status"] == "failed"
    assert "500" in body["status_detail"]

    polaris.fail_paths.clear()
    resp = client.post("/v1/projects/flood/provision", headers=ALICE)
    assert resp.status_code == 200
    assert resp.json()["status"] == "ready"
    assert resp.json()["status_detail"] is None


def test_only_admins_can_reprovision(client, polaris):
    create(client)
    client.put("/v1/projects/flood/members/bob", json={"role": "write"}, headers=ALICE)
    assert (
        client.post("/v1/projects/flood/provision", headers=auth("bob")).status_code
        == 403
    )


def test_delete_removes_namespace_and_roles(client, polaris):
    create(client)
    assert client.delete("/v1/projects/flood", headers=ALICE).status_code == 204
    assert polaris.namespaces == set()
    assert polaris.principal_roles == set()
    assert polaris.catalog_roles == set()


def test_delete_refused_while_project_has_tables(client, polaris):
    create(client)
    polaris.tables["flood"].add("streamflow")
    resp = client.delete("/v1/projects/flood", headers=ALICE)
    assert resp.status_code == 409
    assert "tables" in resp.json()["detail"]
    assert client.get("/v1/projects/flood", headers=ALICE).status_code == 200
    assert "flood" in polaris.namespaces


def test_delete_when_polaris_unreachable_keeps_project(client, polaris):
    create(client)
    polaris.fail_paths.add("/catalog-roles/")
    assert client.delete("/v1/projects/flood", headers=ALICE).status_code == 502
    assert client.get("/v1/projects/flood", headers=ALICE).status_code == 200


def test_project_named_after_an_existing_namespace_fails_safely(client, polaris):
    polaris.add_foreign_namespace("schema_evolution")
    body = create(client, name="schema_evolution").json()
    assert body["status"] == "failed"
    assert "already exists" in body["status_detail"]
    assert polaris.grants == {}
    # Deleting the project leaves the namespace it never owned.
    assert (
        client.delete("/v1/projects/schema_evolution", headers=ALICE).status_code == 204
    )
    assert "schema_evolution" in polaris.namespaces


def test_without_polaris_projects_stay_pending(client):
    assert create(client).json()["status"] == "pending"
    # Nothing was provisioned, so it can still be deleted.
    assert client.delete("/v1/projects/flood", headers=ALICE).status_code == 204


def test_second_reconcile_skips_projects_already_ready(db_url):
    engine = make_engine(db_url)
    with Session(engine) as session:
        session.add(
            Project(
                id=uuid.uuid4(),
                name="pend",
                kind=ProjectKind.TENANT,
                status=ProjectStatus.PENDING,
                created_by="x",
            )
        )
        session.commit()
    fake = FakePolaris()
    # The two system projects are provisioned every time; the tenant only once.
    assert reconcile(engine, fake.admin()) == 3
    assert reconcile(engine, fake.admin()) == 2


def test_project_reports_its_storage_location(client, polaris):
    body = create(client).json()
    assert body["storage_location"] == "s3://warehouse/flood/"
    assert polaris.namespace_props["flood"]["location"] == "s3://warehouse/flood/"
