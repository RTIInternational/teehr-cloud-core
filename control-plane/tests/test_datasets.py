"""The dataset registry (Phase 2.1): register, list, get, delete - tables are
created and dropped as the caller."""

import pytest

from src.main import app
from src.tables import (
    NamespaceMissing,
    TableError,
    TableExists,
    TableForbidden,
    get_tables,
    iceberg_schema,
)
from tests.conftest import auth

ALICE = auth("alice")
BOB = auth("bob")
ROOT = auth("root", roles=("admin",))
COLUMNS = [
    {"name": "value_time", "type": "timestamptz", "required": True},
    {"name": "value", "type": "float64", "doc": "cms"},
]


class FakeTables:
    def __init__(self):
        self.tables: dict[tuple[str, str], list[dict]] = {}
        self.calls: list[tuple[str, str, str, str]] = []  # (op, token, ns, name)
        self.fail_with: Exception | None = None

    def create_table(self, token, namespace, name, columns):
        self.calls.append(("create", token, namespace, name))
        if self.fail_with:
            raise self.fail_with
        if (namespace, name) in self.tables:
            raise TableExists(name)
        self.tables[(namespace, name)] = columns
        return f"s3://warehouse/{namespace}/{name}"

    def drop_table(self, token, namespace, name):
        self.calls.append(("drop", token, namespace, name))
        if self.fail_with:
            raise self.fail_with
        self.tables.pop((namespace, name), None)


@pytest.fixture
def tables(client, polaris) -> FakeTables:
    """Projects get provisioned (fake Polaris), tables go to a fake."""
    fake = FakeTables()
    app.dependency_overrides[get_tables] = lambda: fake
    client.post("/v1/projects", json={"name": "flood"}, headers=ALICE)
    return fake


def register(client, name="obs", headers=ALICE, columns=COLUMNS, **extra):
    return client.post(
        "/v1/projects/flood/datasets",
        json={"name": name, "columns": columns, **extra},
        headers=headers,
    )


def add_member(client, who, role):
    resp = client.put(
        f"/v1/projects/flood/members/{who}", json={"role": role}, headers=ALICE
    )
    assert resp.status_code in (200, 201)


def test_register_creates_the_table_as_the_caller(client, tables):
    resp = register(client, description="gauge obs")
    assert resp.status_code == 201
    body = resp.json()
    assert body["table"] == "flood.obs"
    assert body["kind"] == "table"
    assert body["created_by"] == "alice"
    assert body["description"] == "gauge obs"
    assert [c["name"] for c in body["columns"]] == ["value_time", "value"]
    assert tables.tables[("flood", "obs")][0]["required"] is True
    op, token, ns, name = tables.calls[0]
    assert (op, ns, name) == ("create", "flood", "obs")
    assert token == ALICE["Authorization"].removeprefix("Bearer ")


def test_list_and_get(client, tables):
    register(client, "obs")
    register(client, "sim")
    names = [
        d["name"]
        for d in client.get("/v1/projects/flood/datasets", headers=ALICE).json()
    ]
    assert names == ["obs", "sim"]
    resp = client.get("/v1/projects/flood/datasets/sim", headers=ALICE)
    assert resp.status_code == 200 and resp.json()["table"] == "flood.sim"
    missing = client.get("/v1/projects/flood/datasets/nope", headers=ALICE)
    assert missing.status_code == 404


def test_reader_can_list_but_not_register(client, tables):
    register(client)
    add_member(client, "bob", "read")
    assert client.get("/v1/projects/flood/datasets", headers=BOB).status_code == 200
    resp = register(client, "mine", headers=BOB)
    assert resp.status_code == 403
    assert ("flood", "mine") not in tables.tables


def test_writer_can_register_but_not_delete(client, tables):
    add_member(client, "bob", "write")
    assert register(client, "bobs", headers=BOB).status_code == 201
    resp = client.delete("/v1/projects/flood/datasets/bobs", headers=BOB)
    assert resp.status_code == 403
    assert ("flood", "bobs") in tables.tables


def test_non_member_gets_404(client, tables):
    register(client)
    assert client.get("/v1/projects/flood/datasets", headers=BOB).status_code == 404
    assert register(client, "x", headers=BOB).status_code == 404


def test_delete_drops_the_table_then_the_record(client, tables):
    register(client)
    resp = client.delete("/v1/projects/flood/datasets/obs", headers=ALICE)
    assert resp.status_code == 204
    assert ("flood", "obs") not in tables.tables
    assert (
        client.get("/v1/projects/flood/datasets/obs", headers=ALICE).status_code == 404
    )


def test_duplicate_dataset_is_409(client, tables):
    register(client)
    assert register(client).status_code == 409
    assert len([c for c in tables.calls if c[0] == "create"]) == 1


def test_unregistered_table_of_that_name_is_409(client, tables):
    tables.tables[("flood", "obs")] = []
    resp = register(client)
    assert resp.status_code == 409
    assert "isn't a registered dataset" in resp.json()["detail"]


def test_stale_token_gets_a_refresh_code(client, tables):
    """A member whose token predates their role: Polaris refuses, we say why."""
    tables.fail_with = TableForbidden("no activated role")
    resp = register(client)
    assert resp.status_code == 403
    assert resp.json()["code"] == "token_refresh_required"
    # Nothing was recorded.
    assert client.get("/v1/projects/flood/datasets", headers=ALICE).json() == []


def test_platform_admin_refused_by_catalog_gets_no_refresh_code(client, tables):
    tables.fail_with = TableForbidden("no grants")
    resp = register(client, headers=ROOT)
    assert resp.status_code == 403
    assert resp.json()["code"] is None


@pytest.mark.parametrize(
    "error, status_code",
    [(NamespaceMissing("x"), 409), (TableError("boom"), 502)],
)
def test_catalog_failures(client, tables, error, status_code):
    tables.fail_with = error
    assert register(client).status_code == status_code
    assert client.get("/v1/projects/flood/datasets", headers=ALICE).json() == []


def test_project_not_ready_is_409(client, tables, polaris):
    polaris.fail_paths.add("/namespaces")
    client.post("/v1/projects", json={"name": "broken"}, headers=ALICE)
    resp = client.post(
        "/v1/projects/broken/datasets",
        json={"name": "obs", "columns": COLUMNS},
        headers=ALICE,
    )
    assert resp.status_code == 409
    assert "provision" in resp.json()["detail"]


def test_without_table_storage_503(client, tables):
    app.dependency_overrides[get_tables] = lambda: None
    assert register(client).status_code == 503


def test_system_project_datasets_are_listed_for_everyone(client, tables, db_url):
    from sqlalchemy.orm import Session

    from src.db import make_engine
    from src.provisioning import ensure_system_projects

    with Session(make_engine(db_url)) as session:
        ensure_system_projects(session)
    resp = client.get("/v1/projects/public/datasets", headers=BOB)
    assert resp.status_code == 200
    # ...but a non-member can't register one there.
    resp = client.post(
        "/v1/projects/public/datasets",
        json={"name": "obs", "columns": COLUMNS},
        headers=BOB,
    )
    assert resp.status_code == 403


@pytest.mark.parametrize(
    "columns",
    [
        [],
        [{"name": "a", "type": "decimal"}],
        [{"name": "1bad", "type": "string"}],
        [{"name": "a", "type": "string"}, {"name": "A", "type": "int32"}],
        [{"name": "a", "type": "string", "nullable": True}],
    ],
)
def test_invalid_columns_are_422(client, tables, columns):
    assert register(client, columns=columns).status_code == 422
    assert tables.calls == []


def test_iceberg_schema_maps_declared_columns():
    schema = iceberg_schema(COLUMNS)
    first, second = schema.fields
    assert (first.field_id, first.name, str(first.field_type), first.required) == (
        1,
        "value_time",
        "timestamptz",
        True,
    )
    assert (second.name, str(second.field_type), second.required, second.doc) == (
        "value",
        "double",
        False,
        "cms",
    )
