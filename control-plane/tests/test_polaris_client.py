"""The Polaris client and the provision/deprovision steps, against the fake server."""

import httpx
import pytest

from src.models import Role
from src.polaris import (
    OWNER_PROPERTY,
    READ_PRIVILEGES,
    WRITE_PRIVILEGES,
    NamespaceNotEmpty,
    NamespaceTaken,
    PolarisError,
    catalog_role_name,
    deprovision,
    principal_role_name,
    provision,
)
from tests.fake_polaris import FakePolaris

OWNER = "project-id-1"


@pytest.fixture
def fake():
    return FakePolaris()


# --- namespace ownership ------------------------------------------------------


def test_new_namespace_is_tagged_with_its_owner(fake):
    provision(fake.admin(), "flood", OWNER)
    assert fake.namespace_props["flood"] == {OWNER_PROPERTY: OWNER}


def test_existing_untagged_namespace_is_not_claimed(fake):
    # e.g. legacy `teehr` or `schema_evolution`, created outside the control plane.
    fake.add_foreign_namespace("schema_evolution")
    with pytest.raises(NamespaceTaken):
        provision(fake.admin(), "schema_evolution", OWNER)
    assert fake.principal_roles == set()
    assert fake.grants == {}


def test_namespace_of_another_project_is_not_claimed(fake):
    fake.add_foreign_namespace("flood", {OWNER_PROPERTY: "someone-else"})
    with pytest.raises(NamespaceTaken):
        provision(fake.admin(), "flood", OWNER)


def test_retry_reclaims_own_namespace(fake):
    admin = fake.admin()
    provision(admin, "flood", OWNER)
    fake.principal_roles.clear()  # as if interrupted before the roles
    provision(admin, "flood", OWNER)
    assert len(fake.principal_roles) == 3


def test_deprovision_never_drops_a_namespace_it_does_not_own(fake):
    fake.add_foreign_namespace("flood")
    deprovision(fake.admin(), "flood", OWNER)
    assert "flood" in fake.namespaces


def test_role_names_cannot_collide_with_legacy_roles():
    # Without the "project" infix, project "read" + role write == teehr-read-write.
    assert principal_role_name("read", Role.WRITE) == "teehr-project-read-write"
    assert catalog_role_name("read", Role.WRITE) == "project_read_write"


def test_provision_creates_namespace_roles_grants_and_assignments(fake):
    provision(fake.admin(), "flood", OWNER)

    assert fake.namespaces == {"flood"}
    for role in Role:
        principal, catalog = (
            principal_role_name("flood", role),
            catalog_role_name("flood", role),
        )
        assert principal in fake.principal_roles
        assert fake.assignments[principal] == {catalog}
    assert fake.grants["project_flood_read"] == {("flood", p) for p in READ_PRIVILEGES}
    assert fake.grants["project_flood_write"] == {
        ("flood", p) for p in WRITE_PRIVILEGES
    }
    assert fake.grants["project_flood_admin"] == {
        ("flood", p) for p in WRITE_PRIVILEGES
    }


def test_read_role_cannot_write(fake):
    provision(fake.admin(), "flood", OWNER)
    privileges = {p for _, p in fake.grants["project_flood_read"]}
    assert not privileges & {"TABLE_WRITE_DATA", "TABLE_CREATE", "TABLE_DROP"}


def test_provision_is_idempotent_and_reuses_the_token(fake):
    admin = fake.admin()
    provision(admin, "flood", OWNER)
    provision(admin, "flood", OWNER)
    assert fake.namespaces == {"flood"}
    assert fake.token_requests == 1


def test_older_polaris_duplicate_key_500_counts_as_existing(fake):
    admin = fake.admin()
    original = fake.handle

    def duplicate_key(request):
        if request.url.path.endswith("/principal-roles") and request.method == "POST":
            return httpx.Response(500, text="ERROR: duplicate key value violates")
        return original(request)

    fake.handle = duplicate_key
    admin.ensure_principal_role("teehr-project-x-read")  # no exception


def test_server_error_raises_polaris_error(fake):
    fake.fail_paths.add("/grants")
    with pytest.raises(PolarisError):
        provision(fake.admin(), "flood", OWNER)


def test_deprovision_removes_everything(fake):
    admin = fake.admin()
    provision(admin, "flood", OWNER)
    deprovision(admin, "flood", OWNER)
    assert fake.namespaces == set()
    assert fake.principal_roles == set()
    assert fake.catalog_roles == set()


def test_deprovision_keeps_everything_when_namespace_has_tables(fake):
    admin = fake.admin()
    provision(admin, "flood", OWNER)
    fake.tables["flood"].add("streamflow")
    with pytest.raises(NamespaceNotEmpty):
        deprovision(admin, "flood", OWNER)
    assert "flood" in fake.namespaces
    assert len(fake.principal_roles) == 3


def test_deprovision_of_unprovisioned_project_is_a_no_op(fake):
    deprovision(fake.admin(), "never_provisioned", OWNER)


# --- Polaris concurrency conflicts --------------------------------------------


def _conflict_on_grants(fake, times):
    """Answer the first ``times`` grant writes with Polaris's retry-me 500."""
    original, remaining = fake.handle, {"n": times}

    def handle(request):
        if request.url.path.endswith("/grants") and request.method == "PUT":
            if remaining["n"] > 0:
                remaining["n"] -= 1
                return httpx.Response(
                    500,
                    json={"error": {"type": "RetryOnConcurrencyException"}},
                )
        return original(request)

    fake.handle = handle


def test_concurrency_conflict_is_retried(fake, monkeypatch):
    monkeypatch.setattr("src.polaris.time.sleep", lambda _s: None)
    _conflict_on_grants(fake, times=1)
    provision(fake.admin(), "flood", OWNER)
    assert len(fake.grants["project_flood_read"]) == len(READ_PRIVILEGES)


def test_persistent_concurrency_conflict_fails(fake, monkeypatch):
    monkeypatch.setattr("src.polaris.time.sleep", lambda _s: None)
    _conflict_on_grants(fake, times=10)
    with pytest.raises(PolarisError, match="RetryOnConcurrencyException"):
        provision(fake.admin(), "flood", OWNER)
