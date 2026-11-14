"""Polaris management client, and project provisioning on top of it.

The same calls the ``polaris-bootstrap`` Job makes, packaged so the control plane
can provision a project at runtime: one namespace per project (ADR-0001) and, for
each project role, a principal role -> catalog role -> namespace grants chain.

Role names carry a fixed ``project`` infix so they can never collide with the
existing global roles (``teehr-read-only``/``teehr-read-write`` on the live
``teehr`` namespace) - a project named ``read`` would otherwise produce
``teehr-read-write``. Keycloak realm roles with the same principal-role names map
straight through Polaris's existing ``^teehr-(.+)$`` mapper (#155).
"""

import time
from collections.abc import Iterable, Mapping

import httpx

from src.models import Role

READ_PRIVILEGES = (
    "NAMESPACE_READ_PROPERTIES",
    "TABLE_LIST",
    "TABLE_READ_PROPERTIES",
    "TABLE_READ_DATA",
)
# Mirrors the legacy teehr-read-write grants. Writing table data directly is
# narrowed to the write gateway in Phase 2 (ADR-0004).
WRITE_PRIVILEGES = READ_PRIVILEGES + (
    "NAMESPACE_WRITE_PROPERTIES",
    "TABLE_CREATE",
    "TABLE_DROP",
    "TABLE_WRITE_PROPERTIES",
    "TABLE_WRITE_DATA",
)
# Project admins manage membership in the control plane, not in Polaris, so their
# catalog privileges are the same as a writer's.
ROLE_PRIVILEGES: dict[Role, tuple[str, ...]] = {
    Role.READ: READ_PRIVILEGES,
    Role.WRITE: WRITE_PRIVILEGES,
    Role.ADMIN: WRITE_PRIVILEGES,
}


def principal_role_name(project: str, role: Role) -> str:
    return f"teehr-project-{project}-{role.value}"


def catalog_role_name(project: str, role: Role) -> str:
    return f"project_{project}_{role.value}"


class PolarisError(Exception):
    """A Polaris call failed."""


class NamespaceNotEmpty(PolarisError):
    """The namespace still holds tables, so it was not dropped."""


class NamespaceTaken(PolarisError):
    """A namespace with the project's name exists and belongs to something else."""


# Set on every namespace the control plane creates. An existing namespace is only
# ever claimed (or dropped) when this matches the project, so a project can't be
# granted - or delete - a namespace that predates it (e.g. legacy `teehr`, or
# `schema_evolution`), whatever its name.
OWNER_PROPERTY = "teehr.control-plane.project-id"


CONCURRENCY_RETRIES = 3


def _is_concurrency_conflict(resp: httpx.Response) -> bool:
    return resp.status_code == 500 and "RetryOnConcurrencyException" in resp.text


def _is_duplicate(resp: httpx.Response) -> bool:
    # Polaris answers a duplicate create with 409, or (older builds) a 500 that
    # mentions a duplicate key.
    return resp.status_code == 409 or (
        resp.status_code == 500 and "duplicate key" in resp.text.lower()
    )


class PolarisAdmin:
    """Management access to one Polaris realm and catalog."""

    def __init__(
        self,
        base_url: str,
        realm: str,
        catalog: str,
        client_id: str,
        client_secret: str,
        *,
        warehouse: str | None = None,
        transport: httpx.BaseTransport | None = None,
        timeout: float = 10.0,
    ):
        self.catalog = catalog
        # Catalog warehouse root; projects are pinned to <warehouse>/<name>/.
        self.warehouse = warehouse
        self._client_id = client_id
        self._client_secret = client_secret
        self._http = httpx.Client(
            base_url=base_url,
            headers={"X-Polaris-Realm": realm},
            timeout=timeout,
            transport=transport,
        )
        self._token: str | None = None
        self._token_expires_at = 0.0

    # --- plumbing -----------------------------------------------------------

    def _bearer(self) -> dict[str, str]:
        if self._token is None or time.monotonic() >= self._token_expires_at:
            resp = self._http.post(
                "/api/catalog/v1/oauth/tokens",
                data={
                    "grant_type": "client_credentials",
                    "client_id": self._client_id,
                    "client_secret": self._client_secret,
                    "scope": "PRINCIPAL_ROLE:ALL",
                },
            )
            if resp.status_code != 200:
                raise PolarisError(f"token request failed: {resp.status_code}")
            body = resp.json()
            self._token = body["access_token"]
            # Refresh a minute early.
            self._token_expires_at = (
                time.monotonic() + int(body.get("expires_in", 300)) - 60
            )
        return {"Authorization": f"Bearer {self._token}"}

    def _call(
        self,
        method: str,
        path: str,
        ok: tuple[int, ...],
        *,
        allow_duplicate: bool = False,
        **kwargs,
    ) -> httpx.Response:
        reauthenticated = False
        attempt = 1
        while True:
            try:
                resp = self._http.request(
                    method, path, headers=self._bearer(), **kwargs
                )
            except httpx.HTTPError as exc:
                raise PolarisError(f"{method} {path}: {exc}") from exc
            if resp.status_code in ok or (allow_duplicate and _is_duplicate(resp)):
                return resp
            # A restarted Polaris rejects tokens it issued before: get a new one,
            # once.
            if resp.status_code == 401 and not reauthenticated:
                reauthenticated = True
                self._token = None
                continue
            # Polaris asks to be retried when two writers touch the same entity.
            if _is_concurrency_conflict(resp) and attempt < CONCURRENCY_RETRIES:
                time.sleep(0.2 * attempt)
                attempt += 1
                continue
            break
        raise PolarisError(f"{method} {path} -> {resp.status_code}: {resp.text[:300]}")

    def _mgmt(
        self, method: str, path: str, ok: tuple[int, ...], **kwargs
    ) -> httpx.Response:
        return self._call(method, f"/api/management/v1{path}", ok, **kwargs)

    def _catalog(
        self, method: str, path: str, ok: tuple[int, ...], **kwargs
    ) -> httpx.Response:
        return self._call(method, f"/api/catalog/v1/{self.catalog}{path}", ok, **kwargs)

    # --- namespaces ---------------------------------------------------------

    def create_namespace(self, name: str, properties: dict[str, str]) -> bool:
        """Create a namespace. True if created, False if it already existed."""
        resp = self._catalog(
            "POST",
            "/namespaces",
            (200,),
            allow_duplicate=True,
            json={"namespace": [name], "properties": properties},
        )
        return resp.status_code == 200

    def namespace_properties(self, name: str) -> dict[str, str] | None:
        """A namespace's properties, or None if it doesn't exist."""
        resp = self._catalog("GET", f"/namespaces/{name}", (200, 404))
        if resp.status_code == 404:
            return None
        return resp.json().get("properties") or {}

    def drop_namespace(self, name: str) -> None:
        """Drop an empty namespace. Raises :class:`NamespaceNotEmpty` if it has tables."""
        resp = self._catalog("DELETE", f"/namespaces/{name}", (204, 404, 409))
        if resp.status_code == 409:
            raise NamespaceNotEmpty(f"namespace '{name}' still has tables")

    # --- roles and grants ---------------------------------------------------

    def ensure_principal_role(self, name: str) -> None:
        self._mgmt(
            "POST",
            "/principal-roles",
            (201,),
            allow_duplicate=True,
            json={"principalRole": {"name": name}},
        )

    def ensure_catalog_role(self, name: str) -> None:
        self._mgmt(
            "POST",
            f"/catalogs/{self.catalog}/catalog-roles",
            (201,),
            allow_duplicate=True,
            json={"catalogRole": {"name": name}},
        )

    def grant_on_namespace(
        self, catalog_role: str, namespace: str, privilege: str
    ) -> None:
        self._mgmt(
            "PUT",
            f"/catalogs/{self.catalog}/catalog-roles/{catalog_role}/grants",
            (200, 201),
            json={
                "grant": {
                    "type": "namespace",
                    "namespace": [namespace],
                    "privilege": privilege,
                }
            },
        )

    def assign_catalog_role(self, principal_role: str, catalog_role: str) -> None:
        self._mgmt(
            "PUT",
            f"/principal-roles/{principal_role}/catalog-roles/{self.catalog}",
            (200, 201),
            json={"catalogRole": {"name": catalog_role}},
        )

    def unassign_catalog_role(self, principal_role: str, catalog_role: str) -> None:
        self._mgmt(
            "DELETE",
            f"/principal-roles/{principal_role}/catalog-roles/{self.catalog}/"
            f"{catalog_role}",
            (204, 404),
        )

    def catalog_role_assignees(self, catalog_role: str) -> set[str]:
        """Principal roles the catalog role is assigned to."""
        roles = self._mgmt(
            "GET",
            f"/catalogs/{self.catalog}/catalog-roles/{catalog_role}/principal-roles",
            (200,),
        ).json()["roles"]
        return {r["name"] for r in roles}

    def delete_catalog_role(self, name: str) -> None:
        self._mgmt(
            "DELETE", f"/catalogs/{self.catalog}/catalog-roles/{name}", (204, 404)
        )

    def delete_principal_role(self, name: str) -> None:
        self._mgmt("DELETE", f"/principal-roles/{name}", (204, 404))

    # --- named principals (one per Keycloak user, by username) ----------------

    def ensure_principal(self, name: str) -> None:
        """Create the user's named principal if the principal-sync CronJob hasn't yet."""
        self._mgmt(
            "POST",
            "/principals",
            (201,),
            allow_duplicate=True,
            json={"principal": {"name": name}},
        )

    def principal_roles_of(self, principal: str) -> set[str]:
        """Principal roles bound to a principal (none if the principal doesn't exist)."""
        resp = self._mgmt("GET", f"/principals/{principal}/principal-roles", (200, 404))
        if resp.status_code == 404:
            return set()
        return {r["name"] for r in resp.json()["roles"]}

    def grant_principal_role(self, principal: str, role: str) -> None:
        self._mgmt(
            "PUT",
            f"/principals/{principal}/principal-roles",
            (200, 201),
            allow_duplicate=True,
            json={"principalRole": {"name": role}},
        )

    def revoke_principal_role(self, principal: str, role: str) -> None:
        self._mgmt(
            "DELETE", f"/principals/{principal}/principal-roles/{role}", (204, 404)
        )

    # --- read-back (verification and tests) ---------------------------------

    def namespace_grants(self, catalog_role: str) -> set[tuple[str, str]]:
        """(namespace, privilege) pairs granted to a catalog role."""
        grants = self._mgmt(
            "GET",
            f"/catalogs/{self.catalog}/catalog-roles/{catalog_role}/grants",
            (200,),
        ).json()["grants"]
        return {
            (".".join(g["namespace"]), g["privilege"])
            for g in grants
            if g["type"] == "namespace"
        }

    def assigned_catalog_roles(self, principal_role: str) -> set[str]:
        roles = self._mgmt(
            "GET",
            f"/principal-roles/{principal_role}/catalog-roles/{self.catalog}",
            (200,),
        ).json()["roles"]
        return {r["name"] for r in roles}


def _owned_by(properties: dict[str, str] | None, owner: str) -> bool:
    return properties is not None and properties.get(OWNER_PROPERTY) == owner


def namespace_location(warehouse: str, project: str) -> str:
    """The project's storage prefix: ``<warehouse>/<project>/``."""
    return f"{warehouse.rstrip('/')}/{project}/"


def provision(
    admin: PolarisAdmin,
    project: str,
    owner: str,
    warehouse: str | None = None,
    also_granted: Mapping[Role, Iterable[str]] | None = None,
) -> str | None:
    """Create (or complete) a project's namespace, roles and grants. Idempotent.

    ``owner`` is the project's id. With ``warehouse`` the namespace is pinned to
    ``<warehouse>/<project>/``; Polaris keeps the project's tables under it and
    scopes the credentials it vends for them to their own paths. Returns the
    namespace's storage location as Polaris reports it.

    ``also_granted`` gives existing principal roles a project role as well - how
    system projects reach every user (e.g. ``{Role.READ: ["teehr-read-only"]}``).
    When given, it is exact: any other principal role holding the project role
    (besides the project's own) loses it.

    Raises :class:`NamespaceTaken` - before any role or grant exists - if a
    namespace of that name already belongs to something else.
    """
    properties = {OWNER_PROPERTY: owner}
    if warehouse:
        properties["location"] = namespace_location(warehouse, project)
    created = admin.create_namespace(project, properties)
    current = admin.namespace_properties(project)
    if not created and not _owned_by(current, owner):
        raise NamespaceTaken(
            f"a namespace named '{project}' already exists and is not this project's"
        )
    for role in Role:
        principal, catalog_role = (
            principal_role_name(project, role),
            catalog_role_name(project, role),
        )
        admin.ensure_principal_role(principal)
        admin.ensure_catalog_role(catalog_role)
        for privilege in ROLE_PRIVILEGES[role]:
            admin.grant_on_namespace(catalog_role, project, privilege)
        admin.assign_catalog_role(principal, catalog_role)
        if also_granted is None:
            continue
        wanted = set(also_granted.get(role, ()))
        for other in wanted:
            admin.ensure_principal_role(other)
            admin.assign_catalog_role(other, catalog_role)
        # Converge: a role dropped from the configuration loses the access.
        for other in admin.catalog_role_assignees(catalog_role) - wanted - {principal}:
            admin.unassign_catalog_role(other, catalog_role)
    return (current or {}).get("location")


def deprovision(admin: PolarisAdmin, project: str, owner: str) -> None:
    """Remove a project's namespace and roles.

    The namespace goes first, only if this project owns it and only if it is empty,
    so a project that still holds tables is never half-removed
    (:class:`NamespaceNotEmpty` leaves everything as is). A namespace owned by
    something else is left alone; the project's own roles are still removed.
    """
    if _owned_by(admin.namespace_properties(project), owner):
        admin.drop_namespace(project)
    for role in Role:
        admin.delete_catalog_role(catalog_role_name(project, role))
        admin.delete_principal_role(principal_role_name(project, role))
