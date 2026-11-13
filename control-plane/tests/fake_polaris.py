"""A small stateful stand-in for the Polaris REST API, served via httpx.MockTransport.

Covers just the calls the control plane makes, with Polaris's status codes
(201 create, 409 duplicate / namespace not empty, 404 missing), and checks the
realm header and bearer token the way the real server does.
"""

import json
from collections import defaultdict

import httpx

from src.polaris import PolarisAdmin

REALM = "teehr"
CATALOG = "teehr"
TOKEN = "fake-token"


class FakePolaris:
    def __init__(self):
        self.namespaces: set[str] = set()
        self.namespace_props: dict[str, dict[str, str]] = {}
        self.tables: dict[str, set[str]] = defaultdict(set)
        self.principal_roles: set[str] = set()
        self.catalog_roles: set[str] = set()
        self.grants: dict[str, set[tuple[str, str]]] = defaultdict(set)
        self.assignments: dict[str, set[str]] = defaultdict(set)
        self.token_requests = 0
        self.fail_paths: set[str] = set()  # path substrings that answer 500

    def admin(self) -> PolarisAdmin:
        return PolarisAdmin(
            "http://polaris.test",
            REALM,
            CATALOG,
            "root",
            "secret",
            # Late-bound so a test can swap ``handle`` after building the client.
            transport=httpx.MockTransport(lambda request: self.handle(request)),
        )

    # --- request handling ---------------------------------------------------

    def handle(self, request: httpx.Request) -> httpx.Response:
        path, method = request.url.path, request.method
        if request.headers.get("X-Polaris-Realm") != REALM:
            return httpx.Response(400, json={"error": "missing realm"})
        if path == "/api/catalog/v1/oauth/tokens":
            self.token_requests += 1
            return httpx.Response(200, json={"access_token": TOKEN, "expires_in": 3600})
        if request.headers.get("Authorization") != f"Bearer {TOKEN}":
            return httpx.Response(401)
        if any(fragment in path for fragment in self.fail_paths):
            return httpx.Response(500, json={"error": "boom"})
        body = json.loads(request.content) if request.content else {}

        ns_root = f"/api/catalog/v1/{CATALOG}/namespaces"
        mgmt = "/api/management/v1"
        if path == ns_root and method == "POST":
            name = body["namespace"][0]
            resp = self._create(self.namespaces, name, ok=200)
            if resp.status_code == 200:
                self.namespace_props[name] = dict(body.get("properties") or {})
            return resp
        if path.startswith(ns_root + "/"):
            return self._namespace(method, path.removeprefix(ns_root + "/"))
        if path == f"{mgmt}/principal-roles" and method == "POST":
            return self._create(self.principal_roles, body["principalRole"]["name"])
        if path == f"{mgmt}/catalogs/{CATALOG}/catalog-roles" and method == "POST":
            return self._create(self.catalog_roles, body["catalogRole"]["name"])

        role_root = f"{mgmt}/catalogs/{CATALOG}/catalog-roles/"
        if path.startswith(role_root):
            role, _, rest = path.removeprefix(role_root).partition("/")
            if role not in self.catalog_roles:
                return httpx.Response(404)
            if rest == "grants" and method == "PUT":
                grant = body["grant"]
                self.grants[role].add(
                    (".".join(grant["namespace"]), grant["privilege"])
                )
                return httpx.Response(201)
            if rest == "grants" and method == "GET":
                grants = [
                    {"type": "namespace", "namespace": [ns], "privilege": p}
                    for ns, p in sorted(self.grants[role])
                ]
                return httpx.Response(200, json={"grants": grants})
            if rest == "" and method == "DELETE":
                self.catalog_roles.discard(role)
                self.grants.pop(role, None)
                for assigned in self.assignments.values():
                    assigned.discard(role)
                return httpx.Response(204)

        pr_root = f"{mgmt}/principal-roles/"
        if path.startswith(pr_root):
            principal, _, rest = path.removeprefix(pr_root).partition("/")
            if principal not in self.principal_roles:
                return httpx.Response(404)
            if rest == f"catalog-roles/{CATALOG}" and method == "PUT":
                role = body["catalogRole"]["name"]
                if role not in self.catalog_roles:
                    return httpx.Response(404)
                self.assignments[principal].add(role)
                return httpx.Response(201)
            if rest == f"catalog-roles/{CATALOG}" and method == "GET":
                roles = [{"name": r} for r in sorted(self.assignments[principal])]
                return httpx.Response(200, json={"roles": roles})
            if rest == "" and method == "DELETE":
                self.principal_roles.discard(principal)
                self.assignments.pop(principal, None)
                return httpx.Response(204)

        return httpx.Response(405, json={"error": f"unexpected {method} {path}"})

    def add_foreign_namespace(
        self, name: str, properties: dict[str, str] | None = None
    ):
        """A namespace the control plane didn't create (e.g. legacy `teehr`)."""
        self.namespaces.add(name)
        self.namespace_props[name] = dict(properties or {})

    def _create(self, store: set[str], name: str, ok: int = 201) -> httpx.Response:
        if name in store:
            return httpx.Response(409, json={"error": "already exists"})
        store.add(name)
        return httpx.Response(ok, json={})

    def _namespace(self, method: str, name: str) -> httpx.Response:
        if method == "GET":
            if name not in self.namespaces:
                return httpx.Response(404)
            props = self.namespace_props.get(name, {})
            return httpx.Response(200, json={"namespace": [name], "properties": props})
        if method == "DELETE":
            if name not in self.namespaces:
                return httpx.Response(404)
            if self.tables[name]:
                return httpx.Response(409, json={"error": "Namespace not empty"})
            self.namespaces.discard(name)
            self.namespace_props.pop(name, None)
            return httpx.Response(204)
        return httpx.Response(405)
