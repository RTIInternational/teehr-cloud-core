"""A small stateful stand-in for the Keycloak admin REST API (httpx.MockTransport).

Covers the calls the control plane makes - realm roles, users, and users' direct
realm-role mappings - and requires the admin bearer token like the real API.
"""

import json
from collections import defaultdict

import httpx

from src.keycloak_admin import KeycloakAdmin

REALM = "teehr"
TOKEN = "kc-admin-token"


class FakeKeycloak:
    def __init__(self, usernames=("alice", "bob", "carol")):
        # User id == username, matching the `sub` in the tests' tokens.
        self.users: dict[str, dict] = {u: {"id": u, "username": u} for u in usernames}
        self.roles: dict[str, str] = {}  # role name -> id
        self.user_roles: dict[str, set[str]] = defaultdict(set)
        self.fail_paths: set[str] = set()
        self.token = TOKEN
        self.token_requests = 0

    def admin(self) -> KeycloakAdmin:
        return KeycloakAdmin(
            "http://keycloak.test",
            REALM,
            "admin",
            "admin",
            transport=httpx.MockTransport(lambda request: self.handle(request)),
        )

    def revoke_tokens(self) -> None:
        """Like a Keycloak restart or session revocation: old tokens stop working."""
        self.token = f"{TOKEN}-{self.token_requests}-revoked"

    def handle(self, request: httpx.Request) -> httpx.Response:
        path, method = request.url.path, request.method
        if path == "/realms/master/protocol/openid-connect/token":
            self.token_requests += 1
            return httpx.Response(
                200, json={"access_token": self.token, "expires_in": 60}
            )
        if request.headers.get("Authorization") != f"Bearer {self.token}":
            return httpx.Response(401)
        if any(fragment in path for fragment in self.fail_paths):
            return httpx.Response(500, json={"error": "boom"})
        body = json.loads(request.content) if request.content else None
        root = f"/admin/realms/{REALM}"

        if path == f"{root}/roles" and method == "POST":
            if body["name"] in self.roles:
                return httpx.Response(409)
            self.roles[body["name"]] = f"id-{body['name']}"
            return httpx.Response(201)
        if path.startswith(f"{root}/roles/"):
            name = path.removeprefix(f"{root}/roles/")
            if name not in self.roles:
                return httpx.Response(404)
            if method == "GET":
                return httpx.Response(200, json={"id": self.roles[name], "name": name})
            if method == "DELETE":
                del self.roles[name]
                for held in self.user_roles.values():
                    held.discard(name)
                return httpx.Response(204)

        if path == f"{root}/users" and method == "GET":
            wanted = request.url.params.get("username")
            return httpx.Response(
                200, json=[u for u in self.users.values() if u["username"] == wanted]
            )
        if path.startswith(f"{root}/users/"):
            user_id, _, rest = path.removeprefix(f"{root}/users/").partition("/")
            if user_id not in self.users:
                return httpx.Response(404)
            if rest == "" and method == "GET":
                return httpx.Response(200, json=self.users[user_id])
            if rest == "role-mappings/realm":
                if method == "GET":
                    held = sorted(self.user_roles[user_id])
                    return httpx.Response(
                        200, json=[{"id": self.roles[n], "name": n} for n in held]
                    )
                names = {r["name"] for r in body}
                if any(n not in self.roles for n in names):
                    return httpx.Response(404)
                if method == "POST":
                    self.user_roles[user_id] |= names
                    return httpx.Response(204)
                if method == "DELETE":
                    self.user_roles[user_id] -= names
                    return httpx.Response(204)

        return httpx.Response(405, json={"error": f"unexpected {method} {path}"})
