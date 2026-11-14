"""Keycloak admin client: the realm roles that carry project access.

A project member's access reaches Polaris through a realm role named exactly like
the project's Polaris principal role (``teehr-project-<name>-<role>``): it is in the
member's tokens, and the ``polaris-principal-sync`` CronJob binds it to their named
Polaris principal. The control plane also binds that principal role directly
(:mod:`src.access`) so access changes take effect immediately.

Authenticates like the bootstrap Job and the principal-sync CronJob do: the Keycloak
admin user via ``admin-cli`` on the master realm.
"""

import time

import httpx


class KeycloakError(Exception):
    """A Keycloak admin call failed."""


class KeycloakAdmin:
    def __init__(
        self,
        base_url: str,
        realm: str,
        admin_user: str,
        admin_password: str,
        *,
        transport: httpx.BaseTransport | None = None,
        timeout: float = 10.0,
    ):
        self.realm = realm
        self._admin_user = admin_user
        self._admin_password = admin_password
        self._http = httpx.Client(
            base_url=base_url, timeout=timeout, transport=transport
        )
        self._token: str | None = None
        self._token_expires_at = 0.0

    # --- plumbing -----------------------------------------------------------

    def _bearer(self) -> dict[str, str]:
        if self._token is None or time.monotonic() >= self._token_expires_at:
            try:
                resp = self._http.post(
                    "/realms/master/protocol/openid-connect/token",
                    data={
                        "grant_type": "password",
                        "client_id": "admin-cli",
                        "username": self._admin_user,
                        "password": self._admin_password,
                    },
                )
            except httpx.HTTPError as exc:
                raise KeycloakError(f"token request failed: {exc}") from exc
            if resp.status_code != 200:
                raise KeycloakError(f"token request failed: {resp.status_code}")
            body = resp.json()
            self._token = body["access_token"]
            # Admin tokens are short-lived (60s by default); refresh a little early.
            self._token_expires_at = time.monotonic() + max(
                int(body.get("expires_in", 60)) - 10, 5
            )
        return {"Authorization": f"Bearer {self._token}"}

    def _call(
        self, method: str, path: str, ok: tuple[int, ...], **kwargs
    ) -> httpx.Response:
        url = f"/admin/realms/{self.realm}{path}"
        for retry in (True, False):
            try:
                resp = self._http.request(method, url, headers=self._bearer(), **kwargs)
            except httpx.HTTPError as exc:
                raise KeycloakError(f"{method} {path}: {exc}") from exc
            if resp.status_code in ok:
                return resp
            # A revoked or no-longer-valid admin token: get a new one, once.
            if resp.status_code == 401 and retry:
                self._token = None
                continue
            break
        raise KeycloakError(f"{method} {path} -> {resp.status_code}: {resp.text[:300]}")

    # --- realm roles ----------------------------------------------------------

    def ensure_realm_role(self, name: str, description: str = "") -> None:
        self._call(
            "POST",
            "/roles",
            (201, 409),
            json={"name": name, "description": description},
        )

    def delete_realm_role(self, name: str) -> None:
        """Delete a realm role; Keycloak drops it from every user that held it."""
        self._call("DELETE", f"/roles/{name}", (204, 404))

    def _role(self, name: str) -> dict:
        return self._call("GET", f"/roles/{name}", (200,)).json()

    # --- users ----------------------------------------------------------------

    def get_user(self, user_id: str) -> dict | None:
        resp = self._call("GET", f"/users/{user_id}", (200, 404))
        return resp.json() if resp.status_code == 200 else None

    def find_user(self, username: str) -> dict | None:
        users = self._call(
            "GET", "/users", (200,), params={"username": username, "exact": "true"}
        ).json()
        return users[0] if users else None

    def user_realm_roles(self, user_id: str) -> set[str]:
        """Realm roles mapped directly to the user (not via groups or composites)."""
        roles = self._call(
            "GET", f"/users/{user_id}/role-mappings/realm", (200,)
        ).json()
        return {r["name"] for r in roles}

    def add_realm_roles(self, user_id: str, names: set[str]) -> None:
        if names:
            body = [self._role(n) for n in sorted(names)]
            self._call(
                "POST", f"/users/{user_id}/role-mappings/realm", (204,), json=body
            )

    def remove_realm_roles(self, user_id: str, names: set[str]) -> None:
        if names:
            body = [self._role(n) for n in sorted(names)]
            self._call(
                "DELETE", f"/users/{user_id}/role-mappings/realm", (204,), json=body
            )
