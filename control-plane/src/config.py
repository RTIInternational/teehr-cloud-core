"""Control-plane settings, read from the environment."""

import os
from dataclasses import dataclass, field
from functools import lru_cache

from sqlalchemy.engine import URL, make_url


def _csv(value: str | None) -> tuple[str, ...]:
    return tuple(item.strip() for item in (value or "").split(",") if item.strip())


@dataclass(frozen=True)
class Settings:
    database_url: URL
    keycloak_issuer_url: str | None = None
    keycloak_jwks_url: str | None = None
    # A token is accepted when its `aud` or `azp` is in this list.
    allowed_audiences: tuple[str, ...] = ()
    # Realm roles that act as platform admins: full rights on every project.
    admin_roles: tuple[str, ...] = field(default=("admin",))
    # Public Keycloak client the /docs page signs in with. None = no login button.
    docs_client_id: str | None = None
    # Polaris management access for provisioning. None = provisioning disabled.
    polaris_url: str | None = None
    polaris_realm: str = "teehr"
    polaris_catalog: str = "teehr"
    # "client_id:client_secret" (the polaris-secrets/root-credentials format).
    polaris_credentials: str | None = None

    @property
    def oidc_base_url(self) -> str | None:
        """Keycloak's OpenID Connect endpoints, as the browser reaches them."""
        if not self.keycloak_issuer_url:
            return None
        return f"{self.keycloak_issuer_url}/protocol/openid-connect"


def _database_url() -> URL:
    """``CONTROL_PLANE_DB_URL`` wins when set (tests, standalone SQLite).

    Otherwise a deployed service gives the Postgres parts separately so the password
    never has to be URL-escaped by hand. With neither, fall back to a local SQLite file.
    """
    explicit = os.getenv("CONTROL_PLANE_DB_URL")
    if explicit:
        return make_url(explicit)

    host = os.getenv("CONTROL_PLANE_DB_HOST")
    if not host:
        return make_url("sqlite:///./control_plane.db")

    return URL.create(
        "postgresql+psycopg",
        username=os.environ["CONTROL_PLANE_DB_USER"],
        password=os.environ["CONTROL_PLANE_DB_PASSWORD"],
        host=host,
        port=int(os.getenv("CONTROL_PLANE_DB_PORT", "5432")),
        database=os.environ["CONTROL_PLANE_DB_NAME"],
    )


@lru_cache
def get_settings() -> Settings:
    issuer = os.getenv("KEYCLOAK_ISSUER_URL")
    jwks = os.getenv("KEYCLOAK_JWKS_URL") or (
        f"{issuer}/protocol/openid-connect/certs" if issuer else None
    )
    return Settings(
        database_url=_database_url(),
        keycloak_issuer_url=issuer,
        keycloak_jwks_url=jwks,
        allowed_audiences=_csv(os.getenv("KEYCLOAK_ALLOWED_AUDIENCES")),
        admin_roles=_csv(os.getenv("CONTROL_PLANE_ADMIN_ROLES")) or ("admin",),
        docs_client_id=os.getenv("KEYCLOAK_DOCS_CLIENT_ID") or None,
        polaris_url=os.getenv("POLARIS_URL") or None,
        polaris_realm=os.getenv("POLARIS_REALM", "teehr"),
        polaris_catalog=os.getenv("POLARIS_CATALOG")
        or os.getenv("POLARIS_REALM", "teehr"),
        polaris_credentials=os.getenv("POLARIS_ROOT_CREDENTIALS") or None,
    )
