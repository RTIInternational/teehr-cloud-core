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
    # Realm roles that act as platform admins: full rights on every project. Keep
    # this to very few people.
    admin_roles: tuple[str, ...] = field(default=("control-plane-admin",))
    # Realm roles that can see every project and its members, but change nothing.
    viewer_roles: tuple[str, ...] = ()
    # Public Keycloak client the /docs page signs in with. None = no login button.
    docs_client_id: str | None = None
    # The control plane's own identity: a confidential Keycloak client in the realm
    # (service account). Its token is used for both the Keycloak admin API and
    # Polaris, where it maps to the control plane's named principal.
    client_id: str | None = None
    client_secret: str | None = None
    # Polaris management access for provisioning. None = provisioning disabled.
    polaris_url: str | None = None
    polaris_realm: str = "teehr"
    polaris_catalog: str = "teehr"
    # Where the Polaris token comes from: the Keycloak token endpoint. None =
    # Polaris's own token endpoint (tests, standalone).
    polaris_token_url: str | None = None
    # Catalog warehouse root; each project's namespace is pinned to <root>/<name>/.
    polaris_warehouse: str | None = None
    # Object storage, as Polaris's vended credentials reach it. Set for non-AWS S3
    # (RustFS on local); on AWS the defaults are right.
    storage_endpoint: str | None = None
    storage_region: str = "us-east-1"
    storage_path_style: bool = False
    # Polaris principal roles given access to every system project (public,
    # platform): read for every user, write for the platform's writers (ingest).
    system_reader_roles: tuple[str, ...] = ()
    system_writer_roles: tuple[str, ...] = ()
    # Keycloak admin API, for the realm roles that carry project access.
    # None = membership isn't turned into data access.
    keycloak_admin_url: str | None = None
    keycloak_realm: str = "teehr"

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
        admin_roles=_csv(os.getenv("CONTROL_PLANE_ADMIN_ROLES"))
        or ("control-plane-admin",),
        viewer_roles=_csv(os.getenv("CONTROL_PLANE_VIEWER_ROLES")),
        docs_client_id=os.getenv("KEYCLOAK_DOCS_CLIENT_ID") or None,
        polaris_url=os.getenv("POLARIS_URL") or None,
        polaris_realm=os.getenv("POLARIS_REALM", "teehr"),
        polaris_catalog=os.getenv("POLARIS_CATALOG")
        or os.getenv("POLARIS_REALM", "teehr"),
        client_id=os.getenv("CONTROL_PLANE_CLIENT_ID") or None,
        client_secret=os.getenv("CONTROL_PLANE_CLIENT_SECRET") or None,
        polaris_token_url=os.getenv("POLARIS_TOKEN_URL") or None,
        polaris_warehouse=os.getenv("POLARIS_WAREHOUSE") or None,
        storage_endpoint=os.getenv("STORAGE_S3_ENDPOINT") or None,
        storage_region=os.getenv("STORAGE_S3_REGION") or "us-east-1",
        storage_path_style=os.getenv("STORAGE_S3_PATH_STYLE", "").lower() == "true",
        system_reader_roles=_csv(os.getenv("SYSTEM_PROJECT_READER_ROLES")),
        system_writer_roles=_csv(os.getenv("SYSTEM_PROJECT_WRITER_ROLES")),
        keycloak_admin_url=os.getenv("KEYCLOAK_ADMIN_URL") or None,
        keycloak_realm=os.getenv("KEYCLOAK_REALM", "teehr"),
    )
