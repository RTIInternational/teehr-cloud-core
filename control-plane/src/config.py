"""Control-plane settings, read from the environment."""

import os
from dataclasses import dataclass, field
from functools import lru_cache

from sqlalchemy.engine import URL, make_url


def _csv(value: str | None) -> tuple[str, ...]:
    return tuple(item.strip() for item in (value or "").split(",") if item.strip())


def _node_selector(value: str | None) -> tuple[tuple[str, str], ...]:
    """``key=value,...`` (as ``kubectl label``) as pairs."""
    pairs = []
    for item in _csv(value):
        key, sep, val = item.partition("=")
        if not sep or not key.strip():
            raise ValueError(f"JOB_NODE_SELECTOR: expected key=value, got {item!r}")
        pairs.append((key.strip(), val.strip()))
    return tuple(pairs)


def _tolerations(value: str | None) -> tuple[tuple[str, str | None, str | None], ...]:
    """``key[=value][:Effect],...`` (as ``kubectl taint``) as (key, value, effect):
    without a value, any value of the key; without an effect, every effect."""
    out = []
    for item in _csv(value):
        rest, _, effect = item.partition(":")
        key, sep, val = rest.partition("=")
        if not key.strip():
            raise ValueError(
                f"JOB_TOLERATIONS: expected key[=value][:Effect], got {item!r}"
            )
        out.append((key.strip(), val.strip() if sep else None, effect.strip() or None))
    return tuple(out)


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
    # A separate confidential client used only for token exchange (load jobs act as
    # their caller, ADR-0014), so the admin identity above and the "act as users"
    # identity aren't one secret. None = exchange with client_id / client_secret.
    exchange_client_id: str | None = None
    exchange_client_secret: str | None = None
    # Who may create projects: realm roles (any of them). Empty = any signed-in
    # user. A dark launch sets the platform admin role.
    project_creator_roles: tuple[str, ...] = ()
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
    # Largest request body a write through the API accepts; bigger loads go through
    # staged upload and a load job (Phase 3).
    # Untyped (`table`) datasets are a test path until typed kinds exist (data
    # model v1): only platform admins may register them unless this is on.
    untyped_datasets_for_members: bool = False
    max_write_bytes: int = 64 * 1024 * 1024
    # Reads through the API: rows per page by default, and the most a page may
    # hold per format (JSON is far heavier than Arrow/Parquet). Bulk reads belong
    # in jobs (Phase 5).
    page_size: int = 1000
    max_page_rows_json: int = 10_000
    max_page_rows_binary: int = 1_000_000
    # Polaris principal roles given access to every system project (public,
    # platform): read for every user, write for the platform's writers (ingest).
    system_reader_roles: tuple[str, ...] = ()
    system_writer_roles: tuple[str, ...] = ()
    # Keycloak admin API, for the realm roles that carry project access.
    # None = membership isn't turned into data access.
    keycloak_admin_url: str | None = None
    keycloak_realm: str = "teehr"
    # Upload staging (Phase 3): a bucket only the control plane reaches; files are
    # PUT there with a presigned URL. None = uploads disabled.
    staging_bucket: str | None = None
    # S3 endpoint the control plane uses (None: AWS), and the one upload URLs are
    # signed for, when clients reach storage at another address (local KinD).
    staging_endpoint: str | None = None
    staging_public_endpoint: str | None = None
    # Static keys (local); None: the default chain (IRSA on AWS).
    staging_access_key: str | None = None
    staging_secret_key: str | None = None
    upload_url_ttl_seconds: int = 3600
    max_upload_bytes: int = 5 * 1024**3
    # Jobs the API starts (ADR-0014): the image their pods run (the control
    # plane's own), where, with what memory, and how the pod reaches the control
    # plane. None = jobs disabled.
    job_image: str | None = None
    job_namespace: str | None = None
    job_memory_request: str = "1Gi"
    job_memory_limit: str = "4Gi"
    # CPU the job pod requests (e.g. "2"); None: none (it gets what's free).
    job_cpu_request: str | None = None
    # Where job pods run: node labels they need (JOB_NODE_SELECTOR) and taints
    # they tolerate (JOB_TOLERATIONS), e.g. a worker node group, so runs don't
    # share nodes with the platform's services. Empty: any untainted node.
    job_node_selector: tuple[tuple[str, str], ...] = ()
    job_tolerations: tuple[tuple[str, str | None, str | None], ...] = ()
    # DuckDB threads for a job's reads (S3 requests run on them); None: DuckDB's
    # default (the cores it sees).
    job_threads: int | None = None
    # Predicted rows a run's shard holds; None = sized from the job's memory.
    run_rows_per_shard: int | None = None
    job_ttl_seconds: int = 86_400
    # A run shard whose pod hasn't sent a heartbeat for this long is claimed again
    # (longer than a control-plane restart plus the job client's retries).
    shard_heartbeat_lapse_seconds: int = 300
    # Pods a run may spread its shards over (ADR-0017; 1: one pod, as before), and
    # the shards each pod beyond the first should have to be worth starting.
    run_max_pods: int = 1
    run_shards_per_pod: int = 8
    # Jobs one project may have queued or running at once.
    max_active_jobs_per_project: int = 2
    internal_url: str = "http://teehr-control-plane:8000"
    # Encrypts the refresh tokens jobs act as their callers with (any string; a
    # key is derived from it).
    job_token_key: str | None = None
    # Keycloak's token endpoint, as the control plane reaches it (token exchange).
    keycloak_token_url: str | None = None

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
        exchange_client_id=os.getenv("CONTROL_PLANE_EXCHANGE_CLIENT_ID") or None,
        exchange_client_secret=os.getenv("CONTROL_PLANE_EXCHANGE_CLIENT_SECRET")
        or None,
        project_creator_roles=_csv(os.getenv("PROJECT_CREATOR_ROLES")),
        polaris_token_url=os.getenv("POLARIS_TOKEN_URL") or None,
        polaris_warehouse=os.getenv("POLARIS_WAREHOUSE") or None,
        storage_endpoint=os.getenv("STORAGE_S3_ENDPOINT") or None,
        storage_region=os.getenv("STORAGE_S3_REGION") or "us-east-1",
        storage_path_style=os.getenv("STORAGE_S3_PATH_STYLE", "").lower() == "true",
        untyped_datasets_for_members=os.getenv(
            "UNTYPED_DATASETS_FOR_MEMBERS", ""
        ).lower()
        == "true",
        max_write_bytes=int(os.getenv("MAX_WRITE_BYTES") or 64 * 1024 * 1024),
        page_size=int(os.getenv("PAGE_SIZE") or 1000),
        max_page_rows_json=int(os.getenv("MAX_PAGE_ROWS_JSON") or 10_000),
        max_page_rows_binary=int(os.getenv("MAX_PAGE_ROWS_BINARY") or 1_000_000),
        system_reader_roles=_csv(os.getenv("SYSTEM_PROJECT_READER_ROLES")),
        system_writer_roles=_csv(os.getenv("SYSTEM_PROJECT_WRITER_ROLES")),
        keycloak_admin_url=os.getenv("KEYCLOAK_ADMIN_URL") or None,
        keycloak_realm=os.getenv("KEYCLOAK_REALM", "teehr"),
        staging_bucket=os.getenv("STAGING_BUCKET") or None,
        staging_endpoint=os.getenv("STAGING_S3_ENDPOINT") or None,
        staging_public_endpoint=os.getenv("STAGING_S3_PUBLIC_ENDPOINT") or None,
        staging_access_key=os.getenv("STAGING_S3_ACCESS_KEY") or None,
        staging_secret_key=os.getenv("STAGING_S3_SECRET_KEY") or None,
        upload_url_ttl_seconds=int(os.getenv("UPLOAD_URL_TTL_SECONDS") or 3600),
        max_upload_bytes=int(os.getenv("MAX_UPLOAD_BYTES") or 5 * 1024**3),
        job_image=os.getenv("JOB_IMAGE") or None,
        job_namespace=os.getenv("JOB_NAMESPACE") or None,
        job_memory_request=os.getenv("JOB_MEMORY_REQUEST") or "1Gi",
        job_memory_limit=os.getenv("JOB_MEMORY_LIMIT") or "4Gi",
        job_cpu_request=os.getenv("JOB_CPU_REQUEST") or None,
        job_node_selector=_node_selector(os.getenv("JOB_NODE_SELECTOR")),
        job_tolerations=_tolerations(os.getenv("JOB_TOLERATIONS")),
        job_threads=int(os.getenv("JOB_THREADS") or 0) or None,
        run_rows_per_shard=int(os.getenv("RUN_ROWS_PER_SHARD") or 0) or None,
        job_ttl_seconds=int(os.getenv("JOB_TTL_SECONDS") or 86_400),
        run_max_pods=int(os.getenv("JOB_RUN_MAX_PODS") or 1),
        run_shards_per_pod=int(os.getenv("RUN_SHARDS_PER_POD") or 8),
        shard_heartbeat_lapse_seconds=int(
            os.getenv("SHARD_HEARTBEAT_LAPSE_SECONDS") or 300
        ),
        max_active_jobs_per_project=int(os.getenv("MAX_ACTIVE_JOBS_PER_PROJECT") or 2),
        internal_url=os.getenv("CONTROL_PLANE_INTERNAL_URL")
        or "http://teehr-control-plane:8000",
        job_token_key=os.getenv("JOB_TOKEN_KEY") or None,
        keycloak_token_url=os.getenv("KEYCLOAK_TOKEN_URL")
        or os.getenv("POLARIS_TOKEN_URL")
        or None,
    )
