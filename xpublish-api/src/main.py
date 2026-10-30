"""
xpublish REST service for icechunk gridded data.

Serves raster tiles via TilesPlugin (each repo's data group plus its /overviews levels) and
point queries via CfEdrPlugin (reads from each repo's data group: /data, or /references
when the repo isn't materialized).

Environment variables:
  ICECHUNK_BUCKET        S3 bucket that holds all icechunk repos.
                         Example: "warehouse" (local) or "ciroh-rti-public-data" (remote)
  ICECHUNK_PREFIX        Base prefix path; each repo lives at {prefix}/{name}.
                         Example: "icechunk-ingests"
  ICECHUNK_BRANCH        Branch to open for all repos (default: main)
  ICECHUNK_STORAGE_MODE  "local" for the in-cluster S3, "remote" for AWS S3 (default: remote)
  PMTILES_BUCKET         S3 bucket holding the .pmtiles vector-tile archives.
  PMTILES_PREFIX         Prefix within that bucket. Example: "vector-tiles"
  CORS_ORIGINS           Comma-separated list of allowed CORS origins
  DATASET_CACHE_TTL      Seconds between checks for a new snapshot on each dataset's branch;
                         a dataset is re-opened only when the tip moved (default: 60).
                         0 checks on every request.
  REPO_DISCOVERY_TTL     Seconds before re-listing {prefix} for new/removed repos
                         (default: DATASET_CACHE_TTL). Repos are discovered lazily on the
                         first request, so the app starts even with none present.

  Local (ICECHUNK_STORAGE_MODE=local):
    ICECHUNK_ENDPOINT_URL   In-cluster S3 endpoint (required)
    AWS_DEFAULT_REGION      Region (default: us-east-1)
    AWS_ACCESS_KEY_ID       Local S3 access key
    AWS_SECRET_ACCESS_KEY   Local S3 secret key

  Remote (ICECHUNK_STORAGE_MODE=remote):
    AWS_*                   Standard AWS credential env vars or IRSA

  Keycloak (JWT auth):
    KEYCLOAK_ISSUER_URL       Keycloak realm URL (external)
    KEYCLOAK_JWKS_URL         JWKS endpoint override (use internal cluster URL)
    KEYCLOAK_ALLOWED_AUDIENCES  Comma-separated accepted aud/azp values
"""

import logging
import os
import time
import uuid
from contextlib import asynccontextmanager

import numpy as np
import xpublish
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from starlette.middleware.cors import CORSMiddleware
from xpublish_edr import CfEdrPlugin
from xpublish_tiles import lib as xpublish_tiles_lib
from xpublish_tiles.xpublish.tiles import TilesPlugin

from .auth import KeycloakJWTValidator, resolve_identity
from .authz import is_permitted, is_request_permitted, parse_access_rules, unmatched
from .config import config
from .credentials import (
    CREDENTIALS_TTL_SECONDS,
    IcechunkCredentialsResponse,
    PolicyTooLargeError,
    S3SessionCredentials,
    build_session_policy,
    policy_for,
    session_name,
    vend_credentials,
)
from .pmtiles import list_pmtiles_layers, read_pmtiles_range, resolve_pmtiles_location
from .provider import IcechunkDatasetProvider, written_coord_values
from .storage import build_storage_kwargs, resolve_icechunk_location

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


@asynccontextmanager
async def app_lifespan(app: FastAPI):
    app.state.jwt_validator = KeycloakJWTValidator()
    try:
        yield
    finally:
        # Clean up xpublish-tiles module-level executor to avoid leaked semaphores.
        xpublish_tiles_lib.EXECUTOR.shutdown(wait=False, cancel_futures=True)
        xpublish_tiles_lib._semaphores.clear()
        xpublish_tiles_lib._data_load_semaphores.clear()


def build_app() -> FastAPI:
    branch = os.getenv("ICECHUNK_BRANCH", "main")
    cors_origins = [o.strip() for o in os.getenv("CORS_ORIGINS", "*").split(",")]
    storage_mode = os.getenv("ICECHUNK_STORAGE_MODE", "remote")
    cache_ttl = float(os.getenv("DATASET_CACHE_TTL", "60"))
    discovery_ttl = float(os.getenv("REPO_DISCOVERY_TTL", str(cache_ttl)))
    # Invalid rules fail startup rather than silently changing who can read a repo
    access_rules = parse_access_rules(config.TEEHR_ACCESS_RULES)
    logger.info(
        "Access rules: default=%s | restricted repos: %s | restricted layers: %s",
        list(access_rules.default) if access_rules.default is not None else "none (open to logged-in users)",
        sorted(access_rules.items["repos"]),
        sorted(access_rules.items["layers"]),
    )
    # STS on without a role to assume is a misconfiguration; fail now rather than on the first notebook
    if not config.ICECHUNK_STS_UNAVAILABLE and not config.ICECHUNK_ACCESS_ROLE_ARN:
        raise RuntimeError("ICECHUNK_ACCESS_ROLE_ARN is required when ICECHUNK_STS_UNAVAILABLE is false")

    # Only the configuration is resolved here — repos are discovered lazily by
    # the provider, so the app starts before storage is reachable or populated.
    bucket, prefix = resolve_icechunk_location()
    pmtiles_bucket, pmtiles_prefix = resolve_pmtiles_location()

    logger.info(
        "Storage mode: %s | repos: s3://%s/%s/ | tiles: s3://%s/%s/ | cache_ttl: %ss | discovery_ttl: %ss",
        storage_mode,
        bucket,
        prefix,
        pmtiles_bucket,
        pmtiles_prefix,
        cache_ttl,
        discovery_ttl,
    )

    def warn_unmatched_repo_rules(repo_names: list[str]) -> None:
        # A misspelled restriction matches nothing and leaves the real repo readable by default
        missing = unmatched(access_rules, "repos", repo_names)
        if missing:
            logger.warning("Access rules name icechunk repo(s) that don't exist: %s", missing)

    provider = IcechunkDatasetProvider(
        bucket=bucket,
        prefix=prefix,
        storage_kwargs=build_storage_kwargs(),
        branch=branch,
        cache_ttl_seconds=cache_ttl,
        discovery_ttl_seconds=discovery_ttl,
        on_repos_changed=warn_unmatched_repo_rules,
    )

    rest = xpublish.Rest(
        datasets={},
        plugins={
            "icechunk-provider": provider,
            "tiles": TilesPlugin(),
            "edr": CfEdrPlugin(),
        },
    )

    api_app = rest.app

    # --- Notebook credentials (icechunk_access.py in the JupyterHub image) ---

    @api_app.post("/auth/icechunk-credentials", response_model=IcechunkCredentialsResponse)
    def icechunk_credentials(request: Request):
        """Short-lived, read-only S3 credentials for the Icechunk repos the user may read."""
        identity = request.state.identity
        trace_id = uuid.uuid4().hex
        repos = [name for name in provider.repo_names() if is_permitted(access_rules, identity, "repos", name)]
        response = {"bucket": bucket, "prefix": prefix, "repos": repos, "trace_id": trace_id}
        no_credentials = {"credentials": None, "expires_at_epoch_seconds": None, "expires_in_seconds": None}

        # Local S3 has no STS: the notebook uses its own keys and refuses repos not in `repos`
        if config.ICECHUNK_STS_UNAVAILABLE:
            return IcechunkCredentialsResponse(**no_credentials, **response)
        deny_mode, policy_repos = policy_for(access_rules, identity)
        if not deny_mode and not policy_repos:
            logger.info(
                "No readable icechunk repos for %s; no credentials | trace_id=%s", session_name(identity), trace_id
            )
            return IcechunkCredentialsResponse(**no_credentials, **response)

        policy = build_session_policy(bucket, prefix, deny_mode, policy_repos)
        try:
            sts_credentials = vend_credentials(config.ICECHUNK_ACCESS_ROLE_ARN, identity, policy)
        except PolicyTooLargeError as exc:
            logger.error("%s | trace_id=%s", exc, trace_id)
            raise HTTPException(status_code=500, detail=f"{exc} (trace_id {trace_id})") from exc
        except (BotoCoreError, ClientError) as exc:
            logger.error("STS AssumeRole failed: %s | trace_id=%s", type(exc).__name__, trace_id)
            raise HTTPException(status_code=502, detail=f"Could not create credentials (trace_id {trace_id})") from exc

        expires_at = int(sts_credentials["Expiration"].timestamp())
        # Log ids and counts only, never the credentials or the policy's keys
        logger.info(
            "Vended icechunk credentials | user=%s ttl=%ss mode=%s policy_repos=%d | trace_id=%s",
            session_name(identity), CREDENTIALS_TTL_SECONDS, "deny" if deny_mode else "allow",
            len(policy_repos), trace_id,
        )
        return IcechunkCredentialsResponse(
            credentials=S3SessionCredentials(
                access_key_id=sts_credentials["AccessKeyId"],
                secret_access_key=sts_credentials["SecretAccessKey"],
                session_token=sts_credentials["SessionToken"],
            ),
            expires_at_epoch_seconds=expires_at,
            expires_in_seconds=max(expires_at - int(time.time()), 0),
            **response,
        )

    # --- Custom discovery endpoints consumed by the frontend ---

    @api_app.get("/dataset-keys")
    def list_dataset_keys(request: Request):
        # Only the tiles-capable dataset names (not the _data variants) the user may read.
        # Read through the provider rather than a startup snapshot so repos
        # created after the pod started show up on the next page load.
        identity = request.state.identity
        return {
            "datasets": [
                name for name in provider.repo_names()
                if is_permitted(access_rules, identity, "repos", name)
            ]
        }

    @api_app.get("/dataset-variables/{dataset_id}")
    def dataset_variables(dataset_id: str):
        # The overviews hold the same variables as the data group
        data_dt = provider.get_datatree_for_dataset(f"{dataset_id}_data")
        if data_dt is None:
            raise HTTPException(status_code=404, detail=f"Unknown dataset '{dataset_id}'")
        variables = list(data_dt.dataset.data_vars.keys())
        logger.info("Variables for dataset '%s': %s", dataset_id, variables)
        return {"dataset_id": dataset_id, "variables": variables}

    @api_app.get("/datasets/{dataset_id}/coords/{coord_name}")
    def dataset_coord_values(dataset_id: str, coord_name: str):
        # Coords (including time) come from the repo's data group.
        data_dt = provider.get_datatree_for_dataset(f"{dataset_id}_data")
        if data_dt is None:
            raise HTTPException(status_code=404, detail=f"Unknown dataset '{dataset_id}'")
        ds = data_dt.dataset
        if coord_name not in ds.coords:
            raise HTTPException(status_code=404, detail=f"Coordinate '{coord_name}' not found")
        # Time-grid slots not yet written have no data to show
        values = written_coord_values(ds, coord_name)
        if values.ndim != 1:
            values = values.ravel()
        serialized = [
            np.datetime_as_string(v, unit="s") if isinstance(v, np.datetime64) else str(v)
            for v in values
        ]
        logger.debug("Coordinate values for dataset '%s', coord '%s': %s", dataset_id, coord_name, serialized)
        return {"dataset_id": dataset_id, "coord_name": coord_name, "values": serialized}

    @api_app.get("/datasets/{dataset_id}/variable-attrs")
    def dataset_variable_attrs(dataset_id: str):
        data_dt = provider.get_datatree_for_dataset(f"{dataset_id}_data")
        if data_dt is None:
            raise HTTPException(status_code=404, detail=f"Unknown dataset '{dataset_id}'")
        ds = data_dt.dataset
        result = {}
        for var_name in ds.data_vars:
            attrs = {}
            for k, v in ds[var_name].attrs.items():
                if isinstance(v, np.ndarray):
                    attrs[k] = v.tolist()
                elif isinstance(v, (np.integer, np.floating)):
                    attrs[k] = v.item()
                else:
                    attrs[k] = v
            result[var_name] = attrs
        logger.info("Variable attrs for dataset '%s': %s", dataset_id, list(result.keys()))
        return {"dataset_id": dataset_id, "variables": result}

    unmatched_layer_rules: list[str] = []

    @api_app.get("/vector-tiles")
    def list_vector_tiles(request: Request):
        """
        List the .pmtiles archives under the configured prefix that the user may read.

        Returns ``{"items": [{"id": "usgs-basins", "source_layer": "usgs-basins"}]}``.
        Fetch an archive's bytes from ``/api/vector-tiles/{id}.pmtiles``.
        """
        try:
            items = list_pmtiles_layers()
        except Exception as exc:
            logger.error("Vector tile listing failed: %s", exc)
            raise HTTPException(status_code=500, detail="Vector tile listing failed") from exc

        layer_ids = [item["id"] for item in items]
        logger.info("Vector tile layers: %s", layer_ids)
        # Warn only when the set changes; a misspelled restriction leaves the real layer readable by default
        missing = unmatched(access_rules, "layers", layer_ids)
        if missing != unmatched_layer_rules:
            unmatched_layer_rules[:] = missing
            if missing:
                logger.warning("Access rules name vector-tile layer(s) that don't exist: %s", missing)
        identity = request.state.identity
        return {"items": [item for item in items if is_permitted(access_rules, identity, "layers", item["id"])]}

    # --- api_app middleware (gzip, tile caching; CORS is on the outer app) ---

    api_app.add_middleware(GZipMiddleware, minimum_size=1000)

    # A tile URL pins dataset, date and style, so the browser can reuse it briefly
    @api_app.middleware("http")
    async def tile_cache_headers(request: Request, call_next):
        response = await call_next(request)
        if response.status_code == 200 and "/tiles/" in request.url.path:
            response.headers.setdefault("Cache-Control", "private, max-age=300")
        return response

    # --- Outer app ---

    app = FastAPI(title="TEEHR xpublish API", lifespan=app_lifespan)

    # On the outer app, not api_app: GZipMiddleware would compress this 206
    # body while Content-Range still described the uncompressed bytes.  Auth
    # and CORS live out here too, so the Keycloak gate still applies.
    #
    # Must stay ABOVE the mount below: Mount matches on prefix alone, so a
    # mount registered first swallows this path and answers 404.
    @app.get("/api/vector-tiles/{layer}.pmtiles")
    def get_vector_tile_archive(layer: str, request: Request):
        return read_pmtiles_range(layer, request.headers.get("range"))

    app.mount("/api", api_app)

    # Auth middleware is registered first so it ends up innermost.
    # CORSMiddleware is added second so it ends up outermost — this ensures
    # that ALL responses (including 401s from auth) pass through CORSMiddleware
    # and receive the correct Access-Control-Allow-Origin header.
    @app.middleware("http")
    async def auth_middleware(request: Request, call_next):
        if request.method == "OPTIONS":
            return await call_next(request)

        path = request.url.path
        if path == "/health":
            return await call_next(request)

        # resolve_identity raises on bad tokens and JWKS failures. An exception
        # escaping this middleware bypasses CORSMiddleware, so the browser sees
        # an opaque CORS error instead of the 401 or 503.
        try:
            request.state.identity = await resolve_identity(request)
        except HTTPException as exc:
            return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

        if not request.state.identity.is_authenticated:
            return JSONResponse(
                status_code=401,
                content={"detail": "Authentication required"},
            )

        # Access rules for every dataset and vector-tile request; unrecognised dataset paths are denied
        if not is_request_permitted(access_rules, request.state.identity, path):
            return JSONResponse(status_code=403, content={"detail": "Forbidden"})

        return await call_next(request)

    if "*" in cors_origins:
        cors_origins = ["*"]
        allow_credentials = False
    else:
        allow_credentials = True

    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_credentials=allow_credentials,
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type", "Range"],
        # pmtiles reads these off the response to walk the archive; without
        # them exposed the range requests succeed but the browser hides the
        # headers from JS and the archive fails to parse.
        expose_headers=["Content-Range", "Content-Length", "ETag", "Accept-Ranges"],
    )

    # Unauthenticated (probes and the Docker HEALTHCHECK only check the status code), so no dataset names
    @app.get("/health")
    def health():
        return {"status": "ok"}

    return app


app = build_app()
