"""
xpublish REST service for icechunk gridded data.

Serves raster tiles via TilesPlugin (reads from /pyramids group) and
point queries via CfEdrPlugin (reads from /raw_data group).

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
  DATASET_CACHE_TTL      Seconds to cache dataset metadata before re-opening from icechunk
                         (default: 60). Set to 0 to disable caching (re-open on every request).
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
from contextlib import asynccontextmanager

import numpy as np
import xpublish
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from starlette.middleware.cors import CORSMiddleware
from xpublish_edr import CfEdrPlugin
from xpublish_tiles import lib as xpublish_tiles_lib
from xpublish_tiles.xpublish.tiles import TilesPlugin

from .auth import KeycloakJWTValidator, resolve_identity
from .pmtiles import list_pmtiles_layers, read_pmtiles_range, resolve_pmtiles_location
from .provider import IcechunkDatasetProvider
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

    provider = IcechunkDatasetProvider(
        bucket=bucket,
        prefix=prefix,
        storage_kwargs=build_storage_kwargs(),
        branch=branch,
        cache_ttl_seconds=cache_ttl,
        discovery_ttl_seconds=discovery_ttl,
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

    # --- Custom discovery endpoints consumed by the frontend ---

    @api_app.get("/dataset-keys")
    def list_dataset_keys():
        # Only the tiles-capable dataset names (not the _raw_data variants).
        # Read through the provider rather than a startup snapshot so repos
        # created after the pod started show up on the next page load.
        return {"datasets": provider.repo_names()}

    @api_app.get("/dataset-variables/{dataset_id}")
    def dataset_variables(dataset_id: str):
        pyramid_dt = provider.get_datatree_for_dataset(dataset_id)
        if pyramid_dt is None:
            raise HTTPException(status_code=404, detail=f"Unknown dataset '{dataset_id}'")
        children = list(pyramid_dt.children.keys())
        if children:
            variables = list(pyramid_dt[children[0]].data_vars.keys())
            logger.info("Variables for dataset '%s': %s", dataset_id, variables)
            return {"dataset_id": dataset_id, "variables": variables}
        # Pyramid has no children yet (empty repo) — fall back to raw_data variables.
        raw_dt = provider.get_datatree_for_dataset(f"{dataset_id}_raw_data")
        if raw_dt is None:
            raise HTTPException(status_code=404, detail=f"Unknown dataset '{dataset_id}'")
        return {"dataset_id": dataset_id, "variables": list(raw_dt.dataset.data_vars.keys())}

    @api_app.get("/datasets/{dataset_id}/coords/{coord_name}")
    def dataset_coord_values(dataset_id: str, coord_name: str):
        # Coords (including time) live in the /raw_data group, not /pyramids.
        raw_dt = provider.get_datatree_for_dataset(f"{dataset_id}_raw_data")
        if raw_dt is None:
            raise HTTPException(status_code=404, detail=f"Unknown dataset '{dataset_id}'")
        ds = raw_dt.dataset
        if coord_name not in ds.coords:
            raise HTTPException(status_code=404, detail=f"Coordinate '{coord_name}' not found")
        values = ds.coords[coord_name].values
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
        raw_dt = provider.get_datatree_for_dataset(f"{dataset_id}_raw_data")
        if raw_dt is None:
            raise HTTPException(status_code=404, detail=f"Unknown dataset '{dataset_id}'")
        ds = raw_dt.dataset
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

    @api_app.get("/vector-tiles")
    def list_vector_tiles():
        """
        List the .pmtiles archives available under the configured prefix.

        Returns ``{"items": [{"id": "usgs-basins", "source_layer": "usgs-basins"}]}``.
        Fetch an archive's bytes from ``/api/vector-tiles/{id}.pmtiles``.
        """
        try:
            items = list_pmtiles_layers()
        except Exception as exc:
            logger.error("Vector tile listing failed: %s", exc)
            raise HTTPException(status_code=500, detail="Vector tile listing failed") from exc

        logger.info("Vector tile layers: %s", [item["id"] for item in items])
        return {"items": items}

    # --- api_app middleware (gzip only; CORS is on the outer app) ---

    api_app.add_middleware(GZipMiddleware, minimum_size=1000)

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

    @app.get("/health")
    def health():
        return {"status": "ok", "datasets": provider.dataset_ids()}

    return app


app = build_app()
