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
  ICECHUNK_STORAGE_MODE  "local" for minio/kind, "remote" for AWS S3 (default: remote)
  CORS_ORIGINS           Comma-separated list of allowed CORS origins
  DATASET_CACHE_TTL      Seconds to cache dataset metadata before re-opening from icechunk
                         (default: 60). Set to 0 to disable caching (re-open on every request).

  Local (ICECHUNK_STORAGE_MODE=local):
    ICECHUNK_ENDPOINT_URL   MinIO endpoint (default: http://minio:9000)
    AWS_DEFAULT_REGION      Region (default: us-east-1)
    AWS_ACCESS_KEY_ID       MinIO access key
    AWS_SECRET_ACCESS_KEY   MinIO secret key

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

import boto3
from botocore.config import Config
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
from .provider import IcechunkDatasetProvider, RepoConfig

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


def build_s3_client():
    mode = os.getenv("ICECHUNK_STORAGE_MODE", "remote")
    if mode == "local":
        return boto3.client(
            "s3",
            endpoint_url=os.getenv("ICECHUNK_ENDPOINT_URL", "http://minio:9000"),
            region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            config=Config(s3={"addressing_style": "path"}),
        )
    return boto3.client("s3")


def normalize_storage_prefix(prefix: str) -> str:
    return prefix.rstrip("/") + "/" if prefix else ""


def list_storage_prefixes(s3, bucket: str, prefix: str) -> list[dict]:
    paginator = s3.get_paginator("list_objects_v2")
    results = []
    for page in paginator.paginate(Bucket=bucket, Prefix=normalize_storage_prefix(prefix), Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            dir_path = cp["Prefix"]
            dir_name = dir_path.rstrip("/").split("/")[-1]
            if dir_name:
                results.append({"id": dir_name, "path": dir_path})
    return results


def list_storage_files(s3, bucket: str, prefix: str, extension: str) -> list[dict]:
    paginator = s3.get_paginator("list_objects_v2")
    results = []
    for page in paginator.paginate(Bucket=bucket, Prefix=normalize_storage_prefix(prefix)):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(extension):
                continue
            filename = os.path.basename(key)
            if extension == ".pmtiles":
                source_layer = filename[: -len(extension)]
                results.append({"id": source_layer, "path": key, "source_layer": source_layer})
            else:
                results.append({"id": filename, "path": key})
    return results


def discover_available_repos() -> list[RepoConfig]:
    """Discover repos by listing top-level prefixes under ICECHUNK_BUCKET/ICECHUNK_PREFIX."""
    bucket = os.getenv("ICECHUNK_BUCKET", "").strip()
    prefix = os.getenv("ICECHUNK_PREFIX", "").strip().rstrip("/")
    if not bucket:
        raise RuntimeError("ICECHUNK_BUCKET is required: S3 bucket name")
    if not prefix:
        raise RuntimeError("ICECHUNK_PREFIX is required: base prefix path for icechunk repos")

    configs = [
        RepoConfig(name=item["id"], bucket=bucket, prefix=item["path"].rstrip("/"))
        for item in list_storage_prefixes(build_s3_client(), bucket, prefix)
    ]

    if not configs:
        raise RuntimeError(f"No icechunk repos found under s3://{bucket}/{prefix}/")
    return configs


def build_storage_kwargs() -> dict:
    """
    Return kwargs for ic.s3_storage() based on ICECHUNK_STORAGE_MODE.

    - "local":  explicit endpoint + credentials via standard AWS_* env vars,
                plus minio-specific flags (allow_http, force_path_style, endpoint_url).
    - "remote": from_env=True — reads AWS_* env vars or uses IRSA on EKS.
    """
    mode = os.getenv("ICECHUNK_STORAGE_MODE", "remote")
    if mode == "local":
        kwargs: dict = {
            "region": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
            "allow_http": True,
            "endpoint_url": os.getenv("ICECHUNK_ENDPOINT_URL", "http://minio:9000"),
            "force_path_style": True,
        }
        access_key = os.getenv("AWS_ACCESS_KEY_ID")
        secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        if access_key:
            kwargs["access_key_id"] = access_key
        if secret_key:
            kwargs["secret_access_key"] = secret_key
        return kwargs
    return {"from_env": True}


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

    repo_configs = discover_available_repos()
    storage_kwargs = build_storage_kwargs()

    logger.info(
        "Storage mode: %s | repos: %s | cache_ttl: %ss",
        storage_mode,
        [(r.name, r.bucket, r.prefix) for r in repo_configs],
        cache_ttl,
    )

    provider = IcechunkDatasetProvider(
        repo_configs=repo_configs,
        storage_kwargs=storage_kwargs,
        branch=branch,
        cache_ttl_seconds=cache_ttl,
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
        # Return only the tiles-capable dataset names (not the _raw_data variants).
        return {"datasets": [cfg.name for cfg in repo_configs]}

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

    @api_app.get("/storage/contents")
    def list_storage_contents(bucket: str, prefix: str, extension: str = None):
        """
        List S3-compatible storage contents.

        Lists files or directories from an S3 bucket at a given prefix.
        Requires Keycloak JWT authentication (inherited from auth middleware).

        Query parameters:
          - bucket: S3 bucket name (required)
          - prefix: Path/prefix within bucket (required)
          - extension: File extension to filter by, e.g. '.pmtiles' (optional)
            If omitted, lists subdirectories instead of files.

        Returns:
          - For files: [{ "id": "filename", "path": "bucket/prefix/filename.ext", "source_layer": "layer_name" }, ...]
            For pmtiles: source_layer derived from filename (without .pmtiles)
          - For directories: [{ "id": "dir-name", "path": "bucket/prefix/dir-name/" }, ...]
        """
        if not bucket or prefix is None:
            raise HTTPException(status_code=400, detail="bucket and prefix parameters are required")

        try:
            s3 = build_s3_client()
            results = (
                list_storage_files(s3, bucket, prefix, extension)
                if extension
                else list_storage_prefixes(s3, bucket, prefix)
            )

            logger.info(
                "Storage contents: bucket=%s, prefix=%s, extension=%s, found %d items",
                bucket,
                prefix,
                extension or "none",
                len(results),
            )
            return {"bucket": bucket, "prefix": prefix, "extension": extension, "items": results}

        except Exception as e:
            logger.error("Storage contents error: %s", str(e))
            raise HTTPException(status_code=500, detail=f"Storage listing failed: {str(e)}")

    # --- api_app middleware (gzip only; CORS is on the outer app) ---

    api_app.add_middleware(GZipMiddleware, minimum_size=1000)

    # --- Outer app ---

    app = FastAPI(title="TEEHR xpublish API", lifespan=app_lifespan)
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

        request.state.identity = await resolve_identity(request)
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
        allow_headers=["Authorization", "Content-Type"],
    )

    @app.get("/health")
    def health():
        return {"status": "ok", "datasets": provider.dataset_ids()}

    return app


app = build_app()
