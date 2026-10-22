"""Open the Icechunk repos you may read, from a TEEHR JupyterHub notebook.

    from icechunk_access import list_icechunk_repos, open_icechunk_dataset

    list_icechunk_repos()                       # repos you may read
    ds = open_icechunk_dataset("ua_swann_4km")  # its data as an xarray Dataset

xpublish-api decides which repos you may read (see teehr-cloud-core/docs/icechunk-access-control.md).
On the remote platform it also issues short-lived, read-only S3 credentials limited to those repos,
which Icechunk renews automatically while you read. Locally there's no STS: the notebook's own S3
keys are used, and repos you may not read are refused here.
"""
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import icechunk as ic
import requests
import xarray as xr
import zarr

try:
    from teehr.utils.keycloak_auth import refresh_user_token_from_env
except ImportError as exc:  # older teehr kernels (stable/previous)
    raise ImportError(
        "icechunk_access needs teehr with teehr.utils.keycloak_auth; use the dev kernel"
    ) from exc

logger = logging.getLogger(__name__)

XPUBLISH_API_URL = os.getenv("XPUBLISH_API_URL", "http://xpublish-api:8000").rstrip("/")
# "true" only where xpublish-api can issue STS credentials (remote)
USE_STS = os.getenv("ICECHUNK_USE_STS", "false").strip().lower() == "true"
# Renew a minute early, so a read never starts with credentials about to expire
EXPIRY_MARGIN = timedelta(seconds=60)
# Root-group attribute naming the repo's data group; must match xpublish-api's provider
DATA_GROUP_ATTR = "data_group"
DEFAULT_DATA_GROUP = "/raw_data"
SESSION_EXPIRED = (
    "Your Keycloak session has expired; restart your server from the hub control panel "
    "(File > Hub Control Panel > Stop My Server, then Start My Server)"
)


class SessionExpiredError(RuntimeError):
    """The user's Keycloak session can no longer be refreshed."""


def _bearer_token() -> str:
    try:
        token, _ = refresh_user_token_from_env()
    except (RuntimeError, requests.RequestException) as exc:
        raise SessionExpiredError(SESSION_EXPIRED) from exc
    return token


def _xpublish(method: str, path: str) -> dict:
    response = requests.request(
        method,
        f"{XPUBLISH_API_URL}{path}",
        headers={"Authorization": f"Bearer {_bearer_token()}"},
        timeout=30,
    )
    if response.status_code == 401:
        raise SessionExpiredError(SESSION_EXPIRED)
    response.raise_for_status()
    return response.json()


def _request_credentials() -> dict:
    """The credentials route's response: bucket, prefix, readable repos and (remote) S3 credentials."""
    body = _xpublish("POST", "/api/auth/icechunk-credentials")
    if body["credentials"] is None:
        logger.info("icechunk credentials: none issued (repo list only) | trace_id=%s", body["trace_id"])
    else:
        logger.info(
            "icechunk credentials: expire in %ss | trace_id=%s", body["expires_in_seconds"], body["trace_id"]
        )
    return body


class IcechunkCredentials:
    """Icechunk's credential callback: fetches new read-only S3 credentials when the current ones expire.

    Starts from the response the repo was opened with, so each open makes one STS call.
    Plain attributes only, because Icechunk pickles it.
    """

    def __init__(self, initial: dict):
        self.initial = initial

    def __call__(self) -> ic.S3StaticCredentials:
        body = self.initial
        expires_at = body["expires_at_epoch_seconds"]
        if expires_at is None or expires_at - time.time() <= EXPIRY_MARGIN.total_seconds():
            body = _request_credentials()
        credentials = body["credentials"]
        if credentials is None:
            raise PermissionError("xpublish-api issued no credentials: you may not read any Icechunk repo")
        expires_at = datetime.fromtimestamp(body["expires_at_epoch_seconds"], tz=timezone.utc)
        return ic.S3StaticCredentials(
            access_key_id=credentials["access_key_id"],
            secret_access_key=credentials["secret_access_key"],
            session_token=credentials["session_token"],
            expires_after=expires_at - EXPIRY_MARGIN,
        )


def list_icechunk_repos() -> list[str]:
    """The Icechunk repos you may read."""
    return _xpublish("GET", "/api/dataset-keys")["datasets"]


def _anonymous_credentials(url_prefix: str):
    # Copied from xpublish-api's provider; virtual references point at public source buckets
    scheme = url_prefix.split("://", 1)[0]
    if scheme in ("http", "https"):
        return ic.credentials.HttpAccess
    if scheme in ("gs", "gcs"):
        return ic.Credentials.Gcs(ic.credentials.gcs_credentials(anonymous=True))
    if scheme == "s3":
        return ic.Credentials.S3(ic.credentials.s3_credentials(anonymous=True))
    return None


def _storage(body: dict, prefix: str) -> ic.storage.Storage:
    bucket = body["bucket"]
    region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
    if USE_STS:
        return ic.s3_storage(bucket=bucket, prefix=prefix, region=region, get_credentials=IcechunkCredentials(body))
    # Local S3: the notebook's own keys, and the in-cluster endpoint, as the ingest flows do
    kwargs = {"from_env": True, "region": region}
    endpoint = os.getenv("REMOTE_CATALOG_S3_ENDPOINT")
    if endpoint:
        kwargs.update(endpoint_url=endpoint, allow_http=True, force_path_style=True)
    return ic.s3_storage(bucket=bucket, prefix=prefix, **kwargs)


def open_icechunk_repo(name: str) -> ic.Repository:
    """Open a repo you may read; raises PermissionError otherwise.

    Virtual references (data still in the source files) are read anonymously. Repos whose sources
    need credentials (e.g. iSnobal) can only be read from their materialized data.
    """
    body = _request_credentials()
    if name not in body["repos"]:
        raise PermissionError(f"You may not read Icechunk repo '{name}'")
    storage = _storage(body, f"{body['prefix']}/{name}")
    config = ic.Repository.fetch_config(storage)
    containers = (config.virtual_chunk_containers if config else None) or {}
    return ic.Repository.open(
        storage,
        authorize_virtual_chunk_access={prefix: _anonymous_credentials(prefix) for prefix in containers},
    )


def open_icechunk_dataset(name: str, branch: str = "main", **open_zarr_kwargs) -> xr.Dataset:
    """Open a repo's data (its raw_data or references group) as an xarray Dataset."""
    session = open_icechunk_repo(name).readonly_session(branch)
    group = zarr.open_group(session.store, mode="r", zarr_format=3).attrs.get(DATA_GROUP_ATTR, DEFAULT_DATA_GROUP)
    return xr.open_zarr(session.store, group=group, consolidated=False, **open_zarr_kwargs)
