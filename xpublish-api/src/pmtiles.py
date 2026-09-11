"""
Authenticated access to the pmtiles vector-tile archives.

MapLibre reads a pmtiles archive with HTTP range requests, which it issues
itself rather than through the app's fetch wrapper — so the bearer token that
guards the rest of the API never reaches object storage, and object storage
could not validate it anyway (S3 speaks SigV4, not OIDC).  Serving the bytes
here instead keeps the bucket private and puts the archives behind the same
Keycloak gate as everything else.
"""

import logging
import os
import re

from botocore.exceptions import ClientError
from fastapi import HTTPException
from fastapi.responses import Response

from .storage import build_s3_client, list_storage_files, normalize_storage_prefix

logger = logging.getLogger(__name__)

PMTILES_EXTENSION = ".pmtiles"

# The layer name arrives in the URL path and is interpolated into an S3 key, so
# restrict it to characters that cannot escape the configured prefix.
_LAYER_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def resolve_pmtiles_location() -> tuple[str, str]:
    """
    Return the (bucket, prefix) holding the .pmtiles archives.

    An unset bucket is fatal for the same reason as ``ICECHUNK_BUCKET``: an
    empty prefix just means nothing has been published yet, but missing
    configuration can never resolve itself.
    """
    bucket = os.getenv("PMTILES_BUCKET", "").strip()
    prefix = os.getenv("PMTILES_PREFIX", "").strip().strip("/")
    if not bucket:
        raise RuntimeError("PMTILES_BUCKET is required: S3 bucket holding .pmtiles archives")
    return bucket, prefix


def layer_key(prefix: str, layer: str) -> str:
    if ".." in layer or not _LAYER_NAME.match(layer):
        raise HTTPException(status_code=400, detail="Invalid layer name")
    return f"{normalize_storage_prefix(prefix)}{layer}{PMTILES_EXTENSION}"


def list_pmtiles_layers() -> list[dict]:
    """Return the available layers as ``{id, source_layer}`` pairs.

    The S3 key is deliberately omitted: callers address a layer by name
    through ``/vector-tiles/{layer}.pmtiles``, so exposing bucket paths to the
    browser would serve no purpose.
    """
    bucket, prefix = resolve_pmtiles_location()
    layers = list_storage_files(build_s3_client(), bucket, prefix, PMTILES_EXTENSION)
    return [{"id": layer["id"], "source_layer": layer["source_layer"]} for layer in layers]


def read_pmtiles_range(layer: str, range_header: str | None) -> Response:
    """Proxy one (possibly partial) read of a pmtiles archive from S3."""
    bucket, prefix = resolve_pmtiles_location()
    key = layer_key(prefix, layer)

    kwargs = {"Bucket": bucket, "Key": key}
    if range_header:
        kwargs["Range"] = range_header

    try:
        obj = build_s3_client().get_object(**kwargs)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if error_code in {"NoSuchKey", "NoSuchBucket"} or status == 404:
            raise HTTPException(status_code=404, detail=f"Unknown layer '{layer}'") from exc
        if error_code == "InvalidRange" or status == 416:
            raise HTTPException(status_code=416, detail="Requested range not satisfiable") from exc
        logger.error("pmtiles read failed for s3://%s/%s: %s", bucket, key, exc)
        raise HTTPException(status_code=502, detail="Tile archive read failed") from exc

    content_range = obj.get("ContentRange")
    headers = {
        # pmtiles only issues range requests once it has seen this.
        "Accept-Ranges": "bytes",
        "Cache-Control": "private, max-age=300",
    }
    if obj.get("ETag"):
        # pmtiles compares etags across requests to notice a replaced archive.
        headers["ETag"] = obj["ETag"]
    if content_range:
        headers["Content-Range"] = content_range

    return Response(
        content=obj["Body"].read(),
        status_code=206 if content_range else 200,
        media_type="application/octet-stream",
        headers=headers,
    )
