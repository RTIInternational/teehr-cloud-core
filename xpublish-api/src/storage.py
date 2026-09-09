"""
S3-compatible storage helpers shared by the app and the icechunk provider.

These live outside ``main`` so that ``provider`` can list the icechunk prefix
without importing the module that imports it.  ``main`` uses the same helpers
for the ``/storage/contents`` endpoint (pmtiles / directory listings).
"""

import os

import boto3
from botocore.config import Config


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


def resolve_icechunk_location() -> tuple[str, str]:
    """
    Return the (bucket, prefix) that holds all icechunk repos.

    Missing configuration is fatal — unlike an empty prefix, which just means
    nothing has been ingested yet, an unset env var can never resolve itself.
    """
    bucket = os.getenv("ICECHUNK_BUCKET", "").strip()
    prefix = os.getenv("ICECHUNK_PREFIX", "").strip().rstrip("/")
    if not bucket:
        raise RuntimeError("ICECHUNK_BUCKET is required: S3 bucket name")
    if not prefix:
        raise RuntimeError("ICECHUNK_PREFIX is required: base prefix path for icechunk repos")
    return bucket, prefix


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
