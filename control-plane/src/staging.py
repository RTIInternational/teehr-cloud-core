"""Upload staging: where uploaded files wait to be inspected and loaded (Phase 3).

A bucket only the control plane reaches. A client PUTs its file straight there with
a presigned URL for one object, so file bytes never pass through the API. The
control plane reads the start of a staged file to inspect it; the load job gets a
presigned URL to read it (Phase 3, jobs).

Routes depend on :func:`get_staging`, which tests replace.
"""

from functools import lru_cache
from typing import Protocol

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from pyarrow import fs

from src.config import Settings, get_settings


class StagedMissing(Exception):
    """Nothing has been uploaded to the staged location (yet)."""


class Staging(Protocol):
    def upload_url(self, key: str, ttl_seconds: int) -> str:
        """A URL to PUT one object to, valid for ``ttl_seconds``."""

    def download_url(self, key: str, ttl_seconds: int) -> str:
        """A URL to GET one object from, valid for ``ttl_seconds``."""

    def size(self, key: str) -> int:
        """The object's size in bytes; raises StagedMissing if there's none."""

    def open(self, key: str):
        """A seekable, read-only file for the object (reads by byte range)."""

    def delete(self, key: str) -> None:
        """Remove the object; no error if it's gone."""

    def delete_prefix(self, prefix: str) -> int:
        """Remove every object under ``prefix``; returns how many."""


class S3Staging:
    """:class:`Staging` on an S3 bucket."""

    def __init__(self, settings: Settings):
        self.bucket = settings.staging_bucket
        credentials = {}
        if settings.staging_access_key:
            credentials = {
                "aws_access_key_id": settings.staging_access_key,
                "aws_secret_access_key": settings.staging_secret_key,
            }
        path_style = Config(signature_version="s3v4", s3={"addressing_style": "path"})
        default = Config(signature_version="s3v4")
        config = path_style if settings.staging_endpoint else default

        def client(endpoint):
            return boto3.client(
                "s3",
                endpoint_url=endpoint,
                region_name=settings.storage_region,
                config=config,
                **credentials,
            )

        self._s3 = client(settings.staging_endpoint)
        # URLs are signed for the address the client will use.
        self._signer = client(
            settings.staging_public_endpoint or settings.staging_endpoint
        )
        endpoint = settings.staging_endpoint
        self._fs = fs.S3FileSystem(
            access_key=settings.staging_access_key,
            secret_key=settings.staging_secret_key,
            region=settings.storage_region,
            endpoint_override=endpoint.split("://", 1)[-1] if endpoint else None,
            scheme=endpoint.split("://", 1)[0] if endpoint else "https",
            allow_bucket_creation=False,
        )

    def upload_url(self, key: str, ttl_seconds: int) -> str:
        return self._signer.generate_presigned_url(
            "put_object",
            Params={"Bucket": self.bucket, "Key": key},
            ExpiresIn=ttl_seconds,
        )

    def download_url(self, key: str, ttl_seconds: int) -> str:
        return self._s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket, "Key": key},
            ExpiresIn=ttl_seconds,
        )

    def size(self, key: str) -> int:
        try:
            head = self._s3.head_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey"):
                raise StagedMissing(key) from exc
            raise
        return int(head["ContentLength"])

    def open(self, key: str):
        return self._fs.open_input_file(f"{self.bucket}/{key}")

    def delete(self, key: str) -> None:
        self._s3.delete_object(Bucket=self.bucket, Key=key)

    def delete_prefix(self, prefix: str) -> int:
        removed = 0
        pages = self._s3.get_paginator("list_objects_v2").paginate(
            Bucket=self.bucket, Prefix=prefix
        )
        for page in pages:
            keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if keys:  # a page holds at most 1000, the batch-delete limit
                self._s3.delete_objects(Bucket=self.bucket, Delete={"Objects": keys})
                removed += len(keys)
        return removed


@lru_cache
def get_staging() -> Staging | None:
    """Upload staging, or None when it isn't configured."""
    settings = get_settings()
    if not settings.staging_bucket:
        return None
    return S3Staging(settings)
