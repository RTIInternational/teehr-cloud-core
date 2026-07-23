#!/usr/bin/env python3
"""Minimal Polaris + Spark example for local KinD + MinIO development.

This script is designed to run inside a Jupyter single-user pod.
It verifies Spark catalog connectivity, creates a namespace, and then
attempts to create and read a simple Iceberg table.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import time
import traceback

import requests


def _ensure_setup_utils_importable() -> None:
    preferred = "/tmp/polaris-spark-example"
    if preferred not in sys.path:
        sys.path.insert(0, preferred)

    candidates = [
        ".",
        "/workspace",
        "/workspace/examples/developer",
        "/home/jovyan",
        "/home/jovyan/examples/developer",
    ]
    for path in candidates:
        if path not in sys.path:
            sys.path.append(path)


_ensure_setup_utils_importable()

from setup_utils import (
    apply_polaris_token_to_spark,
    create_minio_spark_session,
    ensure_fresh_polaris_user_token,
)


def _decode_token_claims(token: str) -> dict:
    payload = token.split(".")[1]
    payload += "=" * (-len(payload) % 4)
    return json.loads(base64.urlsafe_b64decode(payload.encode()))


def _print_token_claims(token: str) -> None:
    claims = _decode_token_claims(token)
    roles = sorted(claims.get("realm_access", {}).get("roles", []))
    print(f"[example] Token user: {claims.get('preferred_username')}")
    print(f"[example] Token roles: {', '.join(roles)}")


def _print_spark_catalog_config(spark) -> None:
    keys = [
        "spark.sql.catalog.iceberg.type",
        "spark.sql.catalog.iceberg.uri",
        "spark.sql.catalog.iceberg.warehouse",
        "spark.sql.catalog.iceberg.token",
        "spark.sql.catalog.iceberg.header.X-Polaris-Realm",
        "spark.sql.catalog.iceberg.rest.auth.type",
        "spark.sql.catalog.iceberg.rest.auth.oauth2.token",
        "spark.sql.catalog.iceberg.rest.transport.header.X-Polaris-Realm",
        "spark.sql.catalog.iceberg.io-impl",
        "spark.sql.catalog.iceberg.s3.endpoint",
        "spark.sql.catalog.iceberg.s3.path-style-access",
        "spark.sql.catalog.iceberg.s3.region",
        "spark.hadoop.fs.s3a.endpoint",
        "spark.hadoop.fs.s3a.path.style.access",
    ]
    print("[example] Effective Spark catalog config:")
    for key in keys:
        value = spark.conf.get(key, "<missing>")
        if "token" in key and value not in ("<missing>", ""):
            value = f"{value[:12]}...{value[-8:]}"
        print(f"  {key}={value}")


def _apply_direct_catalog_overrides(spark, token: str) -> None:
    realm = os.getenv("POLARIS_DEFAULT_REALM", "teehr")
    spark.conf.set("spark.sql.catalog.iceberg.warehouse", realm)
    apply_polaris_token_to_spark(spark, token, catalog_name="iceberg", realm=realm)


def _is_auth_failure(exc: Exception) -> bool:
    msg = str(exc).lower()
    markers = [
        "notauthorizedexception",
        "unauthorized",
        "http error 401",
        "401",
        "invalid token",
        "token expired",
    ]
    return any(marker in msg for marker in markers)


def _sql_with_token_retry(spark, sql_text: str, token_ctx: dict, show: bool = False):
    try:
        df = spark.sql(sql_text)
        if show:
            df.show(truncate=False)
        return df
    except Exception as exc:
        if not _is_auth_failure(exc):
            raise

        print("[example] SQL failed due to auth; renewing token and retrying once...")
        refreshed, refreshed_refresh_token, renewed = ensure_fresh_polaris_user_token(
            current_token=token_ctx["token"],
            username=token_ctx["username"],
            password=token_ctx["password"],
            client_id=token_ctx["client_id"],
            client_secret=token_ctx["client_secret"],
            refresh_token=token_ctx.get("refresh_token"),
            refresh_window_seconds=300,
        )
        token_ctx["token"] = refreshed
        token_ctx["refresh_token"] = refreshed_refresh_token
        if refreshed_refresh_token:
            os.environ["POLARIS_REFRESH_TOKEN"] = refreshed_refresh_token
        apply_polaris_token_to_spark(spark, refreshed, catalog_name="iceberg")
        if renewed:
            print("[example] Token refreshed; retrying SQL.")

        df = spark.sql(sql_text)
        if show:
            df.show(truncate=False)
        return df


def _set_local_polaris_env_defaults() -> None:
    realm = os.getenv("POLARIS_DEFAULT_REALM", "teehr")
    os.environ["REMOTE_CATALOG_REST_URI"] = os.getenv(
        "REMOTE_CATALOG_REST_URI", "http://polaris:8181/api/catalog"
    )
    os.environ["REMOTE_WAREHOUSE_S3_PATH"] = realm
    os.environ["REMOTE_CATALOG_S3_ENDPOINT"] = os.getenv(
        "REMOTE_CATALOG_S3_ENDPOINT", "http://minio:9000"
    )
    os.environ["REMOTE_CATALOG_S3_PATH_STYLE_ACCESS"] = os.getenv(
        "REMOTE_CATALOG_S3_PATH_STYLE_ACCESS", "true"
    )
    os.environ["POLARIS_DEFAULT_REALM"] = realm


def _validate_direct_polaris_access(token: str) -> None:
    realm = os.getenv("POLARIS_DEFAULT_REALM", "teehr")
    headers = {
        "Authorization": f"Bearer {token}",
        "X-Polaris-Realm": realm,
    }
    resp = requests.get(
        "http://polaris:8181/api/catalog/v1/config",
        headers=headers,
        params={"warehouse": realm},
        timeout=20,
    )
    print(f"[example] Direct Polaris config status: {resp.status_code}")
    if resp.status_code >= 400:
        print(f"[example] Direct Polaris config body: {resp.text[:500]}")
    resp.raise_for_status()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--username", default=os.getenv("POLARIS_TEST_USERNAME", "admin"))
    parser.add_argument("--password", default=os.getenv("POLARIS_TEST_PASSWORD", "admin"))
    parser.add_argument(
        "--oauth-client-id",
        default=os.getenv("POLARIS_OAUTH_CLIENT_ID", "jupyterhub"),
    )
    parser.add_argument(
        "--oauth-client-secret",
        default=os.getenv("POLARIS_OAUTH_CLIENT_SECRET"),
    )
    parser.add_argument(
        "--refresh-token",
        default=os.getenv("POLARIS_REFRESH_TOKEN"),
        help="Refresh token from Jupyter login context, if available",
    )
    parser.add_argument(
        "--namespace",
        default=f"spark_demo_{int(time.time())}",
        help="Namespace to create (default: spark_demo_<timestamp>)",
    )
    parser.add_argument(
        "--table",
        default="hello_table",
        help="Table name to create inside the namespace",
    )
    parser.add_argument(
        "--keep",
        action="store_true",
        help="Keep created namespace/table instead of dropping at end",
    )
    args = parser.parse_args()

    spark = None
    created_namespace = False
    created_table = False

    try:
        token = os.getenv("POLARIS_USER_TOKEN")
        if token:
            print("[example] Using POLARIS_USER_TOKEN from Jupyter environment...")
        print(f"[example] Ensuring a fresh token for user '{args.username}'...")
        token, refresh_token, renewed = ensure_fresh_polaris_user_token(
            current_token=token,
            username=args.username,
            password=args.password,
            client_id=args.oauth_client_id,
            client_secret=args.oauth_client_secret,
            refresh_token=args.refresh_token,
            refresh_window_seconds=300,
        )
        if refresh_token:
            os.environ["POLARIS_REFRESH_TOKEN"] = refresh_token
        if renewed:
            print("[example] Minted or renewed access token.")

        token_ctx = {
            "token": token,
            "username": args.username,
            "password": args.password,
            "client_id": args.oauth_client_id,
            "client_secret": args.oauth_client_secret,
            "refresh_token": refresh_token,
        }

        _print_token_claims(token)

        _set_local_polaris_env_defaults()
        _validate_direct_polaris_access(token)

        print("[example] Creating Spark session...")
        spark = create_minio_spark_session(
            polaris_token=token,
            force_recreate_session=True,
        )

        _apply_direct_catalog_overrides(spark, token)

        _print_spark_catalog_config(spark)

        print("[example] Existing namespaces:")
        _sql_with_token_retry(spark, "SHOW NAMESPACES IN iceberg", token_ctx, show=True)

        fq_namespace = f"iceberg.{args.namespace}"
        fq_table = f"{fq_namespace}.{args.table}"

        print(f"[example] Creating namespace: {fq_namespace}")
        _sql_with_token_retry(spark, f"CREATE NAMESPACE IF NOT EXISTS {fq_namespace}", token_ctx)
        created_namespace = True

        print(f"[example] Creating table: {fq_table}")
        _sql_with_token_retry(spark, f"CREATE TABLE {fq_table} (id int, name string) USING iceberg", token_ctx)
        created_table = True

        print(f"[example] Inserting sample rows into: {fq_table}")
        _sql_with_token_retry(
            spark,
            f"INSERT INTO {fq_table} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')",
            token_ctx,
        )

        print(f"[example] Reading back rows from: {fq_table}")
        _sql_with_token_retry(spark, f"SELECT * FROM {fq_table} ORDER BY id", token_ctx, show=True)

        print("[example] SUCCESS")
        return 0

    except Exception as exc:
        msg = str(exc)
        print("[example] FAILED")
        traceback.print_exc()

        if "warehouse.minio" in msg or "UnknownHostException" in msg:
            print("[example] Diagnostic: Polaris server is attempting virtual-host S3 DNS")
            print("[example] Diagnostic: expected local path-style access for MinIO")
            print("[example] Suggested checks:")
            print("  1) Re-run Polaris bootstrap to refresh catalog properties")
            print("  2) Verify catalog has s3.path-style-access=true and endpoint=http://minio:9000")
            print("  3) Check Polaris logs for the exact hostname in UnknownHostException")
        return 1

    finally:
        if spark is not None:
            if created_table and created_namespace and not args.keep:
                fq_namespace = f"iceberg.{args.namespace}"
                fq_table = f"{fq_namespace}.{args.table}"
                try:
                    print(f"[example] Cleaning up table: {fq_table}")
                    spark.sql(f"DROP TABLE IF EXISTS {fq_table}")
                    print(f"[example] Cleaning up namespace: {fq_namespace}")
                    spark.sql(f"DROP NAMESPACE IF EXISTS {fq_namespace}")
                except Exception:
                    print("[example] Cleanup skipped due to error")
            print("[example] Stopping Spark session...")
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
