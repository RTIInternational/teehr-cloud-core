import base64
import json
import os
import time
from typing import Dict, Optional, Tuple

import requests
from teehr.evaluation.spark_session_utils import create_spark_session


# Handpicked sites that seemed interesting
DEV_LOCATION_ID_LIST = [
    # CONUS
    "usgs-02424000",
    "usgs-03068800",
    "usgs-01570500",
    "usgs-01347000",
    "usgs-05443500",
    "usgs-06770500",
    "usgs-08313000",
    "usgs-11421000",
    "usgs-14319500",
    # Alaska
    "usgs-15200280",
    "usgs-15209700",
    "usgs-15209750",
    "usgs-15214000",
    # Hawaii
    "usgs-16010000",
    "usgs-16019000",
    "usgs-16031000",
    "usgs-16060000",
    # Puerto Rico
    "usgs-50010500",
    "usgs-50011000",
    "usgs-50011085",
    "usgs-50011128"
]


def _decode_jwt_claims(token: str) -> Dict[str, object]:
    payload = token.split(".")[1]
    payload += "=" * (-len(payload) % 4)
    return json.loads(base64.urlsafe_b64decode(payload.encode()))


def _token_expires_soon(token: str, refresh_window_seconds: int = 120) -> bool:
    try:
        claims = _decode_jwt_claims(token)
    except Exception:
        return True
    exp = int(claims.get("exp", 0))
    now = int(time.time())
    return exp <= now + max(refresh_window_seconds, 1)


def _request_oauth_tokens(
    data: Dict[str, str],
    token_endpoint: Optional[str] = None,
    timeout_seconds: int = 20,
) -> Tuple[str, Optional[str]]:
    endpoint = token_endpoint or os.getenv("POLARIS_OAUTH2_SERVER_URI")
    print(endpoint)
    if not endpoint:
        raise RuntimeError("POLARIS_OAUTH2_SERVER_URI is required to mint or refresh a user token")

    resp = requests.post(endpoint, data=data, timeout=timeout_seconds)
    print(resp)
    resp.raise_for_status()
    payload = resp.json()

    access_token = payload.get("access_token")
    print(access_token)
    if not access_token:
        raise RuntimeError("Token endpoint did not return access_token")

    return access_token, payload.get("refresh_token")


def mint_polaris_user_token(
    username: Optional[str],
    password: Optional[str],
    client_id: str,
    client_secret: Optional[str] = None,
    token_endpoint: Optional[str] = None,
) -> str:
    if not username or not password:
        raise RuntimeError("username and password are required for password grant token minting")

    data = {
        "grant_type": "password",
        "client_id": client_id,
        "username": username,
        "password": password,
        "scope": "openid profile email",
    }
    if client_secret:
        data["client_secret"] = client_secret

    access_token, _ = _request_oauth_tokens(data=data, token_endpoint=token_endpoint)
    return access_token


def refresh_polaris_user_token(
    refresh_token: str,
    client_id: str,
    client_secret: Optional[str] = None,
    token_endpoint: Optional[str] = None,
) -> Tuple[str, Optional[str]]:
    if not refresh_token:
        raise RuntimeError("refresh_token is required for refresh grant")

    data = {
        "grant_type": "refresh_token",
        "client_id": client_id,
        "refresh_token": refresh_token,
    }
    if client_secret:
        data["client_secret"] = client_secret

    print(data)
    print(token_endpoint)

    return _request_oauth_tokens(data=data, token_endpoint=token_endpoint)


def ensure_fresh_polaris_user_token(
    current_token: Optional[str],
    username: Optional[str],
    password: Optional[str],
    client_id: str,
    client_secret: Optional[str] = None,
    refresh_token: Optional[str] = None,
    allow_password_fallback: bool = True,
    refresh_window_seconds: int = 120,
    token_endpoint: Optional[str] = None,
) -> Tuple[str, Optional[str], bool]:
    if current_token and not _token_expires_soon(current_token, refresh_window_seconds):
        return current_token, refresh_token, False

    if refresh_token:
        try:
            print("trying to refresh token")
            print(refresh_token)
            refreshed_access, refreshed_refresh = refresh_polaris_user_token(
                refresh_token=refresh_token,
                client_id=client_id,
                client_secret=client_secret,
                token_endpoint=token_endpoint,
            )
            return refreshed_access, (refreshed_refresh or refresh_token), True
        except requests.RequestException:
            if not allow_password_fallback:
                print("Passowrd fall back not enabled")
                raise

    if allow_password_fallback and username and password:
        minted = mint_polaris_user_token(
            username=username,
            password=password,
            client_id=client_id,
            client_secret=client_secret,
            token_endpoint=token_endpoint,
        )
        return minted, refresh_token, True

    raise RuntimeError(
        "Unable to obtain a fresh Polaris user token. "
        "Provide POLARIS_REFRESH_TOKEN or enable password-grant fallback credentials."
    )


def apply_polaris_token_to_spark(
    spark,
    token: str,
    catalog_name: str = "iceberg",
    realm: Optional[str] = None,
) -> None:
    active_realm = realm or os.getenv("POLARIS_DEFAULT_REALM", "teehr")
    base = f"spark.sql.catalog.{catalog_name}"
    spark.conf.set(f"{base}.rest.auth.type", "oauth2")
    spark.conf.set(f"{base}.token", token)
    spark.conf.set(f"{base}.rest.auth.oauth2.token", token)
    spark.conf.set(f"{base}.header.X-Polaris-Realm", active_realm)
    spark.conf.set(f"{base}.rest.transport.header.X-Polaris-Realm", active_realm)

def _as_bool_str(value: str, default: str = "true") -> str:
    normalized = (value or default).strip().lower()
    return "true" if normalized in ("1", "true", "t", "yes", "y", "on") else "false"


def _apply_runtime_spark_configs(spark, configs: Dict[str, str]) -> None:
    for key, value in configs.items():
        if not key.startswith("spark."):
            continue
        spark.conf.set(key, value)


def create_minio_spark_session(
    polaris_token: Optional[str] = None,
    force_recreate_session: bool = False,
    update_configs: Optional[Dict[str, str]] = None,
):
    """Start a Spark session configured for the local Polaris REST catalog.

    If ``polaris_token`` is provided, Spark uses user-token OAuth2 auth.
    Otherwise it falls back to client-credential OAuth2 using env vars.
    """
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123")

    remote_catalog_uri = os.getenv("REMOTE_CATALOG_REST_URI", "http://polaris:8181/api/catalog")
    remote_warehouse_dir = os.getenv("REMOTE_WAREHOUSE_S3_PATH", "s3://warehouse/")
    polaris_realm = os.getenv("POLARIS_DEFAULT_REALM", "teehr")

    if remote_catalog_uri.rstrip("/").endswith("/api/catalog"):
        # Polaris REST expects the catalog identifier here, not the backing S3 URI.
        remote_warehouse_dir = polaris_realm

    s3_endpoint = os.getenv("REMOTE_CATALOG_S3_ENDPOINT", "http://minio:9000")
    s3_path_style = _as_bool_str(os.getenv("REMOTE_CATALOG_S3_PATH_STYLE_ACCESS", "true"))
    s3_region = os.getenv("AWS_REGION", "us-east-2")

    merged_configs: Dict[str, str] = {
        "spark.sql.catalog.iceberg.warehouse": remote_warehouse_dir,
        "spark.sql.catalog.iceberg.header.X-Polaris-Realm": polaris_realm,
        "spark.sql.catalog.iceberg.rest.transport.header.X-Polaris-Realm": polaris_realm,
        "spark.sql.catalog.iceberg.s3.endpoint": s3_endpoint,
        "spark.sql.catalog.iceberg.s3.path-style-access": s3_path_style,
        "spark.sql.catalog.iceberg.s3.region": s3_region,
        "spark.hadoop.fs.s3a.endpoint": s3_endpoint,
        "spark.hadoop.fs.s3a.path.style.access": s3_path_style,
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    }

    if polaris_token:
        merged_configs["spark.sql.catalog.iceberg.rest.auth.type"] = "oauth2"
        merged_configs["spark.sql.catalog.iceberg.token"] = polaris_token
        merged_configs["spark.sql.catalog.iceberg.rest.auth.oauth2.token"] = polaris_token
    else:
        oauth_server_uri = os.getenv("POLARIS_OAUTH2_SERVER_URI")
        spark_polaris_client_secret = os.getenv("SPARK_POLARIS_CLIENT_SECRET")

        merged_configs["spark.sql.catalog.iceberg.rest.auth.type"] = "oauth2"
        merged_configs["spark.sql.catalog.iceberg.scope"] = "openid"
        merged_configs["spark.sql.catalog.iceberg.rest.auth.oauth2.scope"] = "openid"
        if oauth_server_uri:
            merged_configs["spark.sql.catalog.iceberg.oauth2-server-uri"] = oauth_server_uri
            merged_configs["spark.sql.catalog.iceberg.rest.auth.oauth2.server-uri"] = oauth_server_uri
        if spark_polaris_client_secret:
            merged_configs["spark.sql.catalog.iceberg.credential"] = (
                f"spark-polaris:{spark_polaris_client_secret}"
            )
            merged_configs["spark.sql.catalog.iceberg.rest.auth.oauth2.credential"] = (
                f"spark-polaris:{spark_polaris_client_secret}"
            )

    if update_configs:
        merged_configs.update(update_configs)

    call_kwargs = {
        "remote_catalog_uri": remote_catalog_uri,
        "remote_warehouse_dir": remote_warehouse_dir,
        "aws_access_key_id": aws_access_key_id,
        "aws_secret_access_key": aws_secret_access_key,
        "force_recreate_session": force_recreate_session,
        "update_configs": merged_configs,
    }
    if polaris_token:
        # Preferred path for newer teehr versions that support direct token auth.
        call_kwargs["oauth2_token"] = polaris_token

    while True:
        try:
            spark = create_spark_session(**call_kwargs)
            _apply_runtime_spark_configs(spark, merged_configs)
            return spark
        except TypeError as exc:
            msg = str(exc)
            if "oauth2_token" in msg and "oauth2_token" in call_kwargs:
                call_kwargs.pop("oauth2_token", None)
                continue
            if "force_recreate_session" in msg and "force_recreate_session" in call_kwargs:
                call_kwargs.pop("force_recreate_session", None)
                continue
            raise