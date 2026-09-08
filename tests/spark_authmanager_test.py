#!/usr/bin/env python3
"""
Integration test: Spark session permission enforcement via AuthManager

When run without TEST_USERNAME set, orchestrates per-user subprocesses so each
user gets a fresh JVM (matching JupyterHub's per-user-pod model and avoiding
static state leakage in TeehrBrokerAuthManager across spark.stop() calls).

Single-user mode (invoked by orchestrator or directly):
  TEST_USERNAME       - Keycloak username (default: admin)
  TEST_PASSWORD       - Keycloak password (default: admin)
  TEST_EXPECTED_READ  - "true"/"false" (default: true)
  TEST_EXPECTED_WRITE - "true"/"false" (default: true)
"""

import sys
import os
import subprocess
import requests
import time
import gc

# --- Orchestrator mode: spawn one subprocess per user for clean JVM isolation ---
TEST_USERS = {
    "admin":     {"password": "admin",     "expected_read": True,  "expected_write": True},
    "poweruser": {"password": "poweruser", "expected_read": True,  "expected_write": True},
    "user":      {"password": "user",      "expected_read": True,  "expected_write": False},
}

if "TEST_USERNAME" not in os.environ:
    all_passed = True
    for username, config in TEST_USERS.items():
        print(f"\n{'='*60}")
        print(f"Running AuthManager test for: {username}")
        print('='*60)
        env = os.environ.copy()
        env["TEST_USERNAME"] = username
        env["TEST_PASSWORD"] = config["password"]
        env["TEST_EXPECTED_READ"] = str(config["expected_read"]).lower()
        env["TEST_EXPECTED_WRITE"] = str(config["expected_write"]).lower()
        result = subprocess.run(
            [sys.executable, __file__],
            env=env,
            timeout=300,
        )
        if result.returncode != 0:
            all_passed = False

    print(f"\n[test] Spark AuthManager enforcement tests {'PASSED' if all_passed else 'FAILED'}")
    sys.exit(0 if all_passed else 1)

# --- Single-user mode: actual test logic, called by orchestrator subprocess ---

# Set up environment before importing PySpark
os.environ.setdefault("POLARIS_DEFAULT_REALM", "teehr")
# create_spark_session() defaults remote_warehouse_dir to "" (not the realm
# name) when this isn't set, which Polaris's REST catalog rejects with
# "Please specify a warehouse" on any catalog read/write.
os.environ.setdefault("REMOTE_WAREHOUSE_IDENTIFIER", "teehr")
os.environ.setdefault("REMOTE_CATALOG_REST_URI", "http://polaris:8181/api/catalog")
os.environ.setdefault("REMOTE_WAREHOUSE_S3_PATH", "s3://warehouse/")
os.environ.setdefault("REMOTE_CATALOG_S3_ENDPOINT", "http://minio:9000")
os.environ.setdefault("REMOTE_CATALOG_S3_PATH_STYLE_ACCESS", "true")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "minioadmin")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "minioadmin123")
os.environ.setdefault("AWS_REGION", "us-east-2")
# Broker and OAuth endpoints
os.environ.setdefault("POLARIS_BROKER_URL", "http://teehr-api:8000/auth/polaris-token")
os.environ.setdefault(
    "POLARIS_OAUTH2_TOKEN_ENDPOINT",
    "http://keycloak-service:8080/realms/teehr/protocol/openid-connect/token",
)
# Use jupyterhub client (same as the password grant below)
os.environ.setdefault("POLARIS_CLIENT_ID", "jupyterhub")
os.environ.setdefault("POLARIS_CLIENT_SECRET", "local-jupyterhub-client-secret")
# JVM heap
os.environ["JAVA_TOOL_OPTIONS"] = "-Xmx1g"
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

sys.path.insert(0, "/opt/teehr")

# Configuration
KEYCLOAK_URL = "http://keycloak-service:8080"
REALM = "teehr"
CATALOG = "iceberg"
NAMESPACE = "teehr"

MAX_RETRIES = 10
RETRY_DELAY = 2

# Single-user mode — configured via env vars to match JupyterHub's per-user-pod model
USERNAME = os.getenv("TEST_USERNAME", "admin")
PASSWORD = os.getenv("TEST_PASSWORD", "admin")
EXPECTED_READ = os.getenv("TEST_EXPECTED_READ", "true").lower() == "true"
EXPECTED_WRITE = os.getenv("TEST_EXPECTED_WRITE", "true").lower() == "true"


def get_keycloak_tokens(username: str, password: str) -> tuple:
    """Get access_token and refresh_token from Keycloak via password grant."""
    for attempt in range(MAX_RETRIES):
        try:
            token_url = f"{KEYCLOAK_URL}/realms/{REALM}/protocol/openid-connect/token"
            payload = {
                "grant_type": "password",
                "client_id": "jupyterhub",
                "client_secret": "local-jupyterhub-client-secret",
                "username": username,
                "password": password,
                "scope": "openid",
            }
            response = requests.post(token_url, data=payload, timeout=10)
            if response.status_code == 200:
                data = response.json()
                return data["access_token"], data.get("refresh_token", "")
            elif attempt < MAX_RETRIES - 1:
                print(f"    Attempt {attempt + 1}/{MAX_RETRIES}: Keycloak not ready, retrying...")
                time.sleep(RETRY_DELAY)
            else:
                raise Exception(f"Failed to get token for {username}: {response.text}")
        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                print(f"    Attempt {attempt + 1}/{MAX_RETRIES}: Connection failed, retrying...")
                time.sleep(RETRY_DELAY)
            else:
                raise Exception(f"Failed to get token for {username}: {e}")

    raise Exception(f"Failed to get token for {username} after {MAX_RETRIES} attempts")


def test_spark_read_access(spark, username: str) -> bool:
    """Test read access by listing tables in namespace."""
    try:
        print(f"  Testing READ access...")
        tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{NAMESPACE}").collect()
        print(f"    ✓ {username} can READ from {NAMESPACE} namespace (found {len(tables)} tables)")
        return True
    except Exception as e:
        error_msg = str(e).lower()
        if any(w in error_msg for w in ("permission", "forbidden", "denied", "403")):
            print(f"    ✓ {username} correctly denied READ access")
        else:
            print(f"    ✗ ERROR during READ test: {type(e).__name__}: {str(e)[:100]}")
        return False


def test_spark_write_access(spark, username: str, should_write: bool = True) -> bool:
    """Test write access via CREATE TABLE + INSERT. Returns True if write succeeded."""
    table_name = f"test_authmanager_{username}_{int(time.time() * 1000)}"
    full_table_name = f"{CATALOG}.{NAMESPACE}.{table_name}"

    try:
        print(f"  Testing WRITE access (CREATE TABLE + INSERT)...")
        spark.sql(f"""
            CREATE TABLE {full_table_name} (
                id INT,
                name STRING
            )
            USING iceberg
        """)
        spark.sql(f"INSERT INTO {full_table_name} VALUES (1, 'test')")

        try:
            spark.sql(f"DROP TABLE {full_table_name}")
        except Exception:
            pass

        if should_write:
            print(f"    ✓ {username} successfully created table and inserted data")
        else:
            print(f"    ✗ ERROR: {username} should NOT be able to write but succeeded!")
        return True

    except Exception as e:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
        except Exception:
            pass

        error_msg = str(e).lower()
        is_permission_error = any(w in error_msg for w in (
            "permission", "forbidden", "denied", "403", "not authorized", "unauthorized", "access"
        ))

        if should_write:
            print(f"    ✗ ERROR: {username} should be able to write but got: {type(e).__name__}")
            print(f"      {str(e)[:150]}")
        elif is_permission_error:
            print(f"    ✓ {username} correctly denied WRITE access (permission error)")
        else:
            print(f"    ✓ {username} denied WRITE access: {type(e).__name__}")
        return False


def main():
    print(f"[test] Starting Spark AuthManager test for user: {USERNAME}")
    print(f"  Expected: read={EXPECTED_READ}, write={EXPECTED_WRITE}\n")

    all_passed = True
    spark = None
    try:
        print("  Getting Keycloak tokens...")
        access_token, refresh_token = get_keycloak_tokens(USERNAME, PASSWORD)
        if not refresh_token:
            print("  ✗ ERROR: No refresh_token returned - required for AuthManager broker session")
            return 1
        print("  ✓ Tokens obtained (access + refresh)")

        os.environ["POLARIS_USER_TOKEN"] = access_token
        os.environ["POLARIS_REFRESH_TOKEN"] = refresh_token
        os.environ["JUPYTERHUB_USER"] = USERNAME

        print("  Creating Spark session via AuthManager...")
        try:
            from teehr.evaluation.spark_session_utils import create_spark_session
            spark = create_spark_session(
                use_authmanager=True,
                force_recreate_session=True,
            )
            print("  ✓ Spark session created via AuthManager")
        except ImportError:
            print("  ✗ ERROR: spark_session_utils not available")
            return 1

        can_read = test_spark_read_access(spark, USERNAME)
        if can_read != EXPECTED_READ:
            print(f"  ✗ ERROR: Expected read={EXPECTED_READ} but got {can_read}")
            all_passed = False

        can_write = test_spark_write_access(spark, USERNAME, should_write=EXPECTED_WRITE)
        if can_write != EXPECTED_WRITE:
            print(f"  ✗ ERROR: Expected write={EXPECTED_WRITE} but got {can_write}")
            all_passed = False

    except Exception as e:
        print(f"  ✗ ERROR: {type(e).__name__}: {str(e)[:200]}")
        all_passed = False
    finally:
        if spark:
            try:
                spark.stop()
            except Exception:
                pass
        gc.collect()

    result = "PASSED" if all_passed else "FAILED"
    print(f"\n[test] Spark AuthManager test for {USERNAME}: {result}")
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
