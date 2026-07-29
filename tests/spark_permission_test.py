#!/usr/bin/env python3
"""
Integration test: Spark session permission enforcement with Polaris

Validates real permission enforcement through Spark operations:
- admin: Can create tables, insert, read
- poweruser (teehr-read-write): Can create tables, insert, read
- user (teehr-read-only): Can read tables, but NOT create or insert
"""

import sys
import os
import requests
import time
import gc
from typing import Optional

# Set up environment for Polaris/Spark before importing PySpark
os.environ.setdefault("POLARIS_DEFAULT_REALM", "teehr")
os.environ.setdefault("REMOTE_CATALOG_REST_URI", "http://polaris:8181/api/catalog")
os.environ.setdefault("REMOTE_WAREHOUSE_S3_PATH", "s3://warehouse/")
os.environ.setdefault("REMOTE_CATALOG_S3_ENDPOINT", "http://minio:9000")
os.environ.setdefault("REMOTE_CATALOG_S3_PATH_STYLE_ACCESS", "true")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "minioadmin")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "minioadmin123")
os.environ.setdefault("AWS_REGION", "us-east-2")
# Set JVM heap size BEFORE PySpark initializes the JVM - must use JAVA_TOOL_OPTIONS
# spark.driver.memory config is ignored if JVM heap is already too small
os.environ["JAVA_TOOL_OPTIONS"] = "-Xmx1g"
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

# Set up path to import spark_session_utils from /opt/teehr (copied in Dockerfile)
sys.path.insert(0, "/opt/teehr")

# Configuration
KEYCLOAK_URL = "http://keycloak-service:8080"
REALM = "teehr"
CATALOG = "iceberg"   # Spark catalog name (spark.sql.catalog.<name>)
NAMESPACE = "teehr"   # Polaris/Iceberg namespace name

# Retry configuration
MAX_RETRIES = 10
RETRY_DELAY = 2  # seconds

TEST_USERS = {
    "admin": {
        "password": "admin",
        "expected_read": True,
        "expected_write": True
    },
    "poweruser": {
        "password": "poweruser",
        "expected_read": True,
        "expected_write": True
    },
    "user": {
        "password": "user",
        "expected_read": True,
        "expected_write": False
    }
}


def get_keycloak_token(username: str, password: str) -> str:
    """Get a JWT token from Keycloak for a user"""
    for attempt in range(MAX_RETRIES):
        try:
            token_url = f"{KEYCLOAK_URL}/realms/{REALM}/protocol/openid-connect/token"
            payload = {
                "grant_type": "password",
                "client_id": "jupyterhub",
                "client_secret": "local-jupyterhub-client-secret",
                "username": username,
                "password": password,
                "scope": "openid"
            }
            
            response = requests.post(token_url, data=payload, timeout=5)
            if response.status_code == 200:
                return response.json()["access_token"]
            elif response.status_code == 401 and attempt < MAX_RETRIES - 1:
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


def test_spark_read_access(spark, username: str, catalog: str = "iceberg", namespace: str = "teehr") -> bool:
    """Test read access by listing tables in namespace"""
    try:
        print(f"  Testing READ access...")
        # List tables using fully-qualified catalog.namespace reference
        tables = spark.sql(f"SHOW TABLES IN {catalog}.{namespace}").collect()
        print(f"    ✓ {username} can READ from {namespace} namespace (found {len(tables)} tables)")
        return True
    except Exception as e:
        error_msg = str(e).lower()
        if "permission" in error_msg or "forbidden" in error_msg or "denied" in error_msg or "403" in error_msg:
            print(f"    ✓ {username} correctly denied READ access")
            return False
        else:
            print(f"    ✗ ERROR during READ test: {type(e).__name__}: {str(e)[:100]}")
            return False


def test_spark_write_access(spark, username: str, catalog: str = "iceberg", namespace: str = "teehr", should_write: bool = True) -> bool:
    """Test write access by creating a table and inserting data. Returns True if write succeeded, False if denied/failed."""
    table_name = f"test_table_{username}_{int(time.time() * 1000)}"
    full_table_name = f"{catalog}.{namespace}.{table_name}"

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

        # Clean up
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
        # Always attempt cleanup even if write failed partway through
        try:
            spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
        except Exception:
            pass

        error_msg = str(e).lower()
        is_permission_error = any(w in error_msg for w in ("permission", "forbidden", "denied", "403", "not authorized", "unauthorized", "access"))

        if should_write:
            print(f"    ✗ ERROR: {username} should be able to write but got: {type(e).__name__}")
            print(f"      {str(e)[:150]}")
        elif is_permission_error:
            print(f"    ✓ {username} correctly denied WRITE access (permission error)")
        else:
            print(f"    ✓ {username} denied WRITE access: {type(e).__name__}")
        return False


def main():
    """Run Spark permission enforcement tests"""
    print("[test] Starting Spark permission enforcement tests...\n")
    
    all_passed = True
    
    for username, config in TEST_USERS.items():
        print(f"Testing user: {username}")
        print(f"  Expected: read={config['expected_read']}, write={config['expected_write']}")
        
        spark = None
        try:
            # Step 1: Get Keycloak token
            print("  Getting Keycloak token...")
            token = get_keycloak_token(username, config["password"])
            print("  ✓ Token obtained")
            
            # Step 2: Create Spark session with Polaris catalog
            print("  Creating Spark session with Polaris catalog...")
            try:
                from spark_session_utils import create_minio_spark_session
                spark = create_minio_spark_session(
                    polaris_token=token,
                )
                print("  ✓ Spark session created")
            except ImportError:
                # If spark_session_utils is not available, skip this user
                print("  ✗ ERROR: spark_session_utils not available")
                all_passed = False
                continue
            
            # Step 3: Test read access
            can_read = test_spark_read_access(spark, username, catalog=CATALOG, namespace=NAMESPACE)
            if can_read != config["expected_read"]:
                print(f"  ✗ ERROR: Expected read={config['expected_read']} but got {can_read}")
                all_passed = False
            
            # Step 4: Test write access
            can_write = test_spark_write_access(spark, username, catalog=CATALOG, namespace=NAMESPACE, should_write=config["expected_write"])
            if can_write != config["expected_write"]:
                print(f"  ✗ ERROR: Expected write={config['expected_write']} but got {can_write}")
                all_passed = False
            
            print()
            
        except Exception as e:
            print(f"  ✗ ERROR: {type(e).__name__}: {str(e)[:200]}\n")
            all_passed = False
        finally:
            # Cleanup: stop Spark session and force garbage collection
            if spark:
                try:
                    spark.stop()
                except:
                    pass
            # Force garbage collection to free memory between sessions
            gc.collect()
            time.sleep(0.5)
    
    print("[test] Spark permission enforcement tests", "PASSED" if all_passed else "FAILED")
    
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
