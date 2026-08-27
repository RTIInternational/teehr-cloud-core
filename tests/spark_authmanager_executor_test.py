#!/usr/bin/env python3
"""
Integration test: Spark cluster-mode executor auth via AuthManager

Validates that Spark EXECUTORS (not just the driver) can participate in
real distributed Iceberg catalog operations when POLARIS_USE_AUTHMANAGER
is enabled with start_spark_cluster=True, and that the Polaris auth env
vars AuthManager needs are actually propagated to executor pods.

TEST_USERNAME       - Keycloak username (default: admin)
TEST_PASSWORD       - Keycloak password (default: admin)
"""

import sys
import os
import socket
import time
import gc

import requests
from pyspark.sql import Row
from pyspark.sql import functions as F

# Set up environment for Polaris/Spark before importing PySpark
os.environ.setdefault("POLARIS_DEFAULT_REALM", "teehr")
# create_spark_session() (unlike create_minio_spark_session(), which we
# deliberately don't use here) defaults remote_warehouse_dir to "" rather
# than the realm name when this isn't set, which Polaris's REST catalog
# rejects with "Please specify a warehouse" on CREATE TABLE.
os.environ.setdefault("REMOTE_WAREHOUSE_IDENTIFIER", "teehr")
os.environ.setdefault("REMOTE_CATALOG_REST_URI", "http://polaris:8181/api/catalog")
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
os.environ.setdefault("POLARIS_CLIENT_ID", "jupyterhub")
os.environ.setdefault("POLARIS_CLIENT_SECRET", "local-jupyterhub-client-secret")
# JVM heap
os.environ["JAVA_TOOL_OPTIONS"] = "-Xmx1g"
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

sys.path.insert(0, "/opt/teehr")

KEYCLOAK_URL = "http://keycloak-service:8080"
REALM = "teehr"
CATALOG = "iceberg"
NAMESPACE = "teehr"

MAX_RETRIES = 10
RETRY_DELAY = 2

TEST_USERNAME = os.getenv("TEST_USERNAME", "admin")
TEST_PASSWORD = os.getenv("TEST_PASSWORD", "admin")

EXPECTED_EXECUTOR_ENV_KEYS = [
    "POLARIS_DEFAULT_REALM",
    "POLARIS_BROKER_SESSION_TOKEN",
]


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


def probe_executor_env(spark, expected_env_keys, partitions=4):
    """Confirm expected env vars are present on executors.

    Returns one row per partition attempt with only booleans + executor
    identity. Never returns secret values.
    """
    keys = list(expected_env_keys)

    def _probe_partition(it):
        import os
        # Force execution of partition iterator so Spark doesn't prune the task.
        _ = list(it)
        result = {
            "executor_host": socket.gethostname(),
            "pid_present": os.getpid() > 0,
        }
        for k in keys:
            result[f"has_{k}"] = bool(os.environ.get(k))
        yield Row(**result)

    rdd = spark.sparkContext.parallelize(range(partitions), partitions)
    return rdd.mapPartitions(_probe_partition).collect()


def main():
    print(f"[test] Starting Spark cluster-mode executor AuthManager test for user: {TEST_USERNAME}")

    all_passed = True
    spark = None
    try:
        print("  Getting Keycloak tokens...")
        access_token, refresh_token = get_keycloak_tokens(TEST_USERNAME, TEST_PASSWORD)
        if not refresh_token:
            print("  ✗ ERROR: No refresh_token returned - required for AuthManager broker session")
            return 1
        print("  ✓ Tokens obtained (access + refresh)")

        os.environ["POLARIS_USER_TOKEN"] = access_token
        os.environ["POLARIS_REFRESH_TOKEN"] = refresh_token
        os.environ["JUPYTERHUB_USER"] = TEST_USERNAME

        print("  Creating cluster-mode Spark session via AuthManager...")
        from teehr.evaluation.spark_session_utils import create_spark_session
        spark = create_spark_session(
            update_configs={
                "spark.kubernetes.executor.node.selector.teehr-hub/nodegroup-name": "spark-r5-4xlarge",
            },
            start_spark_cluster=True,
            use_authmanager=True,
            force_recreate_session=True,
            executor_instances=1,
            executor_cores=1,
            executor_memory="1g",
        )
        print("  ✓ Spark session created")

        print("  Probing executor environment for propagated Polaris auth vars...")
        rows = probe_executor_env(spark, EXPECTED_EXECUTOR_ENV_KEYS, partitions=4)
        if not rows:
            print("  ✗ ERROR: no executor probe results returned")
            all_passed = False
        for row in rows:
            print(f"    {row.asDict()}")
            for key in EXPECTED_EXECUTOR_ENV_KEYS:
                if not row[f"has_{key}"]:
                    print(f"    ✗ ERROR: executor {row['executor_host']} missing {key}")
                    all_passed = False

        # Real distributed write + read through the executors just probed,
        # exercising the same Iceberg/Polaris auth path end to end. This
        # table is created fresh and dropped at the end -- it never reads
        # from or depends on any pre-existing warehouse table/data.
        table = f"executor_authmanager_test_{int(time.time())}"
        full_table = f"{CATALOG}.{NAMESPACE}.{table}"

        n = 200_000
        parts = 4
        print(f"  Building {n}-row distributed dataset across {parts} partitions...")
        df = (
            spark.range(0, n)
            .repartition(parts)
            .withColumn("grp", (F.col("id") % 17).cast("int"))
            .withColumn("payload", F.concat(F.lit("v-"), F.col("id").cast("string")))
        )
        input_count = df.count()
        print(f"  input_count: {input_count}")
        if input_count != n:
            print(f"  ✗ ERROR: expected input_count={n}, got {input_count}")
            all_passed = False

        print(f"  Writing distributed table {full_table}...")
        df.writeTo(full_table).using("iceberg").create()

        read_df = spark.read.table(full_table).repartition(parts)
        table_count = read_df.count()
        print(f"  table_count: {table_count}")
        if table_count != n:
            print(f"  ✗ ERROR: expected table_count={n}, got {table_count}")
            all_passed = False

        group_count = read_df.groupBy("grp").count().count()
        if group_count != 17:
            print(f"  ✗ ERROR: expected 17 groups, got {group_count}")
            all_passed = False

        spark.sql(f"DROP TABLE {full_table}")
        print(f"  dropped: {full_table}")

    except Exception as e:
        print(f"  ✗ ERROR: {type(e).__name__}: {str(e)[:300]}")
        all_passed = False
    finally:
        if spark:
            try:
                spark.stop()
            except Exception:
                pass
        gc.collect()

    result = "PASSED" if all_passed else "FAILED"
    print(f"\n[test] Spark cluster-mode executor AuthManager test: {result}")
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
