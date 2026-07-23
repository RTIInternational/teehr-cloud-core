#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-teehr-hub}"
POD_NAME="${POD_NAME:-}"
CONTAINER_NAME="${CONTAINER_NAME:-notebook}"
SCRIPT_PATH="${SCRIPT_PATH:-examples/developer/polaris_spark_namespace_table_example.py}"
SETUP_UTILS_PATH="${SETUP_UTILS_PATH:-examples/developer/setup_utils.py}"
EXTRA_ARGS="${EXTRA_ARGS:-}"
POLARIS_TEST_USERNAME="${POLARIS_TEST_USERNAME:-admin}"
POLARIS_TEST_PASSWORD="${POLARIS_TEST_PASSWORD:-admin}"
POLARIS_OAUTH_CLIENT_ID="${POLARIS_OAUTH_CLIENT_ID:-jupyterhub}"
DEBUG_POLARIS="${DEBUG_POLARIS:-0}"
POLARIS_DOCTOR="${POLARIS_DOCTOR:-0}"

if [[ -z "$POD_NAME" ]]; then
  POD_NAME="$(kubectl -n "$NAMESPACE" get pods -o name | sed 's#^pod/##' | grep -E '^jupyter-' | head -n1 || true)"
fi

if [[ -z "$POD_NAME" ]]; then
  echo "No Jupyter single-user pod found in namespace '$NAMESPACE'." >&2
  echo "Start a notebook server first, or set POD_NAME explicitly." >&2
  exit 1
fi

if [[ -z "${POLARIS_OAUTH_CLIENT_SECRET:-}" ]]; then
  POLARIS_OAUTH_CLIENT_SECRET="$(kubectl -n "$NAMESPACE" get secret jupyterhub -o jsonpath='{.data.OAUTH_CLIENT_SECRET}' | base64 --decode)"
fi

if [[ ! -f "$SCRIPT_PATH" ]]; then
  echo "Script not found: $SCRIPT_PATH" >&2
  exit 1
fi

if [[ ! -f "$SETUP_UTILS_PATH" ]]; then
  echo "Setup utils not found: $SETUP_UTILS_PATH" >&2
  exit 1
fi

echo "Running $SCRIPT_PATH in pod $POD_NAME (namespace: $NAMESPACE, container: $CONTAINER_NAME)"

start_epoch="$(date +%s)"

diagnostics_dir=""
run_log=""
if [[ "$POLARIS_DOCTOR" == "1" ]]; then
  diagnostics_dir="${TMPDIR:-/tmp}/polaris-doctor-$(date +%Y%m%d-%H%M%S)-$$"
  mkdir -p "$diagnostics_dir"
  run_log="$diagnostics_dir/run.log"
  echo "[doctor] diagnostics dir: $diagnostics_dir"
fi

tmpdir="${TMPDIR:-/tmp}/polaris-spark-example.$$"
mkdir -p "$tmpdir"
cp "$SCRIPT_PATH" "$tmpdir/polaris_spark_namespace_table_example.py"
cp "$SETUP_UTILS_PATH" "$tmpdir/setup_utils.py"

set +e
if [[ "$POLARIS_DOCTOR" == "1" ]]; then
  (
    tar -C "$tmpdir" -cf - polaris_spark_namespace_table_example.py setup_utils.py | \
    kubectl -n "$NAMESPACE" exec -i "$POD_NAME" -c "$CONTAINER_NAME" -- sh -lc \
      "mkdir -p /tmp/polaris-spark-example && cd /tmp/polaris-spark-example && tar -xf - && export PYTHONPATH=/tmp/polaris-spark-example:\$PYTHONPATH POLARIS_TEST_USERNAME='$POLARIS_TEST_USERNAME' POLARIS_TEST_PASSWORD='$POLARIS_TEST_PASSWORD' POLARIS_OAUTH_CLIENT_ID='$POLARIS_OAUTH_CLIENT_ID' POLARIS_OAUTH_CLIENT_SECRET='$POLARIS_OAUTH_CLIENT_SECRET'; python /tmp/polaris-spark-example/polaris_spark_namespace_table_example.py $EXTRA_ARGS"
  ) 2>&1 | tee "$run_log"
  cmd_exit_code=${PIPESTATUS[0]}
else
  tar -C "$tmpdir" -cf - polaris_spark_namespace_table_example.py setup_utils.py | \
  kubectl -n "$NAMESPACE" exec -i "$POD_NAME" -c "$CONTAINER_NAME" -- sh -lc \
    "mkdir -p /tmp/polaris-spark-example && cd /tmp/polaris-spark-example && tar -xf - && export PYTHONPATH=/tmp/polaris-spark-example:\$PYTHONPATH POLARIS_TEST_USERNAME='$POLARIS_TEST_USERNAME' POLARIS_TEST_PASSWORD='$POLARIS_TEST_PASSWORD' POLARIS_OAUTH_CLIENT_ID='$POLARIS_OAUTH_CLIENT_ID' POLARIS_OAUTH_CLIENT_SECRET='$POLARIS_OAUTH_CLIENT_SECRET'; python /tmp/polaris-spark-example/polaris_spark_namespace_table_example.py $EXTRA_ARGS"
  cmd_exit_code=$?
fi
set -e

if [[ "$POLARIS_DOCTOR" == "1" ]]; then
  {
    echo "exit_code=$cmd_exit_code"
    echo "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "namespace=$NAMESPACE"
    echo "pod_name=$POD_NAME"
    echo "container_name=$CONTAINER_NAME"
    echo "script_path=$SCRIPT_PATH"
  } > "$diagnostics_dir/context.txt"
fi

if [[ "$DEBUG_POLARIS" == "1" ]]; then
  now_epoch="$(date +%s)"
  since_seconds="$((now_epoch - start_epoch + 15))"
  if (( since_seconds < 30 )); then
    since_seconds=30
  fi
  echo
  echo "[debug] Polaris logs for the last ${since_seconds}s"
  kubectl -n "$NAMESPACE" logs deploy/polaris --since="${since_seconds}s" | \
    grep -E "(POST /api/catalog/v1/oauth/tokens|GET /api/catalog/v1/config|HTTP/1.1\" 401|HTTP/1.1\" 200|principal=|roles=|Some principal roles were not found)" || true

  if [[ "$POLARIS_DOCTOR" == "1" ]]; then
    kubectl -n "$NAMESPACE" logs deploy/polaris --since="${since_seconds}s" > "$diagnostics_dir/polaris.log" || true
    grep -E "(POST /api/catalog/v1/oauth/tokens|/api/catalog/v1/config|/api/catalog/v1/.*/namespaces|HTTP/1.1\" 401|HTTP/1.1\" 403|HTTP/1.1\" 500|Some principal roles were not found|UnknownHostException|warehouse.minio)" "$diagnostics_dir/polaris.log" > "$diagnostics_dir/polaris-summary.log" || true
  fi
fi

if [[ "$POLARIS_DOCTOR" == "1" ]]; then
  diagnosis="unknown"
  if [[ "$cmd_exit_code" -eq 0 ]]; then
    diagnosis="success"
  elif grep -qi "NotAuthorizedException\|HTTP Error 401\|401 Unauthorized" "$run_log" 2>/dev/null; then
    diagnosis="authz_or_realm_mismatch"
  elif grep -qi "UnknownHostException\|warehouse\.minio\|NoSuchBucket\|AccessDenied" "$run_log" 2>/dev/null; then
    diagnosis="storage_or_warehouse_misconfig"
  elif grep -qi "Failed to write to grant records\|grant_records_pkey\|duplicate key value" "$run_log" 2>/dev/null; then
    diagnosis="principal_role_grant_idempotency"
  fi

  {
    echo "diagnosis=$diagnosis"
    if [[ "$diagnosis" == "authz_or_realm_mismatch" ]]; then
      echo "hint=Verify token issuer/realm and Polaris principal-role grants for this principal"
    elif [[ "$diagnosis" == "storage_or_warehouse_misconfig" ]]; then
      echo "hint=Verify Polaris catalog warehouse and MinIO endpoint/path-style settings"
    elif [[ "$diagnosis" == "principal_role_grant_idempotency" ]]; then
      echo "hint=Principal sync is re-granting existing role; ensure duplicate grant is treated as success"
    fi
  } > "$diagnostics_dir/diagnosis.txt"

  echo
  echo "[doctor] diagnosis: $diagnosis"
  echo "[doctor] bundle: $diagnostics_dir"
  if [[ -f "$diagnostics_dir/polaris-summary.log" ]]; then
    echo "[doctor] summary:"
    tail -n 60 "$diagnostics_dir/polaris-summary.log" || true
  fi
fi

exit "$cmd_exit_code"
