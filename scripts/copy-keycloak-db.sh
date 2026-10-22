#!/usr/bin/env bash
set -euo pipefail

# Copies the Keycloak Postgres databases (keycloak: users, groups, credentials;
# teehr_api: API keys, delegated sessions) from one cluster to another, e.g. for
# a region move (RTIInternational/teehr-hub#462). Both clusters must share the
# same secrets (API_KEY_HASH_SALT etc.) or copied API keys won't validate.
#
# Stops Keycloak and teehr-api on both clusters, streams pg_dump from the
# source pod into pg_restore on the destination (nothing touches disk unless
# BACKUP_DIR is set), compares row counts, then starts the destination again.
# The source stays stopped so it can't take writes that wouldn't be copied.
#
# Usage:
#   SRC_CONTEXT=<kube-context> DST_CONTEXT=<kube-context> ./scripts/copy-keycloak-db.sh
#
# Env:
#   NAMESPACE   default teehr-hub
#   BACKUP_DIR  also save each dump (custom format) here; delete it afterwards,
#               it holds user emails and password hashes
#   DRY_RUN=1   change nothing: show row counts on both sides and dump sizes

: "${SRC_CONTEXT:?set SRC_CONTEXT}"
: "${DST_CONTEXT:?set DST_CONTEXT}"
NAMESPACE="${NAMESPACE:-teehr-hub}"
BACKUP_DIR="${BACKUP_DIR:-}"
DRY_RUN="${DRY_RUN:-0}"
DATABASES=(keycloak teehr_api)

k() { local ctx="$1"; shift; kubectl --context "$ctx" -n "$NAMESPACE" "$@"; }

psql_count() {
  k "$1" exec deploy/keycloak-pg -- sh -c \
    "psql -U \"\$POSTGRES_USER\" -d $2 -tAc \"SELECT count(*) FROM $3\"" 2>/dev/null || echo "n/a"
}

counts() {
  echo "  users=$(psql_count "$1" keycloak user_entity)" \
    "groups=$(psql_count "$1" keycloak keycloak_group)" \
    "api_keys=$(psql_count "$1" teehr_api api_keys)" \
    "delegated_sessions=$(psql_count "$1" teehr_api delegated_sessions)"
}

stop_writers() {
  echo "Stopping Keycloak and teehr-api on $1"
  k "$1" patch keycloak keycloak --type merge -p '{"spec":{"instances":0}}' >/dev/null
  k "$1" scale deploy teehr-api --replicas=0 >/dev/null
  for _ in $(seq 1 60); do
    kc=$(k "$1" get statefulset keycloak -o jsonpath='{.status.replicas}' 2>/dev/null || true)
    api=$(k "$1" get deploy teehr-api -o jsonpath='{.status.replicas}' 2>/dev/null || true)
    if [[ "${kc:-0}" == "0" && "${api:-0}" == "0" ]]; then
      return
    fi
    sleep 5
  done
  echo "ERROR: Keycloak or teehr-api still running on $1" >&2
  exit 1
}

start_writers() {
  echo "Starting Keycloak and teehr-api on $1"
  k "$1" patch keycloak keycloak --type merge -p '{"spec":{"instances":1}}' >/dev/null
  k "$1" scale deploy teehr-api --replicas=1 >/dev/null
}

echo "Source:      $SRC_CONTEXT"
echo "Destination: $DST_CONTEXT"
echo "Source counts:";      counts "$SRC_CONTEXT"
echo "Destination counts:"; counts "$DST_CONTEXT"

if [[ "$DRY_RUN" == "1" ]]; then
  for db in "${DATABASES[@]}"; do
    bytes=$(k "$SRC_CONTEXT" exec deploy/keycloak-pg -- sh -c \
      "pg_dump -U \"\$POSTGRES_USER\" -Fc $db" | wc -c | tr -d ' ')
    echo "Dump of $db: $bytes bytes"
  done
  echo "Dry run: nothing changed."
  exit 0
fi

read -r -p "Replace the destination's Keycloak and API databases with the source's? [y/N] " answer
[[ "$answer" == "y" ]] || { echo "Aborted."; exit 1; }

before="$(counts "$SRC_CONTEXT")"
stop_writers "$SRC_CONTEXT"
stop_writers "$DST_CONTEXT"
# Re-read: writes may have landed between the first read and the stop.
before="$(counts "$SRC_CONTEXT")"

for db in "${DATABASES[@]}"; do
  echo "Copying $db"
  dump=(k "$SRC_CONTEXT" exec deploy/keycloak-pg -- sh -c "pg_dump -U \"\$POSTGRES_USER\" -Fc $db")
  restore=(k "$DST_CONTEXT" exec -i deploy/keycloak-pg -- sh -c
    "pg_restore -U \"\$POSTGRES_USER\" -d $db --clean --if-exists --single-transaction --exit-on-error")
  if [[ -n "$BACKUP_DIR" ]]; then
    mkdir -p "$BACKUP_DIR"
    "${dump[@]}" | tee "$BACKUP_DIR/$db.dump" | "${restore[@]}"
  else
    "${dump[@]}" | "${restore[@]}"
  fi
done

after="$(counts "$DST_CONTEXT")"
echo "Source (at stop): $before"
echo "Destination now:  $after"
if [[ "$before" != "$after" ]]; then
  echo "ERROR: row counts differ; destination left stopped for inspection." >&2
  exit 1
fi

start_writers "$DST_CONTEXT"
echo ""
echo "Done. The source is still stopped. To roll back to it:"
echo "  kubectl --context $SRC_CONTEXT -n $NAMESPACE patch keycloak keycloak --type merge -p '{\"spec\":{\"instances\":1}}'"
echo "  kubectl --context $SRC_CONTEXT -n $NAMESPACE scale deploy teehr-api --replicas=1"
