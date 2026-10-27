#!/usr/bin/env bash
set -euo pipefail

# Adds every realm user to a Keycloak group. defaultGroups only applies to new
# users, so use this to backfill existing users when a default group is added,
# e.g. /icechunk-read-only before deploying icechunk access rules with a
# default (docs/icechunk-access-control.md). Reads the admin secret,
# port-forwards to Keycloak, and calls the Admin REST API. Users already in
# the group are skipped, so it's safe to re-run. Don't run it on local kind:
# it would add the noaccess test user and break the access tests.
#
# Usage:
#   CONTEXT=<kube-context> DRY_RUN=1 ./scripts/add-users-to-group.sh
#   CONTEXT=<kube-context> ./scripts/add-users-to-group.sh
#
# Env:
#   CONTEXT     kube context (required)
#   NAMESPACE   namespace Keycloak runs in (default: teehr-hub)
#   REALM       realm to update (default: teehr)
#   GROUP       group path without the leading slash (default: icechunk-read-only)
#   DRY_RUN=1   change nothing: list the users that would be added
#   LOCAL_PORT  local port for the port-forward (default: 18080)

: "${CONTEXT:?set CONTEXT}"
NAMESPACE="${NAMESPACE:-teehr-hub}"
REALM="${REALM:-teehr}"
GROUP="${GROUP:-icechunk-read-only}"
DRY_RUN="${DRY_RUN:-0}"
LOCAL_PORT="${LOCAL_PORT:-18080}"
PAGE_SIZE=100

for cmd in kubectl curl jq; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "ERROR: Required command not found: $cmd" >&2
    exit 1
  fi
done

kube=(kubectl --context "$CONTEXT" -n "$NAMESPACE")
echo "Context: $CONTEXT  Realm: $REALM  Group: /$GROUP"

secret_value() {
  "${kube[@]}" get secret keycloak-admin-secrets -o "jsonpath={.data.$1}" | base64 --decode
}
admin_user="$(secret_value username)"
admin_password="$(secret_value password)"

members="$(mktemp)"
"${kube[@]}" port-forward svc/keycloak-service "$LOCAL_PORT:8080" >/dev/null 2>&1 &
pf_pid=$!
trap 'kill "$pf_pid" 2>/dev/null || true; rm -f "$members"' EXIT

base_url="http://localhost:$LOCAL_PORT"
for _ in $(seq 1 30); do
  if curl -s -o /dev/null "$base_url/realms/master"; then
    break
  fi
  sleep 1
done

# admin-cli tokens are short-lived, so get a new one for each page.
refresh_token() {
  token="$(curl -sf "$base_url/realms/master/protocol/openid-connect/token" \
    --data-urlencode grant_type=password \
    --data-urlencode client_id=admin-cli \
    --data-urlencode "username=$admin_user" \
    --data-urlencode "password=$admin_password" | jq -r .access_token)" || {
    echo "ERROR: Couldn't get an admin token from Keycloak" >&2
    exit 1
  }
}

api() {
  curl -sf -H "Authorization: Bearer $token" "$@"
}

refresh_token
group_id="$(api "$base_url/admin/realms/$REALM/group-by-path/$GROUP" | jq -r .id)" || {
  echo "ERROR: Group /$GROUP not found in realm $REALM" >&2
  exit 1
}

first=0
while :; do
  page="$(api "$base_url/admin/realms/$REALM/groups/$group_id/members?first=$first&max=$PAGE_SIZE&briefRepresentation=true")"
  [[ "$(jq length <<<"$page")" -eq 0 ]] && break
  jq -r '.[].id' <<<"$page" >>"$members"
  first=$((first + PAGE_SIZE))
done

total=0
added=0
first=0
while :; do
  refresh_token
  page="$(api "$base_url/admin/realms/$REALM/users?first=$first&max=$PAGE_SIZE&briefRepresentation=true")"
  count="$(jq length <<<"$page")"
  [[ "$count" -eq 0 ]] && break
  total=$((total + count))

  while read -r id username; do
    grep -qx "$id" "$members" && continue
    if [[ "$DRY_RUN" == "1" ]]; then
      echo "Would add $username"
    else
      api -X PUT -o /dev/null "$base_url/admin/realms/$REALM/users/$id/groups/$group_id" || {
        echo "ERROR: Couldn't add $username; re-run to continue" >&2
        exit 1
      }
      echo "Added $username"
    fi
    added=$((added + 1))
  done < <(jq -r '.[] | "\(.id) \(.username)"' <<<"$page")

  first=$((first + PAGE_SIZE))
done

if [[ "$DRY_RUN" == "1" ]]; then
  echo "Dry run: $added of $total users would be added to /$GROUP."
else
  echo "Added $added of $total users to /$GROUP."
fi
