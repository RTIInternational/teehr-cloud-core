#!/usr/bin/env bash
set -euo pipefail

# Exports Keycloak realm users to CSV (e.g. for user announcement emails).
# Read-only: reads the admin secret, port-forwards to Keycloak, and calls the
# Admin REST API with GETs.
#
# Usage:
#   CONTEXT=<kube-context> ./scripts/export-keycloak-users.sh > users.csv
#
# Env:
#   CONTEXT         kube context (default: current context)
#   NAMESPACE       namespace Keycloak runs in (default: teehr-hub)
#   REALM           realm to export (default: teehr)
#   INCLUDE_GROUPS  "true" to add a groups column (one extra request per user)
#   GROUP_FILTER    comma-separated group names; only users in at least one are
#                   listed (implies INCLUDE_GROUPS), e.g. jupyter-user,jupyter-admin.
#                   Matches direct membership only, not parent/subgroups.
#   ALL_USERS       "true" to include disabled users and users without email
#   LOCAL_PORT      local port for the port-forward (default: 18080)

NAMESPACE="${NAMESPACE:-teehr-hub}"
REALM="${REALM:-teehr}"
INCLUDE_GROUPS="${INCLUDE_GROUPS:-false}"
GROUP_FILTER="${GROUP_FILTER:-}"
ALL_USERS="${ALL_USERS:-false}"
LOCAL_PORT="${LOCAL_PORT:-18080}"
PAGE_SIZE=100

if [[ -n "$GROUP_FILTER" ]]; then
  INCLUDE_GROUPS=true
fi

for cmd in kubectl curl jq; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "ERROR: Required command not found: $cmd" >&2
    exit 1
  fi
done

kube=(kubectl -n "$NAMESPACE")
if [[ -n "${CONTEXT:-}" ]]; then
  kube+=(--context "$CONTEXT")
fi

secret_value() {
  "${kube[@]}" get secret keycloak-admin-secrets -o "jsonpath={.data.$1}" | base64 --decode
}
admin_user="$(secret_value username)"
admin_password="$(secret_value password)"

"${kube[@]}" port-forward svc/keycloak-service "$LOCAL_PORT:8080" >/dev/null 2>&1 &
pf_pid=$!
trap 'kill "$pf_pid" 2>/dev/null || true' EXIT

base_url="http://localhost:$LOCAL_PORT"
for _ in $(seq 1 30); do
  if curl -s -o /dev/null "$base_url/realms/master"; then
    break
  fi
  sleep 1
done

token="$(curl -sf "$base_url/realms/master/protocol/openid-connect/token" \
  --data-urlencode grant_type=password \
  --data-urlencode client_id=admin-cli \
  --data-urlencode "username=$admin_user" \
  --data-urlencode "password=$admin_password" | jq -r .access_token)"

api_get() {
  curl -sf -H "Authorization: Bearer $token" "$base_url/admin/realms/$REALM$1"
}

if [[ "$INCLUDE_GROUPS" == "true" ]]; then
  echo "username,email,firstName,lastName,enabled,groups"
else
  echo "username,email,firstName,lastName,enabled"
fi

first=0
while :; do
  page="$(api_get "/users?first=$first&max=$PAGE_SIZE&briefRepresentation=true")"
  count="$(jq length <<<"$page")"
  [[ "$count" -eq 0 ]] && break

  if [[ "$ALL_USERS" != "true" ]]; then
    page="$(jq '[.[] | select(.enabled and (.email // "") != "")]' <<<"$page")"
  fi

  if [[ "$INCLUDE_GROUPS" == "true" ]]; then
    jq -c '.[]' <<<"$page" | while read -r user; do
      id="$(jq -r .id <<<"$user")"
      groups="$(api_get "/users/$id/groups" | jq -r '[.[].name] | join(";")')"
      if [[ -n "$GROUP_FILTER" ]] && ! jq -en --arg have "$groups" --arg want "$GROUP_FILTER" \
        '($have | split(";")) as $h | any($want | split(",")[]; . as $w | $h | index($w))' >/dev/null; then
        continue
      fi
      jq -r --arg groups "$groups" \
        '[.username, .email, .firstName, .lastName, .enabled, $groups] | @csv' <<<"$user"
    done
  else
    jq -r '.[] | [.username, .email, .firstName, .lastName, .enabled] | @csv' <<<"$page"
  fi

  first=$((first + PAGE_SIZE))
done
