#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-teehr-hub}"
POD_NAME="${POD_NAME:-}"
CONTAINER_NAME="${CONTAINER_NAME:-notebook}"
BROKER_URL="${BROKER_URL:-http://teehr-api:8000/auth/polaris-token}"
REQUESTED_TTL_SECONDS="${REQUESTED_TTL_SECONDS:-600}"
AUDIENCE="${AUDIENCE:-account}"

if [[ -z "$POD_NAME" ]]; then
  POD_NAME="$(kubectl -n "$NAMESPACE" get pods -o name | sed 's#^pod/##' | grep -E '^jupyter-' | head -n1 || true)"
fi

if [[ -z "$POD_NAME" ]]; then
  echo "No Jupyter single-user pod found in namespace '$NAMESPACE'." >&2
  exit 1
fi

echo "Testing broker token endpoint from pod $POD_NAME"

kubectl -n "$NAMESPACE" exec -i "$POD_NAME" -c "$CONTAINER_NAME" -- env \
  BROKER_URL="$BROKER_URL" \
  REQUESTED_TTL_SECONDS="$REQUESTED_TTL_SECONDS" \
  AUDIENCE="$AUDIENCE" \
  python - <<'PY'
import json
import os
import requests

def get_fresh_subject_token() -> str:
  token_endpoint = os.getenv(
    "POLARIS_OAUTH2_SERVER_URI",
    "http://keycloak-service:8080/realms/teehr/protocol/openid-connect/token",
  )
  client_id = os.getenv("POLARIS_CLIENT_ID", "jupyterhub")
  client_secret = os.getenv("POLARIS_CLIENT_SECRET", "")

  refresh_token = os.getenv("POLARIS_REFRESH_TOKEN", "")
  if refresh_token:
    payload = {
      "grant_type": "refresh_token",
      "client_id": client_id,
      "refresh_token": refresh_token,
    }
    if client_secret:
      payload["client_secret"] = client_secret
    resp = requests.post(token_endpoint, data=payload, timeout=20)
    if resp.status_code < 400:
      token = resp.json().get("access_token")
      if token:
        return token

  token = os.getenv("POLARIS_USER_TOKEN", "")
  if token:
    return token

  raise RuntimeError("Unable to obtain a usable subject token from pod environment")


token = get_fresh_subject_token()

realm = os.getenv("POLARIS_DEFAULT_REALM", "teehr")
user_id = os.getenv("JUPYTERHUB_USER", "admin")
session_id = (os.getenv("JUPYTERHUB_SERVER_NAME") or "").strip() or user_id

response = requests.post(
    os.environ["BROKER_URL"],
    headers={"Authorization": f"Bearer {token}"},
    json={
        "user_id": user_id,
        "session_id": session_id,
        "realm": realm,
        "catalog": "iceberg",
        "requested_ttl_seconds": int(os.environ["REQUESTED_TTL_SECONDS"]),
        "audience": os.environ["AUDIENCE"],
    },
    timeout=20,
)

print("status:", response.status_code)
try:
    payload = response.json()
except ValueError:
    print(response.text[:1500])
    raise

if response.status_code >= 400:
    print(json.dumps(payload, indent=2)[:2000])
    raise RuntimeError("broker token call failed")

print("trace_id:", payload.get("trace_id"))
print("token_type:", payload.get("token_type"))
print("expires_in_seconds:", payload.get("expires_in_seconds"))
print("issued_for:", payload.get("issued_for"))
print("access_token_prefix:", str(payload.get("access_token", ""))[:24])
PY
