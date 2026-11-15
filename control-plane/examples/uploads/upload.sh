#!/usr/bin/env bash
# Usage: ./upload.sh <project> <file> [username]
# Creates an upload, PUTs the file to its URL, inspects it, prints the result.
set -euo pipefail
H=teehr.local.app.garden
P=$1; F=$2; USER_NAME=${3:-user}
json() { python3 -c "import sys,json;d=json.load(sys.stdin);$1"; }
TOKEN=$(curl -sk https://auth.$H/realms/teehr/protocol/openid-connect/token \
  -d grant_type=password -d client_id=jupyterhub -d client_secret=local-jupyterhub-client-secret \
  -d username="$USER_NAME" -d password="$USER_NAME" | json 'print(d["access_token"])')
AUTH=(-H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json")
CP=https://control-plane.$H/v1/projects/$P

U=$(curl -sk "${AUTH[@]}" -X POST "$CP/uploads" -d "{\"filename\":\"$(basename "$F")\",\"kind\":\"timeseries\"}")
ID=$(echo "$U" | json 'print(d["id"])')
URL=$(echo "$U" | json 'print(d["upload_url"])')
echo "upload $ID"
curl -sk -o /dev/null -w "PUT %{http_code}\n" -X PUT --upload-file "$F" "$URL"
curl -sk "${AUTH[@]}" -X POST "$CP/uploads/$ID/inspect" | python3 -m json.tool
