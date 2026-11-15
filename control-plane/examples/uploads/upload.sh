#!/usr/bin/env bash
# Usage: ./upload.sh <project> <file> [username]
# Creates an upload, PUTs the file to its URL, inspects it, prints the result.
# Local KinD users have password = username (user, admin, poweruser, noaccess).
set -uo pipefail
H=teehr.local.app.garden
P=$1; F=$2; USER_NAME=${3:-user}
BODY=$(mktemp); trap 'rm -f "$BODY"' EXIT

fail() { echo "✗ $1"; [ -s "$BODY" ] && { cat "$BODY"; echo; }; exit 1; }
field() { python3 -c "import sys,json;print(json.load(open(sys.argv[1]))[sys.argv[2]])" "$BODY" "$1"; }
call() {  # call <what> <expected status> curl-args... ; the body lands in $BODY
  local what=$1 want=$2; shift 2
  local code
  : > "$BODY"
  code=$(curl -sk -o "$BODY" -w "%{http_code}" "$@") || fail "$what: can't connect (is the host in /etc/hosts? see README)"
  [ "$code" = "$want" ] || fail "$what: HTTP $code"
}

[ -f "$F" ] || fail "no file $F"
call "sign in as $USER_NAME" 200 https://auth.$H/realms/teehr/protocol/openid-connect/token \
  -d grant_type=password -d client_id=jupyterhub -d client_secret=local-jupyterhub-client-secret \
  -d username="$USER_NAME" -d password="$USER_NAME"
TOKEN=$(field access_token)
AUTH=(-H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json")
CP=https://control-plane.$H/v1/projects/$P

call "create the upload in $P" 201 "${AUTH[@]}" -X POST "$CP/uploads" \
  -d "{\"filename\":\"$(basename "$F")\",\"kind\":\"timeseries\"}"
ID=$(field id); URL=$(field upload_url)
echo "✓ upload $ID"
call "PUT the file to staging (s3.$H)" 200 -X PUT --upload-file "$F" "$URL"
echo "✓ file staged"
call "inspect" 200 "${AUTH[@]}" -X POST "$CP/uploads/$ID/inspect"
echo "✓ inspected"
python3 -m json.tool "$BODY"
