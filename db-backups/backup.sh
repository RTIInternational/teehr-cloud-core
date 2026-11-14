#!/usr/bin/env bash
# Dump one Postgres database to S3 as a custom-format archive (pg_restore-able).
#
# Streams pg_dump straight to S3, so nothing is staged on disk.
#
# Env:
#   BACKUP_NAME   short name, used in the object key (e.g. keycloak)
#   PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD   standard libpq variables
#   S3_BUCKET     destination bucket
#   S3_PREFIX     key prefix (default: postgres)
#   S3_ENDPOINT   S3 endpoint URL for non-AWS storage (RustFS on local); unset on AWS
#   AWS credentials: static keys locally, IRSA (web identity) on AWS
#
# Writes s3://$S3_BUCKET/$S3_PREFIX/$BACKUP_NAME/YYYY/MM/DD/$BACKUP_NAME-<UTC>.dump
set -euo pipefail

: "${BACKUP_NAME:?}" "${PGDATABASE:?}" "${S3_BUCKET:?}"
prefix="${S3_PREFIX:-postgres}"
aws_s3=(aws s3)
[[ -n "${S3_ENDPOINT:-}" ]] && aws_s3=(aws --endpoint-url "$S3_ENDPOINT" s3)

now="$(date -u +%Y%m%dT%H%M%SZ)"
key="$prefix/$BACKUP_NAME/$(date -u +%Y/%m/%d)/$BACKUP_NAME-$now.dump"
target="s3://$S3_BUCKET/$key"

echo "[db-backup] $BACKUP_NAME: dumping $PGDATABASE@$PGHOST to $target"
# Streaming uploads need a size hint above ~50 GB; 200 GB covers these databases.
pg_dump --format=custom --compress=6 --no-password \
    | "${aws_s3[@]}" cp - "$target" --expected-size 200000000000 --only-show-errors

size="$("${aws_s3[@]}" ls "$target" | awk '{print $3}')"
if [[ -z "$size" || "$size" -le 0 ]]; then
    echo "[db-backup] $BACKUP_NAME: upload missing or empty: $target" >&2
    exit 1
fi
echo "[db-backup] $BACKUP_NAME: done, $size bytes"
