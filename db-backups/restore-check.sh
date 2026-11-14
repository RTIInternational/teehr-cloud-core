#!/usr/bin/env bash
# Prove the latest dump of each database restores.
#
# For each name in BACKUP_NAMES, downloads the newest dump under
# s3://$S3_BUCKET/$S3_PREFIX/<name>/, restores it into a throwaway Postgres started
# inside this container (the live databases are never written to), and checks it
# has tables. If <NAME>_PGHOST etc. are set (e.g. KEYCLOAK_PGHOST), the restored
# table count must also match the live database's.
#
# Env: BACKUP_NAMES ("app keycloak polaris prefect"), S3_BUCKET, S3_PREFIX,
#      S3_ENDPOINT, AWS credentials; optional <NAME>_PG{HOST,PORT,DATABASE,USER,PASSWORD}.
set -euo pipefail

: "${BACKUP_NAMES:?}" "${S3_BUCKET:?}"
prefix="${S3_PREFIX:-postgres}"
aws_s3=(aws s3)
[[ -n "${S3_ENDPOINT:-}" ]] && aws_s3=(aws --endpoint-url "$S3_ENDPOINT" s3)

COUNT_SQL="select count(*) from information_schema.tables
           where table_schema not in ('pg_catalog', 'information_schema')"

work="$(mktemp -d)"
chown postgres "$work"
gosu postgres initdb -D "$work/pg" --auth=trust >/dev/null
gosu postgres pg_ctl -D "$work/pg" -o "-c listen_addresses='' -k $work" -w start >/dev/null
trap 'gosu postgres pg_ctl -D "$work/pg" -m fast stop >/dev/null || true' EXIT
scratch=(psql -h "$work" -U postgres -v ON_ERROR_STOP=1 -tA)

failed=0
for name in $BACKUP_NAMES; do
    latest="$("${aws_s3[@]}" ls --recursive "s3://$S3_BUCKET/$prefix/$name/" \
        | awk '{print $4}' | sort | tail -n 1)"
    if [[ -z "$latest" ]]; then
        echo "FAIL $name: no dump under s3://$S3_BUCKET/$prefix/$name/"
        failed=1
        continue
    fi
    dump="$work/$name.dump"
    "${aws_s3[@]}" cp "s3://$S3_BUCKET/$latest" "$dump" --only-show-errors
    chown postgres "$dump"

    "${scratch[@]}" -c "create database restore_$name" >/dev/null
    if ! gosu postgres pg_restore -h "$work" -U postgres --no-owner --no-acl \
            --exit-on-error -d "restore_$name" "$dump"; then
        echo "FAIL $name: pg_restore failed for $latest"
        failed=1
        continue
    fi
    restored="$("${scratch[@]}" -d "restore_$name" -c "$COUNT_SQL")"

    upper="${name^^}"
    host_var="${upper}_PGHOST"
    if [[ -n "${!host_var:-}" ]]; then
        var() { local v="${upper}_$1"; echo "${!v:-${2:-}}"; }
        live="$(PGHOST="$(var PGHOST)" PGPORT="$(var PGPORT 5432)" \
                PGDATABASE="$(var PGDATABASE)" PGUSER="$(var PGUSER)" \
                PGPASSWORD="$(var PGPASSWORD)" psql -tA -c "$COUNT_SQL")"
    else
        live=""
    fi

    if [[ "$restored" -le 0 ]]; then
        echo "FAIL $name: restored $latest but it has no tables"
        failed=1
    elif [[ -n "$live" && "$restored" != "$live" ]]; then
        echo "FAIL $name: restored $restored tables, live database has $live"
        failed=1
    else
        echo "PASS $name: $latest restores, $restored tables${live:+ (live: $live)}"
    fi
done
exit "$failed"
