#!/usr/bin/env bash
# Run backup.sh for every name in BACKUP_NAMES, taking each database's connection
# from <NAME>_PG{HOST,PORT,DATABASE,USER,PASSWORD} (e.g. KEYCLOAK_PGHOST).
# Used by the restore test; the CronJobs run backup.sh once per database.
set -euo pipefail
: "${BACKUP_NAMES:?}"
for name in $BACKUP_NAMES; do
    upper="${name^^}"
    var() { local v="${upper}_$1"; echo "${!v:-${2:-}}"; }
    BACKUP_NAME="$name" \
    PGHOST="$(var PGHOST)" \
    PGPORT="$(var PGPORT 5432)" \
    PGDATABASE="$(var PGDATABASE)" \
    PGUSER="$(var PGUSER)" \
    PGPASSWORD="$(var PGPASSWORD)" \
        backup.sh
done
