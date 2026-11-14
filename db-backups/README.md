# db-backups

Nightly `pg_dump` of each in-cluster Postgres database (`app`, `keycloak`,
`polaris`, `prefect`) to S3, as custom-format archives (`pg_restore`-able).

- One CronJob per database (`db-backup-<name>`), 06:17 UTC by default.
- Objects: `s3://<bucket>/postgres/<name>/YYYY/MM/DD/<name>-<UTC>.dump`.
- Off on a deployment until it sets `dbBackups` in its project variables (see
  `garden.yaml`). On AWS the `db-backup` service account uses a write-only IRSA
  role, and the bucket is versioned with retention (teehr-cloud-platform
  `db-backups.tf`). Locally it writes to the RustFS `db-backups` bucket.

## Check that backups restore

```bash
garden test db-backups-restore
```

This dumps every database, restores each dump into a throwaway Postgres inside the
test pod, and checks the table count matches the live database. The live databases
are only read.

## Restore a database

Restoring replaces live data. Stop whatever writes to the database first (scale
its service to zero through Garden), and take a fresh dump before you start.

1. Find the dump: `aws s3 ls --recursive s3://<bucket>/postgres/<name>/ | sort | tail`
2. Download it: `aws s3 cp s3://<bucket>/<key> <name>.dump`
3. Restore it into the database's server, from a pod or port-forward with that
   database's credentials:
   `pg_restore --clean --if-exists --no-owner -d <database> <name>.dump`
4. Start the service again and check it.

On AWS, use your own credentials for steps 1 and 2: the backup role can't read.
