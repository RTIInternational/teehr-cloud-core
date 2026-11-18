# Postgres client tools (same major version as the servers) plus the AWS CLI, for
# dumping databases to S3 and checking that the dumps restore.
FROM postgres:15-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends awscli \
    && rm -rf /var/lib/apt/lists/*

COPY backup.sh backup-all.sh restore-check.sh /usr/local/bin/
RUN chmod 0755 /usr/local/bin/backup.sh /usr/local/bin/backup-all.sh \
    /usr/local/bin/restore-check.sh
