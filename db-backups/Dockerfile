# Postgres client tools (same major version as the servers) plus the AWS CLI, for
# dumping databases to S3 and checking that the dumps restore.
#
# The CLI is copied from its own image rather than apt-installed, so the build
# fetches nothing from package mirrors: this base image's apt sources are plain
# HTTP, which is blocked on some deployments, and it lacks ca-certificates, so
# they can't simply be switched to HTTPS. Keep the tag in step with
# local-s3/manifests/create-bucket.yaml.
FROM amazon/aws-cli:2.36.45 AS awscli

FROM postgres:15-bookworm

COPY --from=awscli /usr/local/aws-cli /usr/local/aws-cli
RUN ln -s /usr/local/aws-cli/v2/current/bin/aws /usr/local/bin/aws

COPY backup.sh backup-all.sh restore-check.sh /usr/local/bin/
RUN chmod 0755 /usr/local/bin/backup.sh /usr/local/bin/backup-all.sh \
    /usr/local/bin/restore-check.sh
