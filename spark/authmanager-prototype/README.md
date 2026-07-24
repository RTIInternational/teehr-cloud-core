# Spark AuthManager Prototype

This directory contains a minimal broker-backed Iceberg REST AuthManager prototype intended for local validation.

## What this prototype does

- implements an Iceberg AuthManager class (`org.teehr.iceberg.auth.TeehrBrokerAuthManager`)
- requests short-lived Polaris access tokens from a broker endpoint
- caches tokens in-memory and refreshes before expiry
- injects `Authorization: Bearer <token>` into REST catalog requests

## Build

```bash
mvn -f spark/authmanager-prototype/pom.xml -DskipTests package
```

## Spark configuration sketch

Set the auth manager class as the Iceberg REST auth type:

```bash
--conf spark.sql.catalog.iceberg.rest.auth.type=org.teehr.iceberg.auth.TeehrBrokerAuthManager \
--conf spark.sql.catalog.iceberg.rest.auth.teehr.broker.url=http://teehr-token-broker.teehr.svc.cluster.local:8080/v1/polaris/token \
--conf spark.sql.catalog.iceberg.rest.auth.teehr.user-id=${POLARIS_USER_ID} \
--conf spark.sql.catalog.iceberg.rest.auth.teehr.session-id=${JUPYTERHUB_SERVER_NAME} \
--conf spark.sql.catalog.iceberg.rest.auth.teehr.realm=teehr \
--conf spark.sql.catalog.iceberg.rest.auth.teehr.catalog=iceberg \
--conf spark.sql.catalog.iceberg.rest.auth.teehr.audience=account \
--conf spark.sql.catalog.iceberg.rest.auth.teehr.subject-token-env=POLARIS_USER_TOKEN
```

## Prototype notes

- Keep this classpath-local to development images until broker authn/authz is production-ready.
- Do not log access tokens.
- Use broker-issued token TTL of 5 to 15 minutes with proactive refresh.
- The manager sends the subject access token to the broker in the Authorization header and requests an exchanged Polaris token.
- Provide a subject token via `rest.auth.teehr.subject-token` (testing only) or `rest.auth.teehr.subject-token-env`.
