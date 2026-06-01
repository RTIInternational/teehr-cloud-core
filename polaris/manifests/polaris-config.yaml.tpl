apiVersion: v1
kind: ConfigMap
metadata:
  name: polaris-config
data:
  application.properties: |
    # Persistence: PostgreSQL via JDBC
    polaris.persistence.type=relational-jdbc
    polaris.persistence.auto-bootstrap-types=relational-jdbc
    polaris.persistence.relational.jdbc.database-type=postgresql
    polaris.persistence.relational.jdbc.max-retries=5
    polaris.persistence.relational.jdbc.initial-delay-in-ms=100
    polaris.persistence.relational.jdbc.max-duration-in-ms=5000

    # Realm context
    polaris.realm-context.realms=teehr

    # Authentication: MIXED — internal token service available for bootstrap,
    # external JWT (Keycloak) accepted for runtime clients.
    polaris.authentication.type=mixed

    # Map JWT preferred_username → Polaris principal name
    polaris.oidc.principal-mapper.name-claim-path=preferred_username

    # Map Keycloak realm roles → Polaris PRINCIPAL_ROLE:<rolename>
    polaris.oidc.principal-roles-mapper.mappings[0].regex=(.+)
    polaris.oidc.principal-roles-mapper.mappings[0].replacement=PRINCIPAL_ROLE:$1

    # Quarkus OIDC: use Keycloak for JWT JWKS validation (bearer-only resource server)
    quarkus.oidc.application-type=service
    # auth-server-url is injected via environment variable per environment

    # Extract Keycloak realm roles from realm_access/roles JWT claim
    quarkus.oidc.roles.role-claim-path=realm_access/roles

    # Disable OTEL by default (enable separately if monitoring is wired)
    quarkus.otel.sdk.disabled=true

    # Allow S3 and MinIO storage types
    polaris.features."SUPPORTED_CATALOG_STORAGE_TYPES"=["S3","GCS","AZURE","FILE"]
    polaris.features."ALLOW_INSECURE_STORAGE_TYPES"=true
    polaris.readiness.ignore-severe-issues=true
