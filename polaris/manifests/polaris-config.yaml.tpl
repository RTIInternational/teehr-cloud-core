apiVersion: v1
kind: ConfigMap
metadata:
  name: polaris-config
data:
  application.properties: |
    # Persistence: PostgreSQL Configuration
    polaris.persistence.type=relational-jdbc
    polaris.persistence.relational.jdbc.database-type=postgresql

    quarkus.datasource.db-kind=postgresql
    quarkus.datasource.jdbc.url=jdbc:postgresql://polaris-pg:5432/polaris
    quarkus.datasource.username=polaris

    polaris.persistence.relational.jdbc.max-retries=5
    polaris.persistence.relational.jdbc.initial-delay-in-ms=100
    polaris.persistence.relational.jdbc.max-duration-in-ms=5000

    # Authentication Context Configuration
    # This realm is configured to use an external identity provider (IDP) for authentication only.
    #  It accepts tokens issued by Keycloak only.
    # polaris.authentication.type=external
    # This realm is configured to use both the internal and external authentication.
    #  It accepts tokens issued by both Polaris and Keycloak.
    polaris.authentication.type=mixed
    # These are global. You can also set per realm like:
    # polaris.authentication.realm1.type=external
    polaris.oidc.principal-mapper.name-claim-path=preferred_username

    # Quarkus OIDC — tenant-enabled=true is required; without it Quarkus disables the
    # Default tenant and rejects all Bearer tokens with 401 regardless of other config.
    quarkus.oidc.tenant-enabled=true
    quarkus.oidc.application-type=service
    quarkus.oidc.client-id=jupyterhub
    quarkus.tls.trust-all=true
    quarkus.oidc.connection-delay=PT10S
    quarkus.oidc.connection-retry-count=5
    quarkus.oidc.token.audience=account
    quarkus.oidc.token.issuer=any
    quarkus.oidc.roles.role-claim-path=realm_access/roles
    polaris.oidc.principal-roles-mapper.type=default
    polaris.oidc.principal-roles-mapper.mappings[0].regex=^(.*)$
    polaris.oidc.principal-roles-mapper.mappings[0].replacement=PRINCIPAL_ROLE:$1

    # Storage Properties Integration
    polaris.features."SUPPORTED_CATALOG_STORAGE_TYPES"=["S3","GCS","AZURE","FILE"]
    polaris.features."ALLOW_INSECURE_STORAGE_TYPES"=true
    polaris.readiness.ignore-severe-issues=true
