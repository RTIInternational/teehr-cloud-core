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
    # auth-server-url + relative jwks-path enables local JWT validation without discovery.
    # Without auth-server-url, Quarkus falls back to userinfo introspection which fails for service accounts.
    quarkus.oidc.auth-server-url=${var.polaris.oidcIssuerUri}
    quarkus.oidc.discovery-enabled=false
    quarkus.oidc.jwks-path=/protocol/openid-connect/certs
    quarkus.tls.trust-all=true
    quarkus.oidc.connection-delay=PT10S
    quarkus.oidc.connection-retry-count=5
    quarkus.oidc.token.audience=account
    quarkus.oidc.token.issuer=any

    # Access control: map Keycloak realm roles to Polaris PrincipalRoles.
    # Uses realm_access/roles (not groups) to cover both:
    #   - human users: realm roles propagated from Keycloak group membership
    #   - service accounts: realm roles assigned directly (trino-polaris, prefect-polaris)
    #
    # Patterns:
    #   iceberg-catalog-admin → PRINCIPAL_ROLE:iceberg-catalog-admin
    #   teehr-<role>          → PRINCIPAL_ROLE:teehr-<role>
    #
    # Individual/table-level grants: use the polaris-sync-principals script.
    # Named principal role bindings from the sync take precedence over JWT mapping.
    quarkus.oidc.roles.role-claim-path=realm_access/roles
    polaris.oidc.principal-roles-mapper.type=default
    polaris.oidc.principal-roles-mapper.mappings[0].regex=^iceberg-catalog-admin$
    polaris.oidc.principal-roles-mapper.mappings[0].replacement=PRINCIPAL_ROLE:iceberg-catalog-admin
    polaris.oidc.principal-roles-mapper.mappings[1].regex=^teehr-(.+)$
    polaris.oidc.principal-roles-mapper.mappings[1].replacement=PRINCIPAL_ROLE:teehr-$1

    # Storage Properties Integration
    polaris.features."SUPPORTED_CATALOG_STORAGE_TYPES"=["S3","GCS","AZURE","FILE"]
    polaris.features."ALLOW_INSECURE_STORAGE_TYPES"=true
    polaris.features."SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION"=${var.polaris.skipCredentialSubscopingIndirection}
    polaris.readiness.ignore-severe-issues=true
