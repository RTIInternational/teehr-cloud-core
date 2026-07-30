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
    # Keycloak advertises external hostnames in discovery metadata; disable discovery
    # so Polaris uses the configured auth-server-url and internal JWKS path directly.
    quarkus.oidc.discovery-enabled=false
    quarkus.oidc.jwks-path=/protocol/openid-connect/certs
    quarkus.tls.trust-all=true
    quarkus.oidc.connection-delay=PT10S
    quarkus.oidc.connection-retry-count=5
    quarkus.oidc.token.audience=account
    quarkus.oidc.token.issuer=any

    # Access control: map Keycloak group membership (JWT 'groups' claim) directly to
    # Polaris principal roles. This means no user sync is required for group-level
    # access — adding a user to a Keycloak group immediately grants the corresponding
    # Polaris permissions on their next token issuance.
    #
    # Also reads realm_access/roles so Keycloak service accounts (trino-polaris,
    # prefect-polaris) can be granted Polaris access via realm role assignment,
    # without needing a separate Polaris principal entity.
    #
    # Patterns mapped:
    #   /iceberg-catalog-admins (group) → iceberg-catalog-admin
    #   /teehr-<role>  (group)          → teehr-<role>  (e.g. teehr-read-only, teehr-read-write)
    #   iceberg-catalog-admin (realm role for service accounts)
    #   teehr-<role>   (realm role for service accounts)
    #
    # Individual/table-level grants: use the polaris-sync-principals script to create
    # a named principal for the user and assign specific catalog-role grants. These
    # are additive on top of the JWT-based group grants above.
    quarkus.oidc.roles.role-claim-path=groups,realm_access/roles
    polaris.oidc.principal-roles-mapper.type=default
    polaris.oidc.principal-roles-mapper.mappings[0].regex=^/?iceberg-catalog-admins?$
    polaris.oidc.principal-roles-mapper.mappings[0].replacement=PRINCIPAL_ROLE:iceberg-catalog-admin
    polaris.oidc.principal-roles-mapper.mappings[1].regex=^/?teehr-(.+)$
    polaris.oidc.principal-roles-mapper.mappings[1].replacement=PRINCIPAL_ROLE:teehr-$1

    # Storage Properties Integration
    polaris.features."SUPPORTED_CATALOG_STORAGE_TYPES"=["S3","GCS","AZURE","FILE"]
    polaris.features."ALLOW_INSECURE_STORAGE_TYPES"=true
    polaris.features."SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION"=${var.polaris.skipCredentialSubscopingIndirection}
    polaris.readiness.ignore-severe-issues=true
