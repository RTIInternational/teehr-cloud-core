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
    polaris.authentication.type=mixed
    polaris.oidc.principal-mapper.name-claim-path=preferred_username

    # Quarkus OIDC Service Identity Configuration
    quarkus.oidc.application-type=service
    quarkus.oidc.roles.role-claim-path=realm_access/roles
    quarkus.otel.sdk.disabled=true

    # Storage Properties Integration
    polaris.features."SUPPORTED_CATALOG_STORAGE_TYPES"=["S3","GCS","AZURE","FILE"]
    polaris.features."ALLOW_INSECURE_STORAGE_TYPES"=true
    polaris.readiness.ignore-severe-issues=true
