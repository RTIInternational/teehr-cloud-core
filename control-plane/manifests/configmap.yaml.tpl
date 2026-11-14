apiVersion: v1
kind: ConfigMap
metadata:
  name: teehr-control-plane-config
  labels:
    app: teehr-control-plane
    component: control-plane
data:
  # Same identity settings as teehr-api (api/manifests/configmap.yaml.tpl).
  KEYCLOAK_ISSUER_URL: "https://auth.${var.hostname}/realms/teehr"
  KEYCLOAK_JWKS_URL: "http://keycloak-service:8080/realms/teehr/protocol/openid-connect/certs"
  KEYCLOAK_ALLOWED_AUDIENCES: "teehr-api,teehr-frontend,teehr-frontend-local-dev,jupyterhub,teehr-control-plane-docs"
  # Public client the /docs page signs in with (realm config, local only).
  KEYCLOAK_DOCS_CLIENT_ID: "teehr-control-plane-docs"
  # Realm roles with full rights on every project (very few people), and roles that
  # can see every project and its members but change nothing.
  CONTROL_PLANE_ADMIN_ROLES: "control-plane-admin"
  CONTROL_PLANE_VIEWER_ROLES: "control-plane-viewer"
  # Polaris management API, for provisioning project namespaces and roles.
  POLARIS_URL: "http://polaris:8181"
  POLARIS_REALM: "${var.polaris.defaultRealm}"
  POLARIS_CATALOG: "${var.polaris.defaultRealm}"
  # Each project's namespace is pinned to <warehouse>/<project>/.
  POLARIS_WAREHOUSE: "${var.polaris.catalogWarehouse}"
  # System projects (public, platform): every user reads (the default-group role);
  # only ingest's prefect-polaris service account writes.
  SYSTEM_PROJECT_READER_ROLES: "teehr-read-only"
  SYSTEM_PROJECT_WRITER_ROLES: "teehr-platform-write"
  # Keycloak admin API, for the realm roles that carry project access.
  KEYCLOAK_ADMIN_URL: "http://keycloak-service:8080"
  KEYCLOAK_REALM: "teehr"
