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
  # The control plane's own Keycloak client; the secret is in control-plane-secrets.
  CONTROL_PLANE_CLIENT_ID: "teehr-control-plane"
  # Polaris management API, for provisioning project namespaces and roles. The
  # token comes from Keycloak; Polaris maps it to the named principal
  # service-account-teehr-control-plane (polaris-bootstrap acl-config).
  POLARIS_URL: "http://polaris:8181"
  POLARIS_TOKEN_URL: "http://keycloak-service:8080/realms/teehr/protocol/openid-connect/token"
  POLARIS_REALM: "${var.polaris.defaultRealm}"
  POLARIS_CATALOG: "${var.polaris.defaultRealm}"
  # Each project's namespace is pinned to <warehouse>/<project>/.
  POLARIS_WAREHOUSE: "${var.polaris.catalogWarehouse}"
  # Object storage as Polaris's vended credentials reach it (dataset I/O as the
  # caller): RustFS on local.
  STORAGE_S3_ENDPOINT: "${var.polaris.catalogS3Endpoint}"
  STORAGE_S3_REGION: "${var.polaris.catalogS3Region}"
  STORAGE_S3_PATH_STYLE: "${var.polaris.catalogS3PathStyleAccess}"
  # System projects (public, platform): every user reads (the default-group role);
  # only ingest's prefect-polaris service account writes.
  SYSTEM_PROJECT_READER_ROLES: "teehr-read-only"
  SYSTEM_PROJECT_WRITER_ROLES: "teehr-platform-write"
  # Keycloak admin API (teehr realm only), for the realm roles that carry project
  # access.
  KEYCLOAK_ADMIN_URL: "http://keycloak-service:8080"
  KEYCLOAK_REALM: "teehr"
  # Upload staging (Phase 3): a bucket only the control plane reaches. Local only
  # until the remote bucket and IAM role exist (#171); empty = uploads disabled.
  STAGING_BUCKET: "${environment.name == 'local' ? 'uploads' : ''}"
  STAGING_S3_ENDPOINT: "${environment.name == 'local' ? 'http://local-s3:9000' : ''}"
  # Upload URLs are signed for the address clients use: on local, the S3 ingress
  # (so a laptop can PUT); on AWS, S3 itself.
  STAGING_S3_PUBLIC_ENDPOINT: "${environment.name == 'local' ? 'https://s3.' + var.hostname : ''}"
