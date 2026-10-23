apiVersion: v1
kind: ConfigMap
metadata:
  name: xpublish-api-config
  labels:
    app: xpublish-api
    component: backend
data:
  ICECHUNK_BUCKET: "${var.icechunk.bucket}"
  ICECHUNK_PREFIX: "${var.icechunk.prefix}"
  ICECHUNK_BRANCH: "main"
  PMTILES_BUCKET: "${var.pmtiles.bucket}"
  PMTILES_PREFIX: "${var.pmtiles.prefix}"
  # Dynamically set based on environment name: "local" or "remote"
  ICECHUNK_STORAGE_MODE: "${environment.name}"
  # Cluster S3 endpoint; empty in remote, where the AWS SDK resolves it.
  ICECHUNK_ENDPOINT_URL: "${var.polaris.catalogS3Endpoint}"
  AWS_DEFAULT_REGION: "${ environment.name == 'local' ? 'us-east-1' : var.aws.region }"
  CORS_ORIGINS: "${var.allowedOrigins}"
  KEYCLOAK_ISSUER_URL: "https://auth.${var.hostname}/realms/teehr"
  # Internal cluster URL avoids routing JWKS fetches through the ingress
  KEYCLOAK_JWKS_URL: "http://keycloak-service:8080/realms/teehr/protocol/openid-connect/certs"
  KEYCLOAK_ALLOWED_AUDIENCES: "teehr-api,teehr-frontend,teehr-frontend-local-dev"
  # Read access by Keycloak realm role, from var.xpublish.accessRules (a YAML map), as JSON.
  # A deployment without the var gets {}: every repo and layer open to logged-in users.
  # See docs/icechunk-access-control.md.
  TEEHR_ACCESS_RULES: '${(var.xpublish.accessRules || "") == "" ? "{}" : jsonEncode(var.xpublish.accessRules)}'
  # Notebook S3 credentials: the role xpublish-api assumes, and whether STS is available.
  # Missing vars mean no STS: the credentials route returns the repo list only.
  ICECHUNK_ACCESS_ROLE_ARN: "${var.icechunk.accessRoleArn || ''}"
  ICECHUNK_STS_UNAVAILABLE: "${var.icechunk.storageStsUnavailable || 'true'}"
