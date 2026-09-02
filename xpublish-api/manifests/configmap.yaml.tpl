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
  # Dynamically set based on environment name: "local" or "remote"
  ICECHUNK_STORAGE_MODE: "${environment.name}"
  # Local mode: explicit MinIO endpoint. Remote mode: empty (AWS SDK handles it).
  ICECHUNK_ENDPOINT_URL: "${ environment.name == 'local' ? 'http://minio:9000' : '' }"
  AWS_DEFAULT_REGION: "${ environment.name == 'local' ? 'us-east-1' : var.aws.region }"
  CORS_ORIGINS: "${var.allowedOrigins}"
  KEYCLOAK_ISSUER_URL: "https://auth.${var.hostname}/realms/teehr"
  # Internal cluster URL avoids routing JWKS fetches through the ingress
  KEYCLOAK_JWKS_URL: "http://keycloak-service:8080/realms/teehr/protocol/openid-connect/certs"
  KEYCLOAK_ALLOWED_AUDIENCES: "teehr-api,teehr-frontend"
