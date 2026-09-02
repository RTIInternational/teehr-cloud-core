apiVersion: v1
kind: ConfigMap
metadata:
  name: teehr-api-config
  labels:
    app: teehr-api
    component: backend
data:
  TRINO_HOST: "trino"
  TRINO_PORT: "8080"
  TRINO_USER: "teehr"
  TRINO_CATALOG: "iceberg"
  TRINO_SCHEMA: "teehr"
  CORS_ORIGINS: "${var.allowedOrigins}"
  KEYCLOAK_ISSUER_URL: "https://auth.${var.hostname}/realms/teehr"
  KEYCLOAK_JWKS_URL: "http://keycloak-service:8080/realms/teehr/protocol/openid-connect/certs"
  KEYCLOAK_AUDIENCE: "teehr-api"
  KEYCLOAK_ALLOWED_AUDIENCES: "teehr-api,teehr-frontend,jupyterhub"
  BROKER_TOKEN_EXCHANGE_ENABLED: "true"
  BROKER_TOKEN_ENDPOINT: "http://keycloak-service:8080/realms/teehr/protocol/openid-connect/token"
  BROKER_OAUTH_CLIENT_ID: "teehr-api"
  BROKER_OAUTH_CLIENT_SECRET: ""
  BROKER_TARGET_AUDIENCE: "account"
  BROKER_DEFAULT_SCOPE: "openid profile email"
  BROKER_MIN_TTL_SECONDS: "120"
  BROKER_MAX_TTL_SECONDS: "900"
  BROKER_REQUEST_TIMEOUT_SECONDS: "10"
  BROKER_SUBJECT_CLIENT_ID: "jupyterhub"
  BROKER_DELEGATED_SESSION_TTL_SECONDS: "43200"
  ANON_RATE_LIMIT_RPM: "20"
  AUTH_RATE_LIMIT_RPM: "120"
  ROW_LIMIT_ANON: "200"
  ROW_LIMIT_API_KEY: "50000"
  ROW_LIMIT_BASIC_USER: "50000"
  ROW_LIMIT_AUTH: "10000"