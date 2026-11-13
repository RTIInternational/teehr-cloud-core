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
  KEYCLOAK_ALLOWED_AUDIENCES: "teehr-api,teehr-frontend,teehr-frontend-local-dev,jupyterhub"
  # Realm roles with full rights on every project.
  CONTROL_PLANE_ADMIN_ROLES: "admin"
