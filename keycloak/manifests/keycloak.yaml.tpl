apiVersion: k8s.keycloak.org/v2alpha1
kind: Keycloak
metadata:
  name: keycloak
spec:
  instances: 1
  startOptimized: false
  image: ${actions.build.keycloak-theme.outputs.deploymentImageId}
  db:
    vendor: postgres
    host: keycloak-pg
    database: keycloak
    usernameSecret:
      name: keycloak-db-secrets
      key: username
    passwordSecret:
      name: keycloak-db-secrets
      key: password
  http:
    httpEnabled: true
  ingress:
    enabled: false
  hostname:
    hostname: https://auth.${var.hostname}
    strict: false
    backchannelDynamic: false
  proxy:
    headers: xforwarded
  bootstrapAdmin:
    user:
      secret: keycloak-admin-secrets
  # CPU p95 0.01 cores over 47h (2026-09); request held at 200m to keep a
  # floor under the auth path. Memory peak was 1.17Gi, just over the previous
  # 1Gi request, so the request is raised to cover it.
  resources:
    requests:
      cpu: 200m
      memory: "${environment.name == 'local' ? '512Mi' : '1536Mi'}"
    limits:
      cpu: "1"
      memory: 2Gi
