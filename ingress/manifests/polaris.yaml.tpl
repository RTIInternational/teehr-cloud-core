apiVersion: projectcontour.io/v1
kind: HTTPProxy
metadata:
  name: polaris-httpproxy
  namespace: ${environment.namespace}
spec:
  virtualhost:
    fqdn: polaris.${var.hostname}
    tls:
      secretName: polaris.${var.hostname}-tls
  routes:
  - services:
    - name: polaris
      port: 8181
    conditions:
    - prefix: /api/catalog
  - services:
    - name: polaris
      port: 8181
    conditions:
    - prefix: /api/management
