# Own hostname for the control plane while it is local-only (Phase 1). Folds into
# the single API host when the write gateway lands (Phase 2).
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: ${var.hostname}-control-plane-cert
spec:
  commonName: control-plane.${var.hostname}
  dnsNames:
  - control-plane.${var.hostname}
  issuerRef:
    name: ${var.certificateIssuerName}
    kind: ClusterIssuer
  secretName: control-plane.${var.hostname}-tls
---
apiVersion: projectcontour.io/v1
kind: HTTPProxy
metadata:
  name: control-plane-httpproxy
  namespace: ${environment.namespace}
spec:
  virtualhost:
    fqdn: control-plane.${var.hostname}
    tls:
      secretName: control-plane.${var.hostname}-tls
  routes:
  - conditions:
    - prefix: /
    services:
    - name: teehr-control-plane
      port: 8000
