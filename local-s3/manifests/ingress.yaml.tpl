apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: ${var.hostname}-s3-cert
spec:
  commonName: s3.${var.hostname}
  dnsNames:
  - s3.${var.hostname}
  issuerRef:
    name: ${var.certificateIssuerName}
    kind: ClusterIssuer
  secretName: s3.${var.hostname}-tls
---
apiVersion: projectcontour.io/v1
kind: HTTPProxy
metadata:
  name: local-s3-httpproxy
  namespace: ${environment.namespace}
spec:
  virtualhost:
    fqdn: s3.${var.hostname}
    tls:
      secretName: s3.${var.hostname}-tls
  routes:
  - services:
    - name: local-s3
      port: 9000
    conditions:
    - prefix: /
  # No rewrite: the console builds absolute asset URLs under /rustfs/console.
  - services:
    - name: local-s3
      port: 9001
    conditions:
    - prefix: /rustfs/console
    enableWebsockets: true
