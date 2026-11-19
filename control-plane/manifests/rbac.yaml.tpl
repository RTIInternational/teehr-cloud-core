# The control plane's own service account. On remote it also carries the IAM role
# for the upload staging bucket. It starts the Kubernetes Jobs that run
# work users start through the API (uploads now, evaluation runs later; ADR-0014):
# Jobs only, and reading their pods (why a job failed), in this namespace.
apiVersion: v1
kind: ServiceAccount
metadata:
  name: teehr-control-plane
  labels:
    app: teehr-control-plane
    component: control-plane
  ${if environment.name == "remote" && (var.irsa.controlPlaneRoleArn || "") != ""}
  # The upload staging bucket only (teehr-cloud-platform control-plane-irsa.tf).
  annotations:
    eks.amazonaws.com/role-arn: ${var.irsa.controlPlaneRoleArn}
  ${endif}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: teehr-control-plane-jobs
  labels:
    app: teehr-control-plane
    component: control-plane
rules:
  - apiGroups: ["batch"]
    resources: ["jobs"]
    # patch: scale a run's Indexed Job to its pods (ADR-0017).
    verbs: ["create", "get", "list", "patch", "delete"]
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: teehr-control-plane-jobs
  labels:
    app: teehr-control-plane
    component: control-plane
subjects:
  - kind: ServiceAccount
    name: teehr-control-plane
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: teehr-control-plane-jobs
