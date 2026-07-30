apiVersion: v1
kind: ServiceAccount
metadata:
  name: xpublish-api
  namespace: ${environment.namespace}
  ${if environment.name == "remote"}
  annotations:
    eks.amazonaws.com/role-arn: ${var.irsa.icebergReadOnlyRoleArn}
  ${endif}
