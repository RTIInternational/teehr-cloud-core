# On remote, the jupyter SA can carry an IRSA role scoped to external,
# non-warehouse data only (e.g. HEFS on S3), which Polaris cannot vend
# credentials for. Notebook access to the Iceberg warehouse still goes through
# Polaris like every other client.
#
# The annotation is rendered only when var.irsa.jupyterRoleArn is set to a
# non-empty value, so deployments with no such bucket can omit the key
# entirely rather than carry a placeholder. `||` tolerates both a missing key
# and a missing `irsa` block.
apiVersion: v1
kind: ServiceAccount
metadata:
  name: jupyter
  namespace: ${environment.namespace}
  ${if environment.name == "remote" && (var.irsa.jupyterRoleArn || "") != ""}
  annotations:
    eks.amazonaws.com/role-arn: ${var.irsa.jupyterRoleArn}
  ${endif}
  labels:
    app: jupyterhub
    component: jupyter
