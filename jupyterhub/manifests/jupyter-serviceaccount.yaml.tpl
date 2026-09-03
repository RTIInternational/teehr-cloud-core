# On remote, the jupyter SA carries an IRSA role scoped to external,
# non-warehouse data only (e.g. HEFS on S3), which Polaris cannot vend
# credentials for. Notebook access to the Iceberg warehouse still goes through
# Polaris like every other client. Guarded because `local` defines no irsa vars.
apiVersion: v1
kind: ServiceAccount
metadata:
  name: jupyter
  namespace: ${environment.namespace}
  ${if environment.name == "remote"}
  annotations:
    eks.amazonaws.com/role-arn: ${var.irsa.jupyterRoleArn}
  ${endif}
  labels:
    app: jupyterhub
    component: jupyter
