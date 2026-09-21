# The jupyter service account is the Spark driver identity for notebook-launched
# jobs. It needs only the namespaced spark-role in its own namespace to manage
# executor pods/services/configmaps. It must NOT hold any cluster-scoped role.
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: jupyter-spark-role-binding
  namespace: ${environment.namespace}
roleRef:
  kind: Role
  name: spark-role
  apiGroup: rbac.authorization.k8s.io
subjects:
- kind: ServiceAccount
  name: jupyter
  namespace: ${environment.namespace}
