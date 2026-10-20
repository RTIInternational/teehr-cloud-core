apiVersion: v1
kind: ServiceAccount
metadata:
  name: spark
  namespace: ${environment.namespace}
---
# Least-privilege RBAC for Spark-on-Kubernetes in client mode. The driver
# (the Jupyter notebook pod or a Prefect job pod) creates and manages executor
# pods, the driver headless service, and the executor SPARK_CONF configmap,
# all within its own namespace. That is the complete set of permissions Spark
# needs; there is deliberately no ClusterRole and no access to secrets. See
# https://spark.apache.org/docs/latest/running-on-kubernetes.html#rbac
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: spark-role
  namespace: ${environment.namespace}
rules:
- apiGroups: [""]
  resources: ["pods", "services", "configmaps"]
  verbs: ["create", "get", "list", "watch", "delete", "deletecollection", "patch", "update"]
# persistentvolumeclaims is retained only for Spark dynamic-PVC executor
# storage. If no flow uses spark.kubernetes.executor.volumes...OnDemand PVCs,
# this line can be removed as well.
- apiGroups: [""]
  resources: ["persistentvolumeclaims"]
  verbs: ["create", "get", "list", "watch", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: spark-role-binding
  namespace: ${environment.namespace}
roleRef:
  kind: Role
  name: spark-role
  apiGroup: rbac.authorization.k8s.io
subjects:
- kind: ServiceAccount
  name: spark
  namespace: ${environment.namespace}
