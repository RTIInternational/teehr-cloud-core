apiVersion: batch/v1
kind: CronJob
metadata:
  name: polaris-principal-sync
spec:
  schedule: "*/5 * * * *"
  concurrencyPolicy: Forbid
  successfulJobsHistoryLimit: 3
  failedJobsHistoryLimit: 3
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: Never
          containers:
            - name: polaris-principal-sync
              image: prefecthq/prefect:3.2.0-python3.10
              env:
                - name: POLARIS_MANAGEMENT_URL
                  value: "http://polaris:8181"
                - name: POLARIS_ROOT_CREDENTIALS
                  valueFrom:
                    secretKeyRef:
                      name: polaris-secrets
                      key: root-credentials
                - name: POLARIS_REALM
                  value: "${var.polaris.defaultRealm}"
                - name: KEYCLOAK_URL
                  value: "http://keycloak-service.teehr-hub.svc.cluster.local:8080"
                - name: KEYCLOAK_ADMIN_USER
                  valueFrom:
                    secretKeyRef:
                      name: keycloak-admin-secrets
                      key: username
                - name: KEYCLOAK_ADMIN_PASSWORD
                  valueFrom:
                    secretKeyRef:
                      name: keycloak-admin-secrets
                      key: password
              command:
                - python
                - /scripts/sync_principals.py
              volumeMounts:
                - name: sync-script
                  mountPath: /scripts
                  readOnly: true
        volumes:
          - name: sync-script
            configMap:
              name: polaris-sync-principals-script
