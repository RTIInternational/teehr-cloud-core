# Memberships -> Keycloak and Polaris, every 10 minutes: repairs drift such as a
# Keycloak import that reset users' realm roles (src/access_reconcile.py).
apiVersion: batch/v1
kind: CronJob
metadata:
  name: control-plane-access-reconcile
  labels:
    app: control-plane-access-reconcile
    component: control-plane
spec:
  schedule: "*/10 * * * *"
  concurrencyPolicy: Forbid
  successfulJobsHistoryLimit: 1
  failedJobsHistoryLimit: 3
  jobTemplate:
    spec:
      backoffLimit: 0
      activeDeadlineSeconds: 540
      ttlSecondsAfterFinished: 3600
      template:
        metadata:
          labels:
            app: control-plane-access-reconcile
        spec:
          restartPolicy: Never
          automountServiceAccountToken: false
          securityContext:
            runAsNonRoot: true
            runAsUser: 10001
          containers:
          - name: reconcile
            image: ${actions.build.teehr-control-plane.outputs.deploymentImageId}
            imagePullPolicy: IfNotPresent
            command: ["python", "-m", "src.access_reconcile"]
            env:
            - name: CONTROL_PLANE_DB_HOST
              value: app-pg
            - name: CONTROL_PLANE_DB_PORT
              value: "5432"
            - name: CONTROL_PLANE_DB_NAME
              valueFrom:
                secretKeyRef:
                  name: app-db-secrets
                  key: database
            - name: CONTROL_PLANE_DB_USER
              valueFrom:
                secretKeyRef:
                  name: app-db-secrets
                  key: username
            - name: CONTROL_PLANE_DB_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: app-db-secrets
                  key: password
            - name: CONTROL_PLANE_CLIENT_SECRET
              valueFrom:
                secretKeyRef:
                  name: control-plane-secrets
                  key: client-secret
            envFrom:
            - configMapRef:
                name: teehr-control-plane-config
            resources:
              requests:
                cpu: 10m
                memory: 128Mi
              limits:
                memory: 256Mi
