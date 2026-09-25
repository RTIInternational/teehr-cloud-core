apiVersion: v1
kind: Service
metadata:
  name: prefect-pg
spec:
  type: ClusterIP
  ports:
    - port: 5432
      targetPort: 5432
  selector:
    app: prefect-pg

---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: prefect-pg
spec:
  replicas: 1
  selector:
    matchLabels:
      app: prefect-pg
  template:
    metadata:
      labels:
        app: prefect-pg
    spec:
      # nodeSelector:
      #   teehr-hub/nodegroup-name: core-a
      containers:
        - name: postgres
          image: postgres:15
          env:
            - name: POSTGRES_DB
              valueFrom:
                secretKeyRef:
                  name: prefect-db-secrets
                  key: database
            - name: POSTGRES_USER
              valueFrom:
                secretKeyRef:
                  name: prefect-db-secrets
                  key: username
            - name: POSTGRES_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: prefect-db-secrets
                  key: password
            - name: PGDATA
              value: /var/lib/postgresql/data/pgdata
          ports:
            - containerPort: 5432
          readinessProbe:
            exec:
              command:
                - pg_isready
                - -U
                - prefect
                - -d
                - prefect
            initialDelaySeconds: 5
            periodSeconds: 10
            timeoutSeconds: 5
            failureThreshold: 6
          livenessProbe:
            exec:
              command:
                - pg_isready
                - -U
                - prefect
                - -d
                - prefect
            initialDelaySeconds: 30
            periodSeconds: 10
            timeoutSeconds: 5
            failureThreshold: 6
          # Sized from 47h of production metrics (2026-09). CPU p95 was 0.21
          # cores against a 2-core request. Memory, by contrast, sat at
          # 7.1-7.9Gi against a 4Gi request and an 8Gi limit -- i.e. routinely
          # ~2x its request and pressed up against the limit, which risks
          # eviction. The request is raised to match observed steady state and
          # the limit lifted to leave real headroom.
          resources:
            requests:
              cpu: "250m"
              memory: "${environment.name == 'local' ? '1Gi' : '8Gi'}"
            limits:
              cpu: "4"
              memory: "${environment.name == 'local' ? '2Gi' : '12Gi'}"
          volumeMounts:
            - name: pgdata
              mountPath: /var/lib/postgresql/data
      volumes:
        - name: pgdata
          persistentVolumeClaim:
            claimName: prefect-pg-data

---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: prefect-pg-data
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 50Gi