apiVersion: v1
kind: Service
metadata:
  name: minio
spec:
  type: ClusterIP
  ports:
    - name: api
      port: 9000
      targetPort: 9000
    - name: console
      port: 9001
      targetPort: 9001
  selector:
    app: minio

---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: minio
spec:
  replicas: 1
  selector:
    matchLabels:
      app: minio
  template:
    metadata:
      labels:
        app: minio
    spec:
      # nodeSelector:
      #   teehr-hub/nodegroup-name: core-a
      containers:
        - name: minio
          # Stopgap: minio/minio was removed from Docker Hub. quay.io still
          # mirrors the final community releases; pinned because the tag is
          # frozen and nothing new will be published.
          image: quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z
          args:
            - server
            - /data
            - --console-address
            - :9001
          env:
            - name: MINIO_ROOT_USER
              valueFrom:
                secretKeyRef:
                  name: minio-secrets
                  key: accesskey
            - name: MINIO_ROOT_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: minio-secrets
                  key: secretkey
            - name: MINIO_BROWSER_REDIRECT_URL
              value: "https://minio.${var.hostname}/ui/"
          ports:
            - containerPort: 9000
            - containerPort: 9001
          volumeMounts:
            - name: minio-data
              mountPath: /data
          readinessProbe:
            httpGet:
              path: /minio/health/ready
              port: 9000
            initialDelaySeconds: 5
            periodSeconds: 10
          livenessProbe:
            httpGet:
              path: /minio/health/live
              port: 9000
            initialDelaySeconds: 5
            periodSeconds: 10
      volumes:
        - name: minio-data
          emptyDir: {}