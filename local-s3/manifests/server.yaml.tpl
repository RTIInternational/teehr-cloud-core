apiVersion: v1
kind: Service
metadata:
  name: local-s3
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
    app: local-s3

---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: local-s3
spec:
  replicas: 1
  selector:
    matchLabels:
      app: local-s3
  template:
    metadata:
      labels:
        app: local-s3
    spec:
      # nodeSelector:
      #   teehr-hub/nodegroup-name: core-a

      # RustFS runs as uid/gid 10001 and needs /data writable.
      securityContext:
        fsGroup: 10001
      containers:
        - name: local-s3
          image: rustfs/rustfs:1.0.0
          env:
            - name: RUSTFS_VOLUMES
              value: /data
            - name: RUSTFS_ADDRESS
              value: 0.0.0.0:9000
            - name: RUSTFS_CONSOLE_ENABLE
              value: "true"
            - name: RUSTFS_CONSOLE_ADDRESS
              value: 0.0.0.0:9001
            - name: RUSTFS_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: local-s3-secrets
                  key: accesskey
            - name: RUSTFS_SECRET_KEY
              valueFrom:
                secretKeyRef:
                  name: local-s3-secrets
                  key: secretkey
          ports:
            - containerPort: 9000
            - containerPort: 9001
          volumeMounts:
            - name: local-s3-data
              mountPath: /data
          readinessProbe:
            httpGet:
              path: /health/ready
              port: 9000
            initialDelaySeconds: 5
            periodSeconds: 10
          livenessProbe:
            httpGet:
              path: /health
              port: 9000
            initialDelaySeconds: 5
            periodSeconds: 10
      volumes:
        - name: local-s3-data
          emptyDir: {}
