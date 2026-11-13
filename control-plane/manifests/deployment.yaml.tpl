apiVersion: apps/v1
kind: Deployment
metadata:
  name: teehr-control-plane
  labels:
    app: teehr-control-plane
    component: control-plane
spec:
  # One replica: migrations run in the init container, so a single pod keeps
  # them from racing. Revisit (move to a Job) before scaling out.
  replicas: 1
  selector:
    matchLabels:
      app: teehr-control-plane
  template:
    metadata:
      labels:
        app: teehr-control-plane
        component: control-plane
    spec:
      securityContext:
        runAsNonRoot: true
        runAsUser: 10001
      initContainers:
      - name: migrate
        image: ${actions.build.teehr-control-plane.outputs.deploymentImageId}
        imagePullPolicy: IfNotPresent
        command: ["alembic", "upgrade", "head"]
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
      containers:
      - name: control-plane
        image: ${actions.build.teehr-control-plane.outputs.deploymentImageId}
        imagePullPolicy: IfNotPresent
        ports:
        - containerPort: 8000
          name: http
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
        # Polaris root credentials, as the polaris-bootstrap Job uses. Replace with a
        # dedicated, narrower Polaris principal before this runs outside KinD.
        - name: POLARIS_ROOT_CREDENTIALS
          valueFrom:
            secretKeyRef:
              name: polaris-secrets
              key: root-credentials
        envFrom:
        - configMapRef:
            name: teehr-control-plane-config
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 10
          periodSeconds: 10
          timeoutSeconds: 5
          failureThreshold: 3
        readinessProbe:
          httpGet:
            path: /ready
            port: 8000
          initialDelaySeconds: 5
          periodSeconds: 10
          timeoutSeconds: 3
          failureThreshold: 3
        resources:
          requests:
            cpu: 50m
            memory: 128Mi
          limits:
            cpu: 500m
            memory: 512Mi
