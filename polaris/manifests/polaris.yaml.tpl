apiVersion: v1
kind: ServiceAccount
metadata:
  name: polaris
  namespace: ${environment.namespace}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  labels:
    app: polaris
  name: polaris
spec:
  replicas: 1
  selector:
    matchLabels:
      app: polaris
  template:
    metadata:
      labels:
        app: polaris
    spec:
      serviceAccountName: polaris
      ${if environment.name == "local"}
      hostAliases:
        - ip: "${var.polaris.keycloakIngressIp}"
          hostnames:
            - "auth.${var.hostname}"
      ${endif}
      initContainers:
        - name: schema-bootstrap
          image: apache/polaris-admin-tool:1.5.0
          imagePullPolicy: IfNotPresent
          command:
            - /bin/sh
            - -c
            - |
              JAR=/deployments/polaris-admin-tool.jar
              echo "Found jar: $JAR"
              OUTPUT=$(java $JAVA_OPTS -jar "$JAR" bootstrap \
                --realm="$POLARIS_BOOTSTRAP_REALM" \
                -c "$POLARIS_BOOTSTRAP_REALM,$ROOT_USERNAME,$ROOT_PASSWORD" \
                -p 2>&1)
              EXIT_CODE=$?
              echo "$OUTPUT"
              if [ $EXIT_CODE -eq 0 ]; then
                echo "Bootstrap succeeded."
                exit 0
              fi
              if echo "$OUTPUT" | grep -q "already been bootstrapped"; then
                echo "Metastore already bootstrapped — skipping."
                exit 0
              fi
              echo "Bootstrap failed with unexpected error (exit code $EXIT_CODE)."
              exit $EXIT_CODE
          env:
            # Core persistence assignment
            - name: POLARIS_PERSISTENCE_TYPE
              value: relational-jdbc
            - name: POLARIS_PERSISTENCE_AUTO_BOOTSTRAP_TYPES
              value: relational-jdbc
            - name: POLARIS_REALM_CONTEXT_REALMS
              value: ${var.polaris.realmsCsv}
            - name: POLARIS_BOOTSTRAP_REALM
              value: ${var.polaris.defaultRealm}
            - name: ROOT_USERNAME
              valueFrom:
                secretKeyRef:
                  name: polaris-secrets
                  key: root-username
            - name: ROOT_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: polaris-secrets
                  key: root-password

            # Explicit lowercase system translations to force Agroal activation
            - name: quarkus_datasource_db-kind
              value: postgresql
            - name: quarkus_datasource_jdbc_url
              value: jdbc:postgresql://polaris-pg:5432/polaris
            - name: quarkus_datasource_username
              value: polaris

            # Fetch secret credentials cleanly
            - name: quarkus_datasource_password
              valueFrom:
                secretKeyRef:
                  name: polaris-db-secrets
                  key: password
          volumeMounts:
            - name: polaris-config
              mountPath: /deployments/config/application.properties
              subPath: application.properties
              readOnly: true

      containers:
        - name: polaris
          image: apache/polaris:1.5.0
          imagePullPolicy: IfNotPresent
          env:
            - name: POLARIS_REALM_CONTEXT_REALMS
              value: ${var.polaris.realmsCsv}
            - name: POLARIS_PERSISTENCE_TYPE
              value: relational-jdbc
            - name: QUARKUS_DATASOURCE_JDBC_URL
              value: jdbc:postgresql://polaris-pg:5432/polaris
            - name: QUARKUS_DATASOURCE_USERNAME
              valueFrom:
                secretKeyRef:
                  name: polaris-db-secrets
                  key: username
            - name: QUARKUS_DATASOURCE_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: polaris-db-secrets
                  key: password
            ${if environment.name == "local"}
            - name: QUARKUS_OIDC_AUTH_SERVER_URL
              value: ${var.polaris.oidcIssuerUri}
            - name: AWS_ACCESS_KEY_ID
              valueFrom:
                secretKeyRef:
                  name: minio-secrets
                  key: accesskey
            - name: AWS_SECRET_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: minio-secrets
                  key: secretkey
            - name: AWS_S3_ENDPOINT
              value: ${var.polaris.catalogS3Endpoint}
            - name: AWS_S3_PATH_STYLE_ACCESS
              value: "${var.polaris.catalogS3PathStyleAccess}"
            ${endif}
            ${if environment.name != "local"}
            - name: QUARKUS_OIDC_AUTH_SERVER_URL
              value: ${var.polaris.oidcIssuerUri}
            ${endif}
            - name: AWS_REGION
              value: us-east-2
          ports:
            - name: api
              containerPort: 8181
              protocol: TCP
            - name: management
              containerPort: 8182
              protocol: TCP
          readinessProbe:
            httpGet:
              path: /q/health/ready
              port: 8182
            initialDelaySeconds: 20
            periodSeconds: 10
            timeoutSeconds: 5
            failureThreshold: 6
          livenessProbe:
            httpGet:
              path: /q/health/live
              port: 8182
            initialDelaySeconds: 30
            periodSeconds: 10
            timeoutSeconds: 5
            failureThreshold: 6
          resources:
            requests:
              cpu: 200m
              memory: 512Mi
            limits:
              cpu: 1000m
              memory: 1Gi
          volumeMounts:
            - name: polaris-config
              mountPath: /deployments/config/application.properties
              subPath: application.properties
              readOnly: true
      volumes:
        - name: polaris-config
          configMap:
            name: polaris-config

---
apiVersion: v1
kind: Service
metadata:
  labels:
    app: polaris
  name: polaris
spec:
  ports:
    - name: api
      protocol: TCP
      port: 8181
      targetPort: 8181
    - name: management
      protocol: TCP
      port: 8182
      targetPort: 8182
  selector:
    app: polaris
