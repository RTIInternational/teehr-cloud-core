apiVersion: v1
kind: ConfigMap
metadata:
  name: polaris-schema
data:
  init.sql: |
    -- Idempotent schema initialization for Apache Polaris 1.5.0 (PostgreSQL)
    -- Safe to run on fresh or already-initialized databases.
    CREATE SCHEMA IF NOT EXISTS POLARIS_SCHEMA;
    SET search_path TO POLARIS_SCHEMA;

    CREATE TABLE IF NOT EXISTS version (
        version_key TEXT PRIMARY KEY,
        version_value INTEGER NOT NULL
    );
    INSERT INTO version (version_key, version_value)
    VALUES ('version', 4)
    ON CONFLICT (version_key) DO UPDATE SET version_value = EXCLUDED.version_value;
    COMMENT ON TABLE version IS 'the version of the JDBC schema in use';

    CREATE TABLE IF NOT EXISTS entities (
        realm_id TEXT NOT NULL,
        catalog_id BIGINT NOT NULL,
        id BIGINT NOT NULL,
        parent_id BIGINT NOT NULL,
        name TEXT NOT NULL,
        entity_version INT NOT NULL,
        type_code INT NOT NULL,
        sub_type_code INT NOT NULL,
        create_timestamp BIGINT NOT NULL,
        drop_timestamp BIGINT NOT NULL,
        purge_timestamp BIGINT NOT NULL,
        to_purge_timestamp BIGINT NOT NULL,
        last_update_timestamp BIGINT NOT NULL,
        properties JSONB NOT NULL DEFAULT '{}'::JSONB,
        internal_properties JSONB NOT NULL DEFAULT '{}'::JSONB,
        grant_records_version INT NOT NULL,
        location TEXT,
        location_without_scheme TEXT,
        PRIMARY KEY (realm_id, id),
        CONSTRAINT constraint_name UNIQUE (realm_id, catalog_id, parent_id, type_code, name)
    );
    -- Idempotent column migrations for databases created from older schema versions
    ALTER TABLE entities ADD COLUMN IF NOT EXISTS location TEXT;
    ALTER TABLE entities ADD COLUMN IF NOT EXISTS location_without_scheme TEXT;

    CREATE INDEX IF NOT EXISTS idx_entities ON entities (realm_id, catalog_id, id);
    CREATE INDEX IF NOT EXISTS idx_entities_catalog_id_id ON entities (catalog_id, id);
    CREATE INDEX IF NOT EXISTS idx_locations ON entities (realm_id, parent_id, location);

    CREATE TABLE IF NOT EXISTS grant_records (
        catalog_id BIGINT NOT NULL,
        privilege_type TEXT NOT NULL,
        securable_type TEXT NOT NULL,
        securable_id BIGINT NOT NULL,
        grantee_type TEXT NOT NULL,
        grantee_id BIGINT NOT NULL,
        realm_id TEXT NOT NULL,
        PRIMARY KEY (realm_id, securable_id, grantee_id, privilege_type)
    );
    CREATE INDEX IF NOT EXISTS idx_grant_records ON grant_records (realm_id, catalog_id, securable_id);
    CREATE INDEX IF NOT EXISTS idx_grant_records_grantee ON grant_records (realm_id, grantee_id);

    CREATE TABLE IF NOT EXISTS principal_authentication_data (
        realm_id TEXT NOT NULL,
        principal_id BIGINT NOT NULL,
        main_secret_hash TEXT NOT NULL,
        secondary_secret_hash TEXT,
        created_at BIGINT NOT NULL,
        PRIMARY KEY (realm_id, principal_id)
    );

    CREATE TABLE IF NOT EXISTS policy_mapping_record (
        realm_id TEXT NOT NULL,
        catalog_id BIGINT NOT NULL,
        target_type_code INT NOT NULL,
        target_id BIGINT NOT NULL,
        policy_type_code INT NOT NULL,
        policy_id BIGINT NOT NULL,
        parameters JSONB,
        PRIMARY KEY (realm_id, target_id, policy_id)
    );
    COMMENT ON TABLE policy_mapping_record IS 'stores attachments of policies to targets';
    CREATE INDEX IF NOT EXISTS idx_policy_mapping_record ON policy_mapping_record (realm_id, catalog_id, target_type_code, target_id);

    CREATE TABLE IF NOT EXISTS events (
        id BIGSERIAL PRIMARY KEY,
        realm_id TEXT NOT NULL,
        timestamp BIGINT NOT NULL,
        event_type TEXT NOT NULL,
        event_data JSONB NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_events ON events (realm_id, timestamp);

    CREATE TABLE IF NOT EXISTS idempotency_records (
        realm_id TEXT NOT NULL,
        request_id TEXT NOT NULL,
        response_code INT NOT NULL,
        response_body TEXT,
        created_at BIGINT NOT NULL,
        expires_at BIGINT NOT NULL,
        PRIMARY KEY (realm_id, request_id)
    );
    COMMENT ON TABLE idempotency_records IS 'stores idempotency records for REST requests';
    CREATE INDEX IF NOT EXISTS idx_idempotency_records ON idempotency_records (realm_id, request_id);
    CREATE INDEX IF NOT EXISTS idx_idempotency_records_expiry ON idempotency_records (expires_at);

    CREATE TABLE IF NOT EXISTS commit_metrics_report (
        id BIGSERIAL PRIMARY KEY,
        realm_id TEXT NOT NULL,
        catalog_id BIGINT NOT NULL,
        timestamp BIGINT NOT NULL,
        metrics JSONB NOT NULL
    );
    COMMENT ON TABLE commit_metrics_report IS 'stores commit metrics reports';
    CREATE INDEX IF NOT EXISTS idx_commit_metrics_report ON commit_metrics_report (realm_id, catalog_id, timestamp);

    CREATE TABLE IF NOT EXISTS scan_metrics_report (
        id BIGSERIAL PRIMARY KEY,
        realm_id TEXT NOT NULL,
        catalog_id BIGINT NOT NULL,
        timestamp BIGINT NOT NULL,
        metrics JSONB NOT NULL
    );
    COMMENT ON TABLE scan_metrics_report IS 'stores scan metrics reports';
    CREATE INDEX IF NOT EXISTS idx_scan_metrics_report ON scan_metrics_report (realm_id, catalog_id, timestamp);
---
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
      # nodeSelector:
      #   teehr-hub/nodegroup-name: core-a
      initContainers:
        - name: schema-init
          image: postgres:15
          env:
            - name: PGPASSWORD
              valueFrom:
                secretKeyRef:
                  name: polaris-db-secrets
                  key: password
            - name: PGUSER
              valueFrom:
                secretKeyRef:
                  name: polaris-db-secrets
                  key: username
          command:
            - sh
            - -c
            - |
              until pg_isready -h polaris-pg -U "$PGUSER"; do
                echo "Waiting for polaris-pg..."; sleep 2;
              done
              psql -h polaris-pg -U "$PGUSER" -d polaris -f /schema/init.sql
          volumeMounts:
            - name: polaris-schema
              mountPath: /schema
      containers:
        - name: polaris
          # TODO: pin to a specific release tag before production use (e.g., apache/polaris:1.0.0)
          image: apache/polaris:latest
          imagePullPolicy: IfNotPresent
          env:
            # Bootstrap root credentials — format: realm,clientId,clientSecret
            - name: POLARIS_BOOTSTRAP_CREDENTIALS
              valueFrom:
                secretKeyRef:
                  name: polaris-secrets
                  key: bootstrap-credentials
            # Quarkus datasource (JDBC connection to polaris-pg)
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
            # OIDC: Keycloak JWKS endpoint for JWT validation (environment-specific)
            ${if environment.name == "local"}
            - name: QUARKUS_OIDC_AUTH_SERVER_URL
              value: ${var.polaris.oauthServerUri}
            # Local MinIO S3 credentials
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
              value: ${var.polaris.oauthServerUri}
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
        - name: polaris-schema
          configMap:
            name: polaris-schema

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
