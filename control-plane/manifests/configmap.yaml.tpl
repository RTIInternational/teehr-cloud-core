apiVersion: v1
kind: ConfigMap
metadata:
  name: teehr-control-plane-config
  labels:
    app: teehr-control-plane
    component: control-plane
data:
  # Same identity settings as teehr-api (api/manifests/configmap.yaml.tpl).
  KEYCLOAK_ISSUER_URL: "https://auth.${var.hostname}/realms/teehr"
  KEYCLOAK_JWKS_URL: "http://keycloak-service:8080/realms/teehr/protocol/openid-connect/certs"
  # teehr-control-plane-jobs: tokens a job acts as its caller with (token
  # exchange, ADR-0014) are issued to the exchange client.
  KEYCLOAK_ALLOWED_AUDIENCES: "teehr-api,teehr-frontend,teehr-frontend-local-dev,jupyterhub,teehr-control-plane-docs,teehr-control-plane-jobs"
  # Public client the /docs page signs in with.
  KEYCLOAK_DOCS_CLIENT_ID: "teehr-control-plane-docs"
  # Realm roles with full rights on every project (very few people), and roles that
  # can see every project and its members but change nothing.
  CONTROL_PLANE_ADMIN_ROLES: "control-plane-admin"
  CONTROL_PLANE_VIEWER_ROLES: "control-plane-viewer"
  # The control plane's own Keycloak client; the secret is in control-plane-secrets.
  CONTROL_PLANE_CLIENT_ID: "teehr-control-plane"
  # A separate client used only for token exchange (load jobs act as their caller);
  # its secret is control-plane-secrets jobs-client-secret.
  CONTROL_PLANE_EXCHANGE_CLIENT_ID: "teehr-control-plane-jobs"
  # Who may create projects (realm roles). Empty = any signed-in user; a dark launch
  # sets the platform admin role (var.controlPlane.projectCreatorRoles).
  PROJECT_CREATOR_ROLES: "${var.controlPlane.projectCreatorRoles || ''}"
  # Polaris management API, for provisioning project namespaces and roles. The
  # token comes from Keycloak; Polaris maps it to the named principal
  # service-account-teehr-control-plane (polaris-bootstrap acl-config).
  POLARIS_URL: "http://polaris:8181"
  POLARIS_TOKEN_URL: "http://keycloak-service:8080/realms/teehr/protocol/openid-connect/token"
  POLARIS_REALM: "${var.polaris.defaultRealm}"
  POLARIS_CATALOG: "${var.polaris.defaultRealm}"
  # Each project's namespace is pinned to <warehouse>/<project>/.
  POLARIS_WAREHOUSE: "${var.polaris.catalogWarehouse}"
  # Object storage as Polaris's vended credentials reach it (dataset I/O as the
  # caller): RustFS on local.
  STORAGE_S3_ENDPOINT: "${var.polaris.catalogS3Endpoint}"
  STORAGE_S3_REGION: "${var.polaris.catalogS3Region}"
  STORAGE_S3_PATH_STYLE: "${var.polaris.catalogS3PathStyleAccess}"
  # System projects (public, platform): every user reads (the default-group role);
  # only ingest's prefect-polaris service account writes.
  SYSTEM_PROJECT_READER_ROLES: "teehr-read-only"
  SYSTEM_PROJECT_WRITER_ROLES: "teehr-platform-write"
  # Keycloak admin API (teehr realm only), for the realm roles that carry project
  # access.
  KEYCLOAK_ADMIN_URL: "http://keycloak-service:8080"
  KEYCLOAK_REALM: "teehr"
  # Upload staging (Phase 3): a bucket only the control plane reaches. On local,
  # RustFS; on AWS, var.controlPlane.stagingBucket, reached with the service
  # account's IAM role. Empty = uploads disabled.
  STAGING_BUCKET: "${environment.name == 'local' ? 'uploads' : (var.controlPlane.stagingBucket || '')}"
  STAGING_S3_ENDPOINT: "${environment.name == 'local' ? 'http://local-s3:9000' : ''}"
  # Upload URLs are signed for the address clients use: on local, the S3 ingress
  # (so a laptop can PUT); on AWS, S3 itself.
  STAGING_S3_PUBLIC_ENDPOINT: "${environment.name == 'local' ? 'https://s3.' + var.hostname : ''}"
  # Jobs users start through the API (upload loads now; ADR-0014): Kubernetes Jobs
  # running the control plane's own image in this namespace. Jobs also need
  # JOB_TOKEN_KEY (control-plane-secrets); without it, ingest says not configured.
  JOB_IMAGE: "${actions.build.teehr-control-plane.outputs.deploymentImageId}"
  JOB_NAMESPACE: "${environment.namespace}"
  # Job pod sizing, per deployment (var.controlPlane.job*, .runRowsPerShard);
  # empty: the code's defaults (1Gi request, 4Gi limit, no CPU request, DuckDB's
  # threads, shards sized from memory).
  JOB_MEMORY_REQUEST: "${var.controlPlane.jobMemoryRequest || ''}"
  JOB_MEMORY_LIMIT: "${var.controlPlane.jobMemoryLimit || ''}"
  JOB_CPU_REQUEST: "${var.controlPlane.jobCpuRequest || ''}"
  JOB_THREADS: "${var.controlPlane.jobThreads || ''}"
  # Where job pods run (kubectl label / taint syntax), e.g.
  # "teehr-hub/nodegroup-name=driver-r5-2xlarge" and
  # "teehr-hub/dedicated=worker:NoSchedule". Empty: any untainted node.
  JOB_NODE_SELECTOR: "${var.controlPlane.jobNodeSelector || ''}"
  JOB_TOLERATIONS: "${var.controlPlane.jobTolerations || ''}"
  RUN_ROWS_PER_SHARD: "${var.controlPlane.runRowsPerShard || ''}"
  # Runs across pods (ADR-0017): at most this many pods a run, an extra pod per
  # this many shards. Empty: one pod (off), 8.
  JOB_RUN_MAX_PODS: "${var.controlPlane.runMaxPods || ''}"
  RUN_SHARDS_PER_POD: "${var.controlPlane.runShardsPerPod || ''}"
  CONTROL_PLANE_INTERNAL_URL: "http://teehr-control-plane:8000"
