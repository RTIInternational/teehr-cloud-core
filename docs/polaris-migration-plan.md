# Plan: Replace iceberg-rest with Apache Polaris (Keycloak-integrated)

## TL;DR

Replace `tabulario/iceberg-rest` with `apache/polaris`. Tight Keycloak integration via JWKS + a `PrincipalRoleMapper` that reads `realm_access.roles` from JWTs, mapping Keycloak roles directly to Polaris principal roles at runtime — no per-user Polaris registration needed. A data-driven bootstrap job creates the full namespace × privilege ACL structure from a ConfigMap. Polaris and Keycloak are the control plane; all other services are the data plane.

---

> **Implementation order for `garden deploy` to work**: Phase 7 (Garden variables) and the secrets additions from Phase 1 must be done first — before any other phase — because Garden resolves `${var.polaris.*}` template variables at render time and pods reference the new secrets at startup.

## Phase 1: Database — `polaris-pg`

1. Create `polaris-pg/manifests/polaris-pg.yaml` (static, not `.tpl` — matches `iceberg-pg` which uses `manifestFiles`, no Garden templating needed). Mirror `iceberg-pg/manifests/iceberg-pg.yaml` exactly with these substitutions:
   - All `iceberg-pg` → `polaris-pg`, all `iceberg` → `polaris` (names, labels, PVC claim)
   - `POSTGRES_DB`/`POSTGRES_USER`/`POSTGRES_PASSWORD` env vars read from `polaris-db-secrets`
   - Readiness/liveness `pg_isready` probes use `-U polaris -d polaris`
   - PVC storage: 10Gi (same as iceberg-pg)

2. Create `polaris-pg/garden.yaml` — `type: kubernetes`, `name: polaris-database`, `spec.manifestFiles: [./manifests/polaris-pg.yaml]`, depends on `deploy.secrets` — mirrors `iceberg-pg/garden.yaml` exactly with name substitution.

3. Add `polaris-db-secrets` to `secrets/secrets.local.yaml` and `secrets/secrets.remote.yaml`. **Secrets are NOT standalone K8s manifests** — the `secrets/garden.yaml` `$forEach` loop creates K8s Secrets automatically from these varfiles. Add:
   ```yaml
   polaris-db-secrets:
     data:
       database: polaris
       username: polaris
       password: polaris123   # local; use strong password in remote
   polaris-secrets:
     data:
       root-credentials: "root:secret123"   # POLARIS_ROOT_CREDENTIALS format (<username>:<password>); use strong value in remote
   trino-polaris-secrets:
     data:
       client-secret: local-trino-polaris-client-secret          # plain secret — used by keycloak-bootstrap env var
       credential: "trino-polaris:local-trino-polaris-client-secret"  # full credential string — mounted as Trino credential file
   spark-polaris-secrets:
     data:
       client-secret: local-spark-polaris-client-secret
   ```
   > **Note**: `trino-polaris-secrets` needs **two keys** because the Keycloak bootstrap job needs the plain secret value (`client-secret`) to set as the Keycloak client secret, while Trino's credential file mount needs the full `trino-polaris:<secret>` string (`credential`). Using the `credential` key for the Keycloak env var would inject the wrong value.

---

## Phase 2: Polaris Deployment — `polaris/`

4. Create `polaris/manifests/polaris-config.yaml.tpl` — ConfigMap with `polaris-server.yml`:
   - JDBC persistence → `polaris-pg`
   - OIDC issuers as a **list** (multi-realm extensible); JWKS URI internal for `local`, external for `remote`
   - **`PrincipalRoleMapper`** pointing at `realm_access.roles` in the JWT — maps Keycloak role names to Polaris principal role names at runtime. This is the critical link; without it user token pass-through grants zero privileges. Exact config key must be verified against the pinned Polaris release.

5. Create `polaris/manifests/polaris.yaml.tpl` — Deployment + Service + SA:
   - Image: `apache/polaris` pinned to a specific release tag
   - Ports 8182 (catalog REST API), port 8183 (management API); Service exposes both
   - **No IRSA annotation** — Polaris is pure metadata; S3 I/O stays with Trino/Spark
   - ServiceAccount metadata must use `${environment.namespace}` — do NOT copy the hardcoded `namespace: teehr-hub` from `iceberg-rest.yaml.tpl`
   - Local env gets MinIO credentials (same `${if environment.name == "local"}` pattern as `iceberg-rest.yaml.tpl`)
   - `POLARIS_ROOT_CREDENTIALS` from `polaris-secrets` key `root-credentials`
   - JDBC credentials from `polaris-db-secrets` (Quarkus datasource env var names must be verified against the pinned release — typically `QUARKUS_DATASOURCE_JDBC_URL`, `QUARKUS_DATASOURCE_USERNAME`, `QUARKUS_DATASOURCE_PASSWORD`)
   - **Readiness and liveness probes are required** — the `polaris-bootstrap` exec deploy depends on `deploy.polaris` and will fire immediately when Polaris is marked ready; without probes the Quarkus app may still be starting:
     ```yaml
     readinessProbe:
       httpGet:
         path: /q/health/ready
         port: 8182
       initialDelaySeconds: 20
       periodSeconds: 10
     livenessProbe:
       httpGet:
         path: /q/health/live
         port: 8182
       initialDelaySeconds: 30
       periodSeconds: 10
     ```

6. Create `polaris/garden.yaml` — `type: kubernetes`, `spec.manifestTemplates`, depends on `deploy.secrets`, `deploy.polaris-database`, `deploy.keycloak`

---

## Phase 3: Keycloak Bootstrap Updates

7. Update `keycloak-bootstrap/manifests/realm-configmap.yaml.tpl` — inside the existing `teehr-realm.json` data key:
   - Add to `roles.realm` array: `iceberg-catalog-admin`, `iceberg-namespace-public-read`, `iceberg-namespace-public-write`, `iceberg-namespace-restricted-read`, `iceberg-namespace-restricted-write`
   - Add to `groups` array:
     - `iceberg-public-readers` (realmRoles: `iceberg-namespace-public-read`)
     - `iceberg-public-writers` (realmRoles: `iceberg-namespace-public-write`, `iceberg-namespace-public-read`)
     - `iceberg-restricted-readers` (realmRoles: `iceberg-namespace-restricted-read`)
     - `iceberg-restricted-writers` (realmRoles: `iceberg-namespace-restricted-write`, `iceberg-namespace-restricted-read`)
     - `iceberg-catalog-admins` (realmRoles: `iceberg-catalog-admin`)
   - Retain existing `iceberg-user` role and group during transition
   - Add to `clients` array — **two** new confidential service account clients (not three — Polaris does not need its own Keycloak client; it validates tokens via JWKS only and does not perform token introspection or act as an OAuth2 client itself):
     - `trino-polaris` client — `serviceAccountsEnabled: true`, `secret: $(env:TRINO_POLARIS_CLIENT_SECRET)`, add `realm_access` protocol mapper to include roles in access token
     - `spark-polaris` client — `serviceAccountsEnabled: true`, `secret: $(env:SPARK_POLARIS_CLIENT_SECRET)`, same `realm_access` mapper
   - **Note on `realm_access.roles` claim**: Keycloak includes realm roles in access tokens by default, but verify against the running Keycloak version. If the Polaris `PrincipalRoleMapper` needs roles under a custom claim path, add an explicit `oidc-usermodel-realm-role-mapper` protocolMapper to the Polaris-facing clients.

8. Update `keycloak-bootstrap/manifests/bootstrap-job.yaml` — add two new `env` entries matching the existing pattern:
   - `TRINO_POLARIS_CLIENT_SECRET` from `trino-polaris-secrets` key `client-secret`
   - `SPARK_POLARIS_CLIENT_SECRET` from `spark-polaris-secrets` key `client-secret`

---

## Phase 4: Polaris Bootstrap Job — `polaris-bootstrap/`

9. Create `polaris-bootstrap/manifests/acl-config.yaml` — a ConfigMap containing a declarative ACL definition (JSON) that the bootstrap job consumes. This makes the job **data-driven**: adding a new namespace = edit this file + re-run bootstrap job, no code changes:
   ```json
   {
     "realm": "teehr",
     "catalog": "teehr",
     "catalog_admin_keycloak_role": "iceberg-catalog-admin",
     "namespaces": [
       {
         "name": "public",
         "roles": [
           {
             "keycloak_role": "iceberg-namespace-public-read",
             "polaris_principal_role": "public_reader",
             "polaris_catalog_role": "public_read_role",
             "privileges": ["TABLE_READ_DATA", "TABLE_LIST", "NAMESPACE_LIST"]
           },
           {
             "keycloak_role": "iceberg-namespace-public-write",
             "polaris_principal_role": "public_writer",
             "polaris_catalog_role": "public_write_role",
             "privileges": ["TABLE_WRITE_DATA", "TABLE_READ_DATA", "TABLE_LIST", "NAMESPACE_LIST", "CREATE_TABLE"]
           }
         ]
       },
       {
         "name": "restricted",
         "roles": [
           {
             "keycloak_role": "iceberg-namespace-restricted-read",
             "polaris_principal_role": "restricted_reader",
             "polaris_catalog_role": "restricted_read_role",
             "privileges": ["TABLE_READ_DATA", "TABLE_LIST", "NAMESPACE_LIST"]
           },
           {
             "keycloak_role": "iceberg-namespace-restricted-write",
             "polaris_principal_role": "restricted_writer",
             "polaris_catalog_role": "restricted_write_role",
             "privileges": ["TABLE_WRITE_DATA", "TABLE_READ_DATA", "TABLE_LIST", "NAMESPACE_LIST", "CREATE_TABLE"]
           }
         ]
       }
     ]
   }
   ```

10. Create `polaris-bootstrap/manifests/bootstrap-job.yaml` — reuse a Prefect image (same pattern as `prefect-workflows/manifests/load-secrets.yaml`, which uses `prefecthq/prefect:3.2.0-python3.10`; use whichever tag is current in the codebase — no new image or Dockerfile needed) running an inline Python script (`python -c |`) matching that pattern. The script:
    - Accepts `POLARIS_REALM_NAME` env var (default `teehr`); sends `X-Polaris-Realm` header on every Management API call
    - Uses stdlib (`json`, `os`) + `requests` (available in the Prefect image) — no custom image build
    - Reads ACL config from mounted `acl-config.yaml` ConfigMap
    - Creates the `teehr` catalog pointing at the warehouse (S3/MinIO path from env)
    - Iterates config to create: namespaces, catalog roles with privilege grants, principal roles, catalog role → principal role assignments
    - Creates `iceberg-catalog-admin` principal role with full catalog-level grants
    - `ttlSecondsAfterFinished: 100` — matches existing Job pattern in `load-secrets.yaml`
    - Required env vars in the Job manifest:
      - `POLARIS_MANAGEMENT_URL: http://polaris:8183` — base URL for the management API
      - `POLARIS_ROOT_CREDENTIALS` from `polaris-secrets` key `root-credentials` — used to obtain an initial Bearer token from the management API
      - `POLARIS_REALM_NAME: teehr` (or from env)
      - `CATALOG_WAREHOUSE` from `${var.polaris.catalogWarehouse}` (or hardcoded for local/remote)

11. Create `polaris-bootstrap/garden.yaml` — mirrors `keycloak-bootstrap/garden.yaml` with **two deploys in one file** (this is required so the ConfigMap exists in Kubernetes before the Job pod tries to mount it):
    ```yaml
    kind: Deploy
    type: kubernetes
    name: polaris-acl-config
    dependencies:
      - deploy.secrets
    environments:
      - local
      - remote
    spec:
      manifestFiles:
        - ./manifests/acl-config.yaml
    ---
    kind: Deploy
    type: exec
    name: polaris-bootstrap
    dependencies:
      - deploy.polaris
      - deploy.polaris-acl-config
      - deploy.keycloak-bootstrap   # logical ordering: Keycloak roles must exist before bootstrap maps them
    environments:
      - local
      - remote
    spec:
      deployCommand:
        - bash
        - -c
        - >-
          kubectl -n "${environment.namespace}" delete job polaris-bootstrap --ignore-not-found=true &&
          kubectl -n "${environment.namespace}" apply -f ./manifests/bootstrap-job.yaml &&
          kubectl -n "${environment.namespace}" wait --for=condition=Complete job/polaris-bootstrap --timeout=120s
    ```

---

## Phase 5: Trino Catalog Config Update

12. Update `trino/garden.yaml` — **two separate Deploy blocks exist (local and remote); both must be updated**. Trino is a **Helm chart deployment** (`type: helm`) with no separate manifests directory — all config goes in Helm values. Changes per block:

    **Catalog properties** (in `catalogs.iceberg` multiline string):
    - Replace `iceberg.rest-catalog.uri` value with `${var.polaris.catalogUri}`
    - Replace `iceberg.rest-catalog.warehouse` value with `${var.polaris.catalogWarehouse}`
    - Add `iceberg.rest-catalog.security=OAUTH2`
    - Add `iceberg.rest-catalog.oauth2.server-uri=${var.polaris.oauthServerUri}`
    - Add `iceberg.rest-catalog.oauth2.credential-file=/etc/trino/polaris-credential`
    - Add `iceberg.rest-catalog.oauth2.scope=openid`
    - Add `iceberg.rest-catalog.header.X-Polaris-Realm=teehr`
    - Keep `iceberg.catalog.type=rest` (unchanged)

    **Credential file delivery** — Trino catalog properties do not support env var interpolation, so the OAuth2 credential must be a mounted file. The `trino-polaris-secrets` key `credential` stores the full `trino-polaris:<secret>` string. Mount it via Helm values:
    ```yaml
    coordinator:
      extraVolumes:
        - name: polaris-credential
          secret:
            secretName: trino-polaris-secrets
            items:
              - key: credential
                path: polaris-credential
      extraVolumeMounts:
        - name: polaris-credential
          mountPath: /etc/trino/polaris-credential
          subPath: polaris-credential
          readOnly: true
    ```
    No init container needed.

    **Per-block constraints:**
    - Local block: keep existing MinIO `env` entries; keep `s3.path-style-access` and `s3.endpoint` catalog properties
    - Remote block: keep existing `serviceAccount.annotations` IRSA entry
    - Both blocks: keep all existing `accessControl` configmap rules unchanged

---

## Phase 6: Spark Session Updates

13. Update `spark_session_utils.py` — changes are **minimal and additive**, preserving all existing function signatures and behavior:
    - Add an optional `oauth2_token: str = None` parameter to `create_spark_session()` — passed through to `_configure_iceberg_catalogs()`
    - In `_configure_iceberg_catalogs()`: add OAuth2 conf.set calls at the end of the existing function body:
      - If `oauth2_token` provided (JupyterHub user token pass-through): set `rest.auth.type=oauth2` + `rest.auth.oauth2.token=<token>`
      - If absent (Prefect batch): set `rest.auth.type=oauth2`, `rest.auth.oauth2.server-uri` (from `POLARIS_OAUTH2_SERVER_URI` env), `rest.auth.oauth2.credential=spark-polaris:<secret>` (from `SPARK_POLARIS_CLIENT_SECRET` env), `rest.auth.oauth2.scope=openid`
      - Both paths: set `rest.transport.header.X-Polaris-Realm=teehr`
    - The existing `update_configs: Dict[str, str]` parameter on `create_spark_session()` remains available as an override escape hatch — no structural change needed
    - **No changes** to `_create_spark_base_session`, `_set_spark_cluster_configuration`, `_set_aws_credentials_in_spark`, `_update_configs_and_packages`, `_set_catalog_metadata`, or any other existing functions

14. Update `teehr/src/teehr/const.py` — add `POLARIS_OAUTH2_SERVER_URI` and `SPARK_POLARIS_CLIENT_SECRET` env var reads alongside existing constants.

15. Update `prefect-workflows/manifests/prefect-deployer-job.yaml` — change `REMOTE_CATALOG_REST_URI` value from `${var.iceberg.catalogUri}` to `${var.polaris.catalogUri}`.

---

## Phase 7: Garden Variables & Wiring

16. Update `project.garden.yml` — add `polaris` variable group with **local/remote divergence** (same pattern as existing `iceberg.*` divergence):
    ```yaml
    # local:
    polaris:
      catalogUri: http://polaris:8182/api/catalog
      oauthServerUri: http://keycloak-service:8080/realms/teehr/protocol/openid-connect/token
      catalogWarehouse: s3://warehouse/
      catalogType: rest
      inCluster: "true"
      catalogS3PathStyleAccess: "true"
      catalogS3Endpoint: "http://minio:9000"

    # remote:
    polaris:
      catalogUri: https://polaris.${var.hostname}/api/catalog
      oauthServerUri: https://auth.${var.hostname}/realms/teehr/protocol/openid-connect/token
      catalogWarehouse: s3://dev-teehr-iceberg-warehouse/
      catalogType: rest
      inCluster: "false"
      catalogS3PathStyleAccess: "false"
      catalogS3Endpoint: ""
    ```
    Keep existing `iceberg.*` variables during transition. The `polaris` variable group is a complete superset — once all consumers are migrated, `iceberg.*` can be removed entirely.

17. Add comment blocks in `project.garden.yml` marking **control plane** (`keycloak*`, `polaris*`, `cert-manager`) vs **data plane** modules to document the intended future cluster boundary. All cross-plane references go through `polaris.*` variable group entries — no hardcoded in-cluster hostnames in data plane configs.

---

## Phase 8: Polaris Ingress — Required

18. Update `ingress/garden.yaml` — add a new `kind: Deploy` entry `name: polaris-ingress` following the exact pattern of existing entries: `type: kubernetes`, `spec.manifestTemplates: [manifests/polaris.yaml.tpl]`, dependencies on `deploy.cert-manager`, `deploy.letsencrypt`, `deploy.cert`, `deploy.contour`, `deploy.polaris`.

19. Create `ingress/manifests/polaris.yaml.tpl` — Contour `HTTPProxy` following the exact pattern of existing manifests:
    - `fqdn: polaris.${var.hostname}`, TLS `secretName: polaris.${var.hostname}-tls`
    - Route `/api/catalog` → `polaris:8182` (catalog REST API)
    - Route `/api/management` → `polaris:8183` (management API)

---

## Phase 9: Retire iceberg-rest

20. Disable `iceberg-rest` Garden deployment after all consumers (Trino, Spark, Prefect workflows) are verified connected to Polaris. Keep `iceberg-pg` until catalog data migration is confirmed complete.

---

## Files — New

| File | Notes |
|---|---|
| `polaris-pg/garden.yaml` | Mirror `iceberg-pg/garden.yaml`; uses `manifestFiles` |
| `polaris-pg/manifests/polaris-pg.yaml` | Static manifest (no `.tpl`); mirror `iceberg-pg/manifests/iceberg-pg.yaml` |
| `polaris/garden.yaml` | Depends on `polaris-database`, `keycloak`, `secrets` |
| `polaris/manifests/polaris.yaml.tpl` | Deployment + Service + SA; no IRSA annotation |
| `polaris/manifests/polaris-config.yaml.tpl` | ConfigMap with multi-issuer `polaris-server.yml` + `PrincipalRoleMapper` |
| `polaris-bootstrap/garden.yaml` | Two deploys: `type: kubernetes` (acl-config ConfigMap) + `type: exec` (job); both with `environments: [local, remote]` |
| `polaris-bootstrap/manifests/bootstrap-job.yaml` | Reuses `prefecthq/prefect:3.4.24-python3.12`; inline Python; parameterized on `POLARIS_REALM_NAME` |
| `polaris-bootstrap/manifests/acl-config.yaml` | Declarative namespace × privilege ACL ConfigMap |
| `ingress/manifests/polaris.yaml.tpl` | Contour HTTPProxy; routes for ports 8182 and 8183 |

## Files — Modified

| File | Change |
|---|---|
| `secrets/secrets.local.yaml` | Add `polaris-db-secrets`, `polaris-secrets`, `trino-polaris-secrets`, `spark-polaris-secrets` |
| `secrets/secrets.remote.yaml` | Same four secrets with production-grade values |
| `keycloak-bootstrap/manifests/realm-configmap.yaml.tpl` | Add 5 realm roles, 5 groups, 2 confidential clients (`trino-polaris`, `spark-polaris`) |
| `keycloak-bootstrap/manifests/bootstrap-job.yaml` | Add 2 new secret env vars (`TRINO_POLARIS_CLIENT_SECRET`, `SPARK_POLARIS_CLIENT_SECRET`) |
| `trino/garden.yaml` | Both local + remote Deploy blocks: OAuth2 catalog auth + credential-file volume mount via Helm values |
| `spark_session_utils.py` | Additive: dual-path OAuth2 + `X-Polaris-Realm` header; no existing signatures changed |
| `teehr/src/teehr/const.py` | Add `POLARIS_OAUTH2_SERVER_URI`, `SPARK_POLARIS_CLIENT_SECRET` |
| `prefect-workflows/manifests/prefect-deployer-job.yaml` | Update `REMOTE_CATALOG_REST_URI` to `${var.polaris.catalogUri}` |
| `project.garden.yml` | Add `polaris` variable group + control/data plane comments |
| `ingress/garden.yaml` | Add `polaris-ingress` Deploy entry |

---

## Verification

1. `kubectl get pods` — `polaris` and `polaris-pg` both Running
2. `kubectl logs deployment/polaris` — OIDC + `PrincipalRoleMapper` config loaded; no startup errors
3. Fetch `client_credentials` token for `trino-polaris` from Keycloak → `GET https://polaris.${var.hostname}/api/catalog/v1/config` with `X-Polaris-Realm: teehr` → expect 200; confirm `realm_access.roles` present in decoded token
4. `trino --execute "SHOW SCHEMAS IN iceberg"` → teehr schema visible
5. JupyterHub Spark with user token injected → user with no namespace role gets 403 from Polaris
6. Prefect batch job → `spark-polaris` service client token accepted; `REMOTE_CATALOG_REST_URI` resolves to Polaris
7. **ACL matrix**:
   - `iceberg-namespace-public-read` member → read `public` ✓, write `public` ✗, read `restricted` ✗
   - `iceberg-namespace-public-write` member → read+write `public` ✓, `restricted` ✗
   - `iceberg-namespace-restricted-read` member → read `restricted` ✓, write `public` ✗
   - `iceberg-catalog-admin` member → full access to all namespaces ✓
8. Edit `acl-config.yaml` to add a new namespace, re-run bootstrap job → new namespace ACLs applied; no code changes required
9. Run existing `teehr/tests/` catalog operation tests

---

## Decisions

- `apache/polaris` image (Apache incubator), pinned to a specific release tag — not `latest`
- `polaris.yaml.tpl` ServiceAccount uses `${environment.namespace}` — not hardcoded `teehr-hub` like `iceberg-rest.yaml.tpl`
- Readiness/liveness probes on `/q/health/ready` and `/q/health/live` (port 8182) required so Garden waits for Polaris to be truly ready before firing `polaris-bootstrap`
- `polaris` Keycloak client removed — Polaris validates tokens via JWKS only and needs no Keycloak service account
- New dedicated `polaris-pg` PostgreSQL instance (not reusing `iceberg-pg`)
- `polaris-pg` uses `manifestFiles` (static, no `.tpl`) — matching `iceberg-pg` pattern exactly
- Secrets via `secrets/secrets.local.yaml` + `secrets/secrets.remote.yaml` varfiles — consistent with existing `$forEach` pattern; no standalone K8s Secret manifests
- Trino OAuth2 credential delivered via mounted credential-file (`iceberg.rest-catalog.oauth2.credential-file`) — avoids env var interpolation limitations in Trino catalog properties; stored as full `trino-polaris:<secret>` string, mounted via `subPath`, no init container
- Trino: `client_credentials` — access control enforced at Trino layer; Polaris sees service identity
- Spark in Prefect: `client_credentials` (`spark-polaris`) — headless batch, no user context
- Spark in JupyterHub: user token pass-through — Polaris enforces per-user namespace/table ACLs
- Polaris has **no IRSA annotation** — pure metadata service on the control plane
- Control plane: Polaris + Keycloak (future: dedicated cluster); Data plane: all other services
- All cross-plane URLs go through `polaris.*` Garden variable group — no hardcoded in-cluster hostnames in data plane configs
- `polaris-server.yml` uses an issuer allow-list (not single issuer) from day one for multi-realm extensibility
- `polaris-bootstrap` reuses `prefecthq/prefect:3.4.24-python3.12` image (already in codebase via `load-secrets.yaml`) — no new Dockerfile or image build
- `polaris-bootstrap/garden.yaml` uses **two deploys**: `type: kubernetes` (deploys `acl-config.yaml` ConfigMap) + `type: exec` (runs the job) — matches `keycloak-bootstrap` pattern exactly; required because the Job pod mounts the ConfigMap as a volume
- `trino-polaris-secrets` has **two keys**: `client-secret` (plain value, used by `keycloak-bootstrap` env var) and `credential` (full `trino-polaris:<secret>` string, mounted as Trino credential file) — the same secret provides both without duplication
- `polaris` variable group is a complete superset of `iceberg` variable group, enabling full future removal of `iceberg.*` after migration
- `polaris-bootstrap` job env vars include `POLARIS_MANAGEMENT_URL` (http://polaris:8183) and `POLARIS_ROOT_CREDENTIALS` from `polaris-secrets` — required for the script to authenticate and call the management API
- `polaris-bootstrap` exec deploy depends on `deploy.keycloak-bootstrap` for logical ordering (Keycloak roles must exist before the bootstrap job maps them)
- Trino `iceberg.rest-catalog.warehouse` updated to `${var.polaris.catalogWarehouse}` alongside the URI change
- `spark_session_utils.py` changes are additive only — new `oauth2_token: str = None` parameter; all existing callers unaffected; `update_configs` escape hatch unchanged
- `X-Polaris-Realm` header explicit in all client configs from day one
- Keycloak role taxonomy: namespace × privilege; group-based assignment; coarse `iceberg-user` retained during transition
- `PrincipalRoleMapper` in `polaris-server.yml` resolves JWT `realm_access.roles` → Polaris principal roles at runtime; no per-user Polaris principal registration needed

---

## Further Considerations

1. **Polaris image tag**: Pin to a specific release (e.g., `0.9.0`) — `apache/polaris` is under active development and `latest` may break between deployments.

2. **DB schema init**: Confirm whether Polaris auto-migrates its PostgreSQL schema on first start or requires a separate init job — check release notes for the pinned version before implementing Phase 1.

3. **Catalog data migration**: Existing tables registered in `iceberg-pg`'s JDBC catalog will not auto-appear in Polaris. The `polaris-bootstrap` job needs a migration step to re-register existing namespaces/tables, or plan for a re-ingest window before retiring `iceberg-rest` in Phase 9.

4. **OPA for Trino access control (future)**: Trino uses a single `trino-polaris` service identity so Polaris cannot enforce per-user namespace/table ACLs for Trino queries. Open Policy Agent (OPA) — a lightweight Go service on the control plane — can fill this gap. Trino's native OPA system access control plugin receives full query context (user identity, Keycloak groups, target catalog/schema/table) and evaluates Rego policies that mirror the Keycloak role taxonomy. Policy changes hot-reload via ConfigMap without Trino restarts. Would require: new `opa/` Garden module + `access-control.name=opa` in Trino config + policy ConfigMap mirroring the Phase 3 role taxonomy.

5. **Per-workflow Prefect clients (future)**: Replace single `spark-polaris` client with per-category Keycloak clients (`prefect-ingest`, `prefect-metrics`, etc.), each granted only the namespace roles it needs. Prefect deployment job templates inject credentials via workflow-specific K8s Secrets. `spark_session_utils.py` reads `SPARK_POLARIS_CLIENT_ID` from env rather than hardcoding. No changes needed to Polaris bootstrap or `acl-config.yaml`.
