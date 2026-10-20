# Polaris Integration Tests

This directory contains **Python-based integration tests** for the Polaris + Keycloak + Spark ecosystem running in KinD.

## Test Files

- `keycloak_users_test.py` — Validates user provisioning in Keycloak
- `polaris_oidc_test.py` — Validates Polaris OIDC token acceptance
- `polaris_namespace_test.py` — Validates namespace provisioning
- `polaris_roles_test.py` — Validates role-based access control setup
- `spark_session_auth_test.py` — Validates Spark session creation and write permissions

## What These Tests Do

These are **end-to-end integration tests** that validate:

1. **Keycloak User Provisioning** (`polaris-keycloak-users`)
   - Admin, user, and poweruser accounts exist
   - Users have correct group membership
   - Users can authenticate to Keycloak

2. **Polaris OIDC Integration** (`polaris-oidc-token-validation`)
   - Polaris accepts Keycloak-issued tokens
   - Token validation succeeds with correct realm header
   - JWT claims are properly mapped

3. **Namespace Provisioning** (`polaris-namespace-list`)
   - The `iceberg` catalog exists
   - The `teehr` namespace exists in the catalog
   - Root credentials can list namespaces

4. **Role-Based Access Control** (`polaris-role-acl-validation`)
   - Principal roles (`teehr-read-only`, `teehr-read-write`, `iceberg-catalog-admin`) exist
   - Catalog roles (`teehr_read_only_role`, `teehr_read_write_role`, `catalog_admin_role`) exist
   - Roles are properly bound to namespaces and privileges

5. **Spark Session Authentication** (`spark-session-auth`)
   - Poweruser can create Spark sessions with Keycloak credentials
   - Read-only user can create Spark sessions with Keycloak credentials
   - Poweruser can list tables in `iceberg.teehr` namespace
   - Read-only user can list tables in `iceberg.teehr` namespace
   - Poweruser **can create tables** in `iceberg.teehr` namespace
   - Read-only user **cannot create tables** (write denied)

## Running the Tests

### Run all tests through Garden:
```bash
garden test
```

### Run a specific test through Garden:
```bash
garden test polaris-keycloak-users
garden test polaris-oidc-token-validation
garden test spark-session-auth
```

### Run tests locally (for development):
```bash
# Each test can be run directly from the command line
python3 tests/keycloak_users_test.py
python3 tests/polaris_oidc_test.py
python3 tests/polaris_namespace_test.py
python3 tests/polaris_roles_test.py
python3 tests/spark_session_auth_test.py
```

### Run tests with verbose output:
```bash
garden test --verbose
```

## Test Execution Order

Garden automatically resolves dependencies. The test order is:

1. `polaris-keycloak-users` (runs after `deploy.keycloak-bootstrap`)
2. `polaris-oidc-token-validation` (runs after `deploy.polaris-bootstrap`)
3. `polaris-namespace-list` (runs after `deploy.polaris-bootstrap`)
4. `polaris-role-acl-validation` (runs after `deploy.polaris-bootstrap`)
5. `spark-session-auth` (runs after `deploy.polaris-bootstrap` and `deploy.spark`)

## Future Tests

### Spark Integration Tests (to implement)
- Spark session creation with Keycloak credentials
- Table creation in iceberg.teehr namespace as read-write user
- Read-only access validation (write attempt should fail)
- Namespace isolation across roles

### Trino Integration Tests (to implement)
- Trino catalog configuration validation
- Query access with different user roles
- Namespace-level permission enforcement

### End-to-End Flow (to implement)
- Full user journey: Keycloak login → JupyterHub → Spark session → Polaris access
- Data pipeline execution with role-based filtering

## Extending These Tests

To add a new test, add a new `kind: Test` block to `garden.yaml`:

```yaml
---
kind: Test
name: my-new-test
dependencies:
  - deploy.some-service
timeout: 60
spec:
  image: some-container-image
  command:
    - /bin/sh
    - -c
    - |
      # your test logic here
      exit 0  # success
      exit 1  # failure
```

### Tips for Writing Tests

- Use `set -e` to fail fast on errors
- Print progress with `echo "[test] ..."` for clarity
- Use `grep` and pipe to `/dev/null` for silent checks
- Use `curl -w "\n%{http_code}"` to capture HTTP status separately
- Resolve service names via internal DNS (e.g., `keycloak-service:8080`)
- Use environment secrets from Garden where available
