# Polaris Access Control Design

## Overview

Access to the Iceberg catalog (Polaris) is controlled through a two-layer model:

1. **Group-level access** — enforced automatically via JWT claim mapping (no sync required)
2. **Individual/table-level access** — enforced via named Polaris principals (sync required)

---

## Layer 1: Group-level access (JWT claim mapping)

Access flows through two independent mappings, not one:

1. **Keycloak**: group membership → composite **realm roles**, configured per-group in
   `keycloak-bootstrap/manifests/realm-configmap.yaml.tpl` (each group's `realmRoles` list).
   Keycloak includes a user's realm roles in the `realm_access.roles` claim of every
   token by default — this is standard Keycloak behavior, not something configured here.
2. **Polaris**: reads realm role names out of that claim and maps them to Polaris
   principal roles via a regex mapper, configured in `polaris/manifests/polaris-config.yaml.tpl`.

Polaris itself never sees Keycloak group names or paths — only the realm role names
that groups happen to be composited to. This means:

- No principal sync is needed for standard access
- Adding a user to a Keycloak group grants them the corresponding Polaris permissions
  on their **next token issuance** (no delay, no operational coupling)
- Removing a user from a group immediately revokes access

### Group → realm role → Polaris principal role mapping

| Keycloak group | Composite realm role(s) | Polaris principal role | Effect |
|---|---|---|---|
| `/teehr-read-only` | `teehr-read-only` | `teehr-read-only` | Can list and read tables in the `teehr` namespace |
| `/teehr-read-write` | `teehr-read-write`, `teehr-read-only` | `teehr-read-write`, `teehr-read-only` | Can create, read, write, and drop tables |
| `/iceberg-catalog-admins` | `iceberg-catalog-admin` | `iceberg-catalog-admin` | Full catalog management |

Realm roles not in this table (e.g. `basic-user`, `jupyter-user`) are ignored by Polaris —
note the *realm role* names are singular/unprefixed even where the *group* name (e.g.
`iceberg-catalog-admins`) isn't; don't confuse the two when tracing a permission issue.

### Polaris configuration

Configured in `polaris/manifests/polaris-config.yaml.tpl`:

```properties
quarkus.oidc.roles.role-claim-path=realm_access/roles
polaris.oidc.principal-roles-mapper.type=default
polaris.oidc.principal-roles-mapper.mappings[0].regex=^iceberg-catalog-admin$
polaris.oidc.principal-roles-mapper.mappings[0].replacement=PRINCIPAL_ROLE:iceberg-catalog-admin
polaris.oidc.principal-roles-mapper.mappings[1].regex=^teehr-(.+)$
polaris.oidc.principal-roles-mapper.mappings[1].replacement=PRINCIPAL_ROLE:teehr-$1
```

### Adding a new access tier

1. Create a Keycloak group named `/teehr-<tier>` in `keycloak-bootstrap/manifests/`, with a
   composite `realmRoles: ["teehr-<tier>"]` mapping — the realm role is what actually reaches
   Polaris, so this step is required, not just the group itself
2. Add a namespace policy for the new principal role in `polaris-bootstrap/manifests/acl-config.yaml.tpl`
3. No Polaris config change needed — the `teehr-(.+)` pattern picks it up automatically
4. Add users to the group in Keycloak

---

## Layer 2: Individual/table-level access (principal sync)

For use cases requiring finer-grained control beyond group defaults:

- Granting a specific user read access to a specific table (not the whole namespace)
- Temporary elevated access for a single user
- Audit trails tied to a named Polaris principal entity

### How it works

The `polaris-sync-principals-script` (run as part of `polaris-bootstrap`) creates a
named Polaris principal for each Keycloak user and assigns principal role bindings
based on their group membership. These bindings are **additive** — they stack on top
of the JWT-based group grants.

To grant a user access to a specific table:
1. Ensure the user has a synced principal in Polaris (sync script handles this)
2. Create a table-level catalog role with the desired privilege
3. Bind that catalog role to the user's principal role via the Polaris management API

### Why the sync is optional for basic access

Since JWT group mapping covers the common case, the sync is only needed when you
require per-principal grants. The sync can be run on-demand or on a schedule — it
does not need to run before users can access the catalog.

---

## Permission model

### Namespace-level grants (acl-config.yaml.tpl)

| Principal role | Catalog role | Privileges |
|---|---|---|
| `teehr-read-only` | `teehr_read_only_role` | `NAMESPACE_READ_PROPERTIES`, `TABLE_LIST`, `TABLE_READ_PROPERTIES`, `TABLE_READ_DATA` |
| `teehr-read-write` | `teehr_read_write_role` | All read-only + `NAMESPACE_WRITE_PROPERTIES`, `TABLE_CREATE`, `TABLE_DROP`, `TABLE_WRITE_PROPERTIES`, `TABLE_READ_DATA`, `TABLE_WRITE_DATA` |
| `iceberg-catalog-admin` | `catalog_admin_role` | `CATALOG_MANAGE_CONTENT`, `CATALOG_MANAGE_METADATA` |

### Storage

- MinIO (local) / S3 (remote): credentials configured in Polaris catalog `storageConfigInfo`
- `stsUnavailable=true` for local MinIO (no STS credential vending)
- `s3.remote-signing-enabled=false` — clients use their own configured S3 credentials

---

## Authentication paths

### Direct token (notebooks, API clients)

```
User → Keycloak password/refresh grant → JWT (jupyterhub client)
    → Spark: spark.sql.catalog.iceberg.token = {jwt}
    → Polaris: validates JWT, maps realm_access.roles (from Keycloak group
      membership) to principal roles, enforces permissions
```

### AuthManager / broker (JupyterHub spawned notebooks)

```
User logs in → JupyterHub OAuth → Keycloak issues access_token + refresh_token
    → Broker (/auth/polaris-session) stores refresh_token, returns broker_session_token
    → TeehrBrokerAuthManager (JAR) holds broker_session_token
    → On each Iceberg operation: JAR calls /auth/polaris-token/session
    → Broker refreshes token via Keycloak (refresh_token grant)
    → Returns refreshed jupyterhub JWT (preserves realm_access.roles claim)
    → Polaris: same realm_access.roles-based mapping as direct token path
```

Note: The broker does **not** do token exchange — it refreshes the user's token directly
to preserve the `realm_access.roles` claim (which is what Polaris actually reads; it's
derived from the user's Keycloak group membership, per Layer 1 above). Token exchange
with a different audience was found to strip this claim, preventing per-user permission
enforcement.
