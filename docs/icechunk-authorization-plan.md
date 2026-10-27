# Icechunk Authorization Plan

Last updated: 2026-10-16

> **Status: implemented** (teehr-cloud-core #96). The how-to for the rules is
> [`icechunk-access-control.md`](./icechunk-access-control.md); this doc covers
> the design, its limits and its history.
>
> **Delivered earlier** (branch `57-add-polygon-layers-to-snow-dashboard`), which
> made every path *authenticated*:
> - `GET /api/vector-tiles/{layer}.pmtiles` serves archives from S3 with range
>   passthrough, behind the Keycloak gate
> - `GET /api/vector-tiles` lists available layers; the old general-purpose
>   `/api/storage/contents` S3 lister is gone
> - The browser no longer addresses object storage directly: `VITE_S3_ENDPOINT`
>   and `VITE_PMTILES_BUCKET` were removed from the frontend
> - MinIO's `warehouse` bucket anonymous-download grant revoked
>
> **This plan** makes them *authorized*, and gives notebooks read access to the
> repos a user may read.

---

## Goal

Let a Keycloak user's realm roles decide which Icechunk repos and `.pmtiles`
layers they may read, from one rules map, in the gridded dashboard and in
JupyterHub notebooks.

## Design

```
dashboard ── Bearer <user JWT> ──▶ xpublish-api middleware ── 403 / serve with its own IRSA role
notebook  ── Bearer <user JWT> ──▶ xpublish-api POST /api/auth/icechunk-credentials
                                     ├─ realm_access.roles → readable repos (authz.py)
                                     └─ sts:AssumeRole(icechunk-s3-warehouse-access,
                                          RoleSessionName=<preferred_username>, 900 s,
                                          session policy narrowed to those repos)
          ◀── read-only S3 credentials + readable repos
```

In terms of [`iceberg-auth-storage-roadmap.md`](./iceberg-auth-storage-roadmap.md):
- **The dashboard is Pattern B.** xpublish-api authorizes at the API edge and
  reads with its own identity.
- **Notebooks are Pattern C.** Each user gets short-lived credentials scoped to
  what they may read, and AWS enforces it.

### Rules key off `realm_access.roles`, with a default role

The rules map is the `xpublish.accessRules` garden var, passed to xpublish-api
as `TEEHR_ACCESS_RULES` JSON:
`{"default": [...], "repos": {...}, "layers": {...}}`.

- `default` roles read every unlisted repo and layer. fved sets
  `["icechunk-read-only"]`, a realm role every new user gets through the
  `/icechunk-read-only` group in `defaultGroups`.
- A listed repo or layer replaces the default with its own roles; `[]` means
  nobody.
- Without a `default` (hub's `{}`), unlisted items are open to every logged-in
  user.

This follows Polaris's "no data role, no data", while keeping Icechunk and
Iceberg access separate per user. The role isn't named `teehr-…` because Polaris
maps every `^teehr-(.+)$` realm role to a principal role. Repos are created
dynamically by Prefect, so per-repo grants would hide every new ingest until
someone edited config; restricting by exception avoids that.

xpublish-api fails to start on a malformed map, and logs a WARNING for rules
naming a repo or layer it can't find, since a misspelled restriction leaves the
real repo readable by default.

### Dashboard: path-based middleware, fail closed

`IcechunkDatasetProvider` can't see the caller: its hookimpls
(`get_datasets()`, `get_datatree(dataset_id, group)`) get no request. So the
check is middleware on the outer app, which reads the dataset id from
`/api/datasets/{id}/…` and `/api/dataset-variables/{id}` and the layer from
`/api/vector-tiles/{layer}.pmtiles` (`authz.request_target`).

- **Fail closed.** The routes under `/api/datasets/{id}/` are listed in
  `authz.DATASET_ROUTES`. Any other path there, and xpublish's own
  `GET /api/datasets`, is a 403. When upgrading xpublish or a plugin, add its new
  routes there and in the tests.
- An id ending `_raw_data` must be readable as both repos it could mean.
- **Listings are filtered at the routes**, not in the provider (which has no
  identity): `/api/dataset-keys` and `/api/vector-tiles`. That's presentation;
  a guessed id still gets a 403.
- `/health` returns only `{"status": "ok"}`; it used to list every dataset id
  without auth.

### Notebooks: STS credential vending

xpublish-api already holds the bucket config, repo discovery and an IRSA role,
so it vends the credentials. Polaris can't: it vends only for Iceberg table
locations. The teehr-api broker isn't needed either: it exists because Spark's
JVM can't refresh a Keycloak token, while the Icechunk callback runs in Python
and refreshes with `teehr.utils.keycloak_auth.refresh_user_token_from_env()`.

- **Role.** xpublish-api's IRSA role assumes `<env>-icechunk-s3-warehouse-access`
  (read-only on the Icechunk prefix, trusted only by xpublish-api). That's role
  chaining, capped at 1 hour by AWS.
- **Session policy, two modes**, so its size grows with the rules map, not with
  the number of repos:
  - **deny mode** (the user holds a default role, or there's no `default`):
    allow the prefix, deny each listed repo they can't read;
  - **allow mode** (a `default` is set and the user doesn't hold it): allow only
    the listed repos they can read. With none, there's no STS call: the route
    returns empty `repos` and no credentials.
  - Repo prefixes always end in `/`, so `isnobal_boise` never matches
    `isnobal_boise_v2`.
  - AWS caps session policies at 2,048 characters, about a dozen repos in either
    list. Over that, the route returns a 500 with a `trace_id` rather than an STS
    error. If that's ever reached, put restricted repos under their own
    sub-prefix so one wildcard denies them all.
- **TTL 900 s, below the Keycloak idle timeout (30 min).** Icechunk calls the
  credential callback only on expiry, and each call refreshes the Keycloak token,
  which keeps the SSO session alive while reading. A TTL above the idle timeout
  would let the refresh token expire between calls.
- **Audit.** The STS session name is the user's `preferred_username`, so
  CloudTrail shows who read what. Each call logs a `trace_id` with ids and
  counts only, never credentials or tokens, and the client logs the same id.
- **Locally** (`ICECHUNK_STS_UNAVAILABLE=true`) the route returns the readable
  repos with no credentials; the client refuses other repos and uses the local
  S3 keys.
- **Client.** `jupyterhub-docker/icechunk_access/` (`list_icechunk_repos`,
  `open_icechunk_repo`, `open_icechunk_dataset`), pip-installed into the
  notebook image with `--no-deps` so it can't replace the git-pinned teehr.
  Listing uses `/api/dataset-keys`, so it doesn't create AWS credentials.

### Why this avoids the earlier Pattern C blockers

The earlier version of this doc deferred Pattern C because of these issues.
They still apply to any per-user reads inside xpublish-api, which is why the
dashboard stays Pattern B:

- The provider's caches (`_repos` keyed by repo name, `_cache` by `dataset_id`)
  are process-global. They're correct while everything is read with the service
  identity. With per-user credentials, the first user to open a repo would bake
  their credentials into a cache every later request reads through.
- Icechunk's `get_credentials` is `Callable[[], S3StaticCredentials]`, with no
  arguments, called from the Rust runtime off the request path. Per-user reads
  need a separate `Storage` per identity. Notebooks have one user per process,
  so that's not an issue there.
- A `client_credentials` grant yields the service account's subject. The
  notebook client sends the user's own token.
- Keycloak token exchange to a different audience strips `realm_access.roles`
  (see [`polaris-access-control.md`](./polaris-access-control.md)), and
  `AssumeRoleWithWebIdentity` would need that, plus a publicly reachable JWKS.
  Plain `AssumeRole` from xpublish-api's IRSA role, with a session policy, needs
  neither.

---

## What this does not do

- **Dashboard reads stay Pattern B.** xpublish-api reads with its own role, so
  its process-global caches stay correct.
- **Derived Iceberg data** (mean areal values, etc.) is governed by Polaris.
- **PMTiles aren't reachable from notebooks.** They're outside the Icechunk
  prefix; `layers` rules only affect the dashboard.
- **Restricted repo names are visible** in top-level S3 listings with deny-mode
  credentials, though not readable.
- **Virtual references that need source credentials** can't be read from
  notebooks. iSnobal's chunks are in `s3://m3w-transfer` (M3Works keys), so
  notebooks read iSnobal only from `/raw_data`.
- **Other credentials bypass the rules.** The map governs only the credentials
  xpublish-api vends. Anyone with their own IAM access to the bucket bypasses
  it: keys from `aws configure` in `~/.aws/credentials` (persisted in the
  JupyterHub home dir), `AWS_*` env vars or an admin role. The AWS default
  credential chain prefers those over the pod's IRSA role, and teehr's Spark
  setup also picks them up through botocore.
- **No AWS enforcement locally.** Local S3 has no STS.
- **teehr-hub keeps auth off.** It sets `accessRules: {}`, an empty
  `icechunk.accessRoleArn` and `storageStsUnavailable: "true"`. Enabling it needs
  a hub `icechunk-s3-warehouse-access` role assumable by
  `teehr-hub-xpublish-api-irsa` (in teehr-cloud-platform), and, if hub sets a
  `default`, backfilling hub users into `/icechunk-read-only` first.

---

## Testing

- Unit tests in `xpublish-api/tests/`: `authz_test.py` (rules),
  `access_test.py` (every route through the real app with a stub token
  validator, including fail-closed paths and `_raw_data` ids),
  `credentials_test.py` (policy modes, prefix boundaries, size limit).
- Integration test `garden test xpublish-access-enforcement`
  (`tests/xpublish_access_test.py`): creates two throwaway repos, checks each
  persona (`admin`, `poweruser`, `user`, `noaccess`) against the deployed rules,
  then deletes them. Restricted-repo checks run only if the rules list
  `authz_test_restricted`.
- Remote-only checks are manual: vended credentials get AccessDenied on a
  restricted repo and succeed on a permitted one, in both policy modes.

Traps:

- **Assert specific status codes.** `spark_permission_test.py` treats *any*
  exception as "denied", so a connection error passes. Pair every deny with an
  allow in the same run, so a dead service can't look like a working control.
- **Tokens.** xpublish-api decides by realm roles, not by client. `jupyterhub`
  tokens carry the `teehr-api` audience and are accepted (an earlier version of
  this doc said otherwise). A wrong audience gives a 401 that looks like an
  authorization failure.
- **`noaccess` must have no data roles.** The test asserts it, so a bootstrap
  change adding `defaultGroups` can't make the deny checks pass for the wrong
  reason.
- **Malformed tokens must return 401.** That relies on `auth_middleware`
  catching `resolve_identity` errors (mirroring `api/src/main.py`). If a 500
  appears, check that before suspecting the test.
- **Personal AWS credentials.** For remote checks, run
  `aws sts get-caller-identity` first: it should fail or show an assumed role,
  not an IAM user. Otherwise every repo opens with personal keys and deny checks
  pass for the wrong reason.

---

## Follow-ups

- **Shared auth package.** xpublish-api's `auth.py` is a trimmed copy of
  teehr-api's. A small core package with `KeycloakJWTValidator`, `AuthIdentity`
  and `resolve_identity` would serve both; it could also hold the three copies of
  the virtual-chunk anonymous-credentials helper (xpublish's provider, fved's
  `grid_utils`, the notebook client).
- **IMDS hop limit.** fved's EKS node groups set
  `http_put_response_hop_limit = 2`, so pods can reach the node IAM role. It
  can't read the warehouse buckets, but it includes ECR push and SSM Parameter
  Store read.

---

## References

- [`icechunk-access-control.md`](./icechunk-access-control.md): how to use the
  rules
- [`polaris-access-control.md`](./polaris-access-control.md): the group and role
  model this mirrors
- [`polaris-identity-propagation-plan.md`](./polaris-identity-propagation-plan.md)
- [`iceberg-auth-storage-roadmap.md`](./iceberg-auth-storage-roadmap.md):
  Pattern A/B/C
- [`access-control-matrix.md`](./access-control-matrix.md): per-service
  enforcement points and the local test personas
