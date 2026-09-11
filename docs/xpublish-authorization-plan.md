# xpublish-api Authorization Plan

Last updated: 2026-09-11

> **Status: not started.** This is the follow-up to the branch that moved
> `.pmtiles` archives behind the API. That change made every path
> *authenticated*; this one makes them *authorized*.
>
> **Already delivered** (branch `57-add-polygon-layers-to-snow-dashboard`):
> - `GET /vector-tiles/{layer}.pmtiles` serves archives from S3 with range
>   passthrough, behind the existing Keycloak gate
> - `GET /api/vector-tiles` lists available layers; the old general-purpose
>   `/api/storage/contents` S3 lister is gone
> - The browser no longer addresses object storage directly — `VITE_S3_ENDPOINT`
>   and `VITE_PMTILES_BUCKET` were removed from the frontend
> - MinIO's `warehouse` bucket anonymous-download grant revoked
>
> **Not yet true:** any user with a valid token can read any icechunk repo and
> any pmtiles layer.

---

## Goal

Let a Keycloak user's group membership decide which icechunk repos and which
`.pmtiles` layers they may read, enforced uniformly in one place.

## Design decisions

### Enforcement goes in path-based middleware, not the provider

`IcechunkDatasetProvider` cannot see the caller. The xpublish hookimpls it
implements — `get_datasets()` and `get_datatree(dataset_id, group)` — take no
request context, so a check placed inside the provider has no identity to check
against. This is the same structural fact that blocks per-user credential
vending (see [`polaris-identity-propagation-plan.md`](./polaris-identity-propagation-plan.md)).

Every xpublish-generated route carries the dataset id in the path
(`/api/datasets/{dataset_id}/tiles/…`, and likewise for EDR and zarr), so
middleware on the outer app can extract it and decide. One check covers tiles,
EDR, zarr, and the custom discovery endpoints.

Filtering `dataset_ids()` / `repo_names()` is still worth doing so users don't
see repos they can't open — but it is **presentation, not enforcement**. A user
who guesses a dataset id must still get a 403.

### Rules key off `realm_access.roles`

Polaris already maps `realm_access.roles` (derived from Keycloak group
membership) to its principal roles, and `AuthIdentity` in
[`xpublish-api/src/auth.py`](../xpublish-api/src/auth.py) already parses that
claim. Using the same claim means no new Keycloak groups and consistent
behavior between the Iceberg catalog and the gridded data.

The existing tiers are `teehr-read-only`, `teehr-read-write`, and
`iceberg-catalog-admin`.

### Default-allow, with explicit restriction

A dataset or layer not named in the rules map is readable by any authenticated
user. Repos are created dynamically by Prefect ingests, so default-deny would
make every new ingest invisible until someone edited configuration. This also
matches Polaris, which grants read access to the `teehr` namespace by default.

Flipping to default-deny is a one-line change if the tradeoff stops being worth
it. Worth revisiting if a genuinely sensitive dataset ever lands in the bucket.

---

## Phases

### Phase 0 — tests first

Write these before the implementation so enforcement is provable from the start.

1. Add a `noaccess` persona (group `basic-user` only) to
   `keycloak-bootstrap/manifests/local-users-configmap.yaml.tpl`. **This is a
   prerequisite, not a nicety:** the three existing personas (`admin`,
   `poweruser`, `user`) all carry at least `teehr-read-only`, so none of them
   can express "denied" and every test would pass trivially.
2. Add `tests/xpublish_access_test.py`, modelled on
   `tests/spark_permission_test.py`, plus a `kind: Test` entry in
   `tests/garden.yaml` depending on `deploy.xpublish-api` and
   `deploy.keycloak-local-users-bootstrap`.

Coverage:

| Case | Expected |
|---|---|
| Permitted dataset, permitted persona | 200 |
| Forbidden dataset | 403 (not 401, not 404, not 500) |
| Forbidden dataset requested by id, absent from the listing | 403 |
| `/api/dataset-keys` per persona | only permitted ids |
| `/api/vector-tiles` per persona | only permitted layers |
| No token / malformed token | 401 |
| Anonymous read direct from MinIO | fails |
| Traversal in `{layer}` | 400 or 404, never a read |

Two traps to avoid, both inherited from the existing suite:

- **Use `client_id: teehr-frontend`**, not `jupyterhub`. xpublish-api runs with
  `KEYCLOAK_ALLOWED_AUDIENCES=teehr-api,teehr-frontend`, so a jupyterhub-client
  token is rejected at the audience check and you will debug a 401 that has
  nothing to do with authorization.
- **Assert specific status codes.** `spark_permission_test.py` treats *any*
  exception as "correctly denied", which in a security test means a connection
  error or a typo passes. Pair every deny assertion with a positive assertion in
  the same run so a dead service can't look like a working control.

### Phase 1 — icechunk authorization

1. `config.py`: add `TEEHR_ACCESS_RULES` (JSON), wired through
   `xpublish-api/manifests/configmap.yaml.tpl` and `deployment.yaml.tpl` the
   same way the existing settings are.
2. New `src/authz.py`: parse the rules; expose `roles_for_dataset(dataset_id)`
   and `is_permitted(identity, dataset_id)`.
3. `main.py`: middleware that extracts `dataset_id` from `/api/datasets/{id}/…`
   and from `/api/dataset-variables/{id}`, returning 403 on deny.
4. Filter `dataset_ids()` / `repo_names()` in the provider.

### Phase 2 — pmtiles authorization

1. Apply the same rules map to `{layer}` in `/vector-tiles/{layer}.pmtiles`.
2. Filter `/api/vector-tiles` to permitted layers.

The route and its range handling already exist; this is only the decision.

### Phase 3 — close out

1. Add rows to [`access-control-matrix.md`](./access-control-matrix.md) for the
   new enforcement points.
2. Full `garden test`, then a browser check that a permitted user sees the
   polygon layer and `noaccess` does not.

---

## What this does not do

This is authorization at the API edge — **Pattern B** in
[`iceberg-auth-storage-roadmap.md`](./iceberg-auth-storage-roadmap.md). The
service identity (IRSA in remote, MinIO root locally) still reads the whole
bucket, so a leaked credential, an in-cluster pod, or a bad bucket policy
bypasses every rule here. The tests can prove an *unauthenticated* party is
blocked; they cannot prove a *credentialed* one is.

Closing that gap is Pattern C, per-user credential vending, which is deferred.
Notes for when it is picked up:

- The provider's caches (`_repos` keyed by repo name, `_cache` by `dataset_id`)
  are process-global. They are *correct* under Pattern B because everything they
  hold was read with the service identity. Under Pattern C they become an
  authorization bypass: the first user to open a repo bakes their credentials
  into a cache every later request reads through. Re-keying by identity is a
  prerequisite, not an afterthought.
- icechunk's `get_credentials` is `Callable[[], S3StaticCredentials]` — no
  arguments, invoked from the Rust runtime off the request path. Per-user
  credentials require a separate `Storage` object per identity.
- A `client_credentials` grant yields the service account's subject, identical
  for every user. Use the user's own token.
- Keycloak **token exchange with a different audience strips the
  `realm_access.roles` claim** — this was found the hard way during the Polaris
  work and is recorded in
  [`polaris-access-control.md`](./polaris-access-control.md). AWS needs a
  specific `aud` for `AssumeRoleWithWebIdentity`; that tension is unresolved.
- AWS must reach Keycloak's JWKS publicly to register it as an IAM OIDC
  provider. The in-cluster issuer URL will not work.
- Local cannot exercise any of this: `project.garden.yml` already records
  `catalogRoleArn: ""  # MinIO has no real STS` and `storageStsUnavailable: true`.

---

## References

- [`polaris-access-control.md`](./polaris-access-control.md) — the group/role
  model this mirrors
- [`polaris-identity-propagation-plan.md`](./polaris-identity-propagation-plan.md)
  — Pattern C status
- [`iceberg-auth-storage-roadmap.md`](./iceberg-auth-storage-roadmap.md) —
  Pattern A/B/C taxonomy
- [`access-control-matrix.md`](./access-control-matrix.md) — per-service
  enforcement points and the local test personas
