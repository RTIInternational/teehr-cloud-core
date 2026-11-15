# control-plane

The project control plane (Phase 1, epic #148): projects, members, datasets and
runs, plus the provisioning of each project's Polaris namespace, roles and storage
prefix. Design: `docs/design/adr/0001-warehouse-layout.md`, `docs/design/system-layout.md`.

## Kept apart on purpose

- **Separate deployable** (`teehr-control-plane`), local only until the Phase 1 gate.
  It does not change `teehr-api`.
- **Own Postgres schema** (`control_plane`) on the shared `app-pg`. Nothing here
  touches `api/`'s tables.
- **Migrations are Alembic**, run by the Deployment's init container
  (`alembic upgrade head`), never at app startup.

To remove it: delete the Garden actions in this directory, then
`DROP SCHEMA control_plane CASCADE` on `app-pg`.

## Provisioning

Creating a project provisions, in Polaris: a namespace named after the project, and
for each project role (read/write/admin) a principal role
`teehr-project-<name>-<role>` → catalog role `project_<name>_<role>` → grants on
that namespace. The response's `status` is `ready`, or `failed` with the reason in
`status_detail`; `POST /v1/projects/{name}/provision` retries, and pending/failed
projects are also retried when the service starts.

- Every namespace the control plane creates is tagged with the project's id
  (`teehr.control-plane.project-id`). An existing namespace is never claimed or
  dropped unless the tag matches, so a project can't take over `teehr`,
  `schema_evolution`, or anything else created outside it.
- Deleting a project drops its namespace only when empty; otherwise 409.

## System projects

`public` and `platform` (ADR-0001) are recorded and provisioned every time the
service starts, as new namespaces under their own prefixes. They start empty: the
legacy `teehr` namespace is untouched, and its data is copied into `public` later
(#165).

- **Read**: every user, via `SYSTEM_PROJECT_READER_ROLES` (`teehr-read-only`, the
  Keycloak default group).
- **Write**: `SYSTEM_PROJECT_WRITER_ROLES` (`teehr-platform-write`, held only by
  ingest's `prefect-polaris` service account).
- Platform admins manage them (edit, members, re-provision); nobody can delete them.

The grants follow these settings exactly on every restart: a role removed from a
setting loses the access.

## Platform roles

- `control-plane-admin` (group `control-plane-admins`, `CONTROL_PLANE_ADMIN_ROLES`):
  edit, re-provision or delete any project and its members. Keep it to very few
  people.
- `control-plane-viewer` (group `control-plane-viewers`,
  `CONTROL_PLANE_VIEWER_ROLES`): see every project and its members, change nothing.
  Project metadata only: it gives no access to a project's data.

On KinD, `admin` is a control-plane admin and `poweruser` a viewer. The full access
review is #167.

## Membership is data access

Each member gets exactly the role for their membership, in two places, updated as
soon as membership changes (add, role change, remove, project delete):

- **Keycloak**: realm role `teehr-project-<name>-<role>` - in their tokens.
- **Polaris**: the same-named principal role, bound to their named principal
  (their username). Polaris uses a named principal's bindings rather than the
  token, so this is what grants or removes access immediately.

Add members by username with `POST /v1/projects/{name}/members`. If an update can't
be applied, the membership is still saved, the project goes `failed`, and
`POST /v1/projects/{name}/provision` (or the next restart) re-applies every member's
access. The `polaris-principal-sync` CronJob is the backstop: it also revokes
project bindings a user no longer holds in Keycloak.


## Datasets (Phase 2)

`/v1/projects/{name}/datasets` registers, lists, gets and deletes a project's
datasets. A dataset is one Iceberg table in the project namespace, with declared
columns (`string`, `int32`, `int64`, `float32`, `float64`, `boolean`, `date`,
`timestamp`, `timestamptz`, `binary`, `decimal(5,4)`).

- **Timeseries** (`type: timeseries`, `docs/design/warehouse-data-model.md`):
  register `role`, `variable`, `statistic`, `period` (default `instant` for
  `inst`), `optional_columns` (`reference_time`, on for forecasts; `member`;
  `quantile`; `quality`), `extra_columns` and `time_partition` (`month`, `year`,
  `none`). The API builds the columns, key, partitioning (on `reference_time` for
  forecasts, else `value_time`), sort order and Iceberg properties, and checks
  the metadata against the domains. Values are stored in the variable's canonical
  unit. Each gets a `<name>_series` series table (one row per `series_key`:
  location, layer, period override, resolution, `series_attributes`) unless
  `series_table` names an existing one to share. Deleting the last timeseries
  that uses a series table deletes it too.

- The API creates and drops tables **as the caller**: their own token goes to
  Polaris (PyIceberg, vended credentials), so Polaris enforces their project role.
  The API never touches project data with its own identity.
- `key_columns` (optional) name the columns that identify a row; they become the
  table's Iceberg identifier fields and are what upserts match on (load jobs).
- Register needs the write role, delete needs admin, list/get need read.
- Untyped datasets (`type: table`, the default) are a test path, not part of the
  data model. Only platform admins may register them
  (`UNTYPED_DATASETS_FOR_MEMBERS=true` opens it to project members).
- Polaris only activates roles the token carries. A token issued before the caller
  joined the project gets `403` with `code: token_refresh_required`: get a new
  token and retry.
- **Writes**: `POST .../datasets/{dataset}/data` with an Arrow IPC stream
  (`application/vnd.apache.arrow.stream`), Parquet (`application/vnd.apache.parquet`),
  NDJSON (`application/x-ndjson`) or a JSON array of objects (`application/json`)
  body. JSON is parsed with the declared column types: `timestamptz` values need a
  zone offset (`Z`, `+05:00`), or pass `timezone=America/Denver` to read times
  without one in that zone (stored as UTC; daylight-saving gaps and repeats are
  refused). CSV files go through staged upload (Phase 3).
  `mode=upsert` replaces rows with the same key and adds the rest (the default for
  timeseries and series tables, which refuse `append`); `mode=append` (the default
  for untyped datasets); or `mode=overwrite`, optionally with a `filter`
  (e.g. `location_id = 'usgs-01'`) to replace only matching rows. A batch with
  repeated key values is refused for upserts and typed datasets. Data is checked
  against the declared columns (lossless type conversion only; required columns
  can't be null) and committed as the caller in one snapshot. Bodies are capped
  (`MAX_WRITE_BYTES`, 64 MiB): bulk loads go through staged upload and a load job
  (Phase 3).
- **Timeseries writes**: `unit=` (UCUM) says what unit `value` is in; values are
  converted to the dataset's unit (through SI) and a unit of another dimension is
  refused. Without it, values are taken as already in the dataset's unit. Series
  keys not yet in the series table are added first (with the dataset's default
  layer; existing series rows are left alone). A timeseries write sends every
  column of the dataset (null where there's no value): a row is a whole record, so
  a revised record replaces the old one, quality code included, and leaving a
  column out is refused rather than wiping it. A series-table upsert changes only
  the columns sent; the rest keep their values.
- **Reads**: `GET .../datasets/{dataset}/data`, as the caller, a page at a time:
  `columns` (comma-separated), `filter` (same syntax as writes), `limit`, `cursor`,
  `count=true`. JSON `{rows, next_cursor, total}` by default; Arrow or Parquet when
  `Accept` asks, with `X-Next-Cursor` / `X-Total-Rows` headers. The cursor pins the
  snapshot the first page read, so a read isn't disturbed by writes landing
  meanwhile; every page says which in `X-Snapshot-Id`. Pages are capped per format
  (`MAX_PAGE_ROWS_JSON` 10k, `MAX_PAGE_ROWS_BINARY` 1M). Each page re-scans up to
  its offset, so deep paging of big tables is slow: bulk reads belong in jobs.
- **Lineage**: every write records who, when, mode, filter, row count, the
  Iceberg snapshot and, for a timeseries, the unit values arrived in and how many
  series it added; `GET .../datasets/{dataset}/writes` lists them.
- Today's access (Jupyter, Trino/teehr-api, the `teehr` namespace) is unchanged.

## Domains

`/v1/domains/units`, `/v1/domains/periods` and `/v1/domains/variables` list and
add the domain rows a dataset's metadata must name. Public rows (seeded by
migration: UCUM units with their affine conversion to SI, the five shared periods
of ADR-0012, a starting set of variables) are readable by anyone and added by
platform admins. A project's own rows (`?project=` to list, `"project"` in the body
to add) are readable by its readers and added by its writers. A name is defined
once across public and a project's rows. Rows can't be edited or deleted through
the API yet.

## Errors

Every error is RFC 9457 problem details (`application/problem+json`):
`{type, title, status, detail}`, plus `code` where a client can act on it
(`token_refresh_required`) and, on a 422, `errors` listing every problem
(`loc`, `msg`, `type`). The same shape as VeriRace's API.

## Its own identity

The control plane signs in as itself: the confidential Keycloak client
`teehr-control-plane` in the `teehr` realm (client credentials; secret in
`control-plane-secrets`). One token serves both systems:

- **Keycloak admin API**: the client's service account holds `manage-realm`,
  `view-realm`, `manage-users`, `view-users` and `query-users` on the `teehr` realm
  only (realm roles are realm configuration, hence `manage-realm`). No master-realm
  or cross-realm access.
- **Polaris**: the token maps to the named principal
  `service-account-teehr-control-plane`, bound to `service_admin`
  (`polaris-bootstrap` acl-config). Polaris activates a role only when the token
  also carries it, so the service account holds the realm role
  `polaris-service-admin`, which Polaris's role mapper turns into
  `PRINCIPAL_ROLE:service_admin`. Neither the binding nor the realm role alone is
  enough. Polaris 1.8 has no narrower option: creating
  principal roles and binding them to principals are service-level operations only
  `service_admin` can perform. It is a separate, rotatable identity, not root.

The client is enabled, and has those roles, only in the local environment.

## Try it in a browser (KinD)

1. Add `127.0.0.1 control-plane.teehr.local.app.garden` to `/etc/hosts`.
2. Open <https://control-plane.teehr.local.app.garden/docs> and accept the
   self-signed certificate.
3. **Authorize** → `KeycloakLogin` → sign in (e.g. `user` / `user`). Every endpoint
   then runs as you. You can also paste a raw access token under `BearerAuth`.

The login uses the public `teehr-control-plane-docs` Keycloak client, which is
enabled only in the local environment.

## Develop

```bash
cd control-plane
python3 -m venv .venv
.venv/bin/pip install -e '.[dev]'
.venv/bin/pytest
.venv/bin/ruff check . && .venv/bin/ruff format --check .
garden deploy teehr-control-plane --sync
```

Without `CONTROL_PLANE_DB_HOST` / `CONTROL_PLANE_DB_URL` the service uses a local
SQLite file, which is also what the tests use.
