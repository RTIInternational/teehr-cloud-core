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
- It connects with the Polaris **root** credentials, like the `polaris-bootstrap`
  Job. Replace them with a narrower principal before this runs outside KinD.

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

It uses the Keycloak **admin** login (like the bootstrap Job and the CronJob).
Replace it with a dedicated service account before this runs outside KinD.

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
