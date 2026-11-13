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
