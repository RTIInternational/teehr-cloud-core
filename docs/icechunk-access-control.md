# Icechunk Access Control

Last updated: 2026-10-16

How to control who can read Icechunk repos and vector-tile layers. For the design and its history, see [`icechunk-authorization-plan.md`](./icechunk-authorization-plan.md).

## How the rules work

Access works like Polaris: **a default role reads everything, and no role reads nothing.** Individual repos can then be restricted.

1. `default` names the roles that can read any repo or layer **not listed**. On fved that's `icechunk-read-only`, which every new user gets.
2. **Listing** a repo replaces the default for it: only users holding one of its listed roles can read it.
3. An **empty list** (`[]`) denies everyone, admins included.
4. Vector-tile layers (`.pmtiles` archives) work the same way, under `layers` instead of `repos`.
5. Without a `default` (hub's `{}`), unlisted items are open to every logged-in user.

| Goal | Entry | Who can read it |
|---|---|---|
| Leave a repo with the default access | *(no entry)* | users with `icechunk-read-only` |
| Share a repo with one project team only | `isnobal_boise: ["boise-snow-reader"]` | members of that team's group |
| Share with a team and admins | `isnobal_boise: ["boise-snow-reader", "admin"]` | the team and admins |
| Keep default access and add a team without the default role | `isnobal_boise: ["icechunk-read-only", "boise-snow-reader"]` | default-role holders and the team |
| Hide a repo while it's being staged | `isnobal_boise: []` | nobody (Prefect still writes it) |
| Share a polygon layer with one project team only | `rfc-basins: ["boise-snow-reader"]` under `layers` | members of that team's group |
| Remove someone's access to all unrestricted repos | remove them from `/icechunk-read-only` in Keycloak | everyone else as before |

---

## Overview

One rules map, enforced by xpublish-api, controls access to gridded data in Icechunk repos and to the PMTiles vector-tile layers shown with it. It applies in two places:

1. **The gridded dashboard.** xpublish-api returns 403 for a repo or layer the user can't read, and leaves it out of `/api/dataset-keys` and `/api/vector-tiles`.
2. **Notebooks.** xpublish-api hands out 15-minute, read-only S3 credentials that only work for the repos the user may read. AWS enforces this, so the credentials are useless for other repos even outside our code.

Comparison with Polaris:

| | Polaris (Iceberg tables) | Icechunk rules map |
|---|---|---|
| Default role | `teehr-read-only` reads the whole `teehr` namespace | `icechunk-read-only` reads every unlisted repo and layer |
| User with no data role | reads nothing | reads nothing |
| Finer control | grants per namespace or table, by role | restrictions per repo or layer, by role |
| Privileges | read, write, manage | read only (nobody writes through xpublish-api) |
| Defined in | core templates, the same for every deployment | a garden variable per deployment (`xpublish.accessRules`) |

Both read the Keycloak **realm roles** in the token's `realm_access.roles` claim, but use separate roles, so a user's Iceberg and Icechunk access can differ. New users get both default roles through the realm's `defaultGroups`.

---

## How access is decided

1. **Keycloak** maps group membership to realm roles, per group in `keycloak-bootstrap/manifests/realm-configmap.yaml.tpl`.
2. **xpublish-api** maps realm roles to access, using the rules map from the client repo's `project.garden.yml`.

| Repo or layer | Who can read it |
|---|---|
| Not listed | holders of a `default` role (everyone logged in, if there's no `default`) |
| Listed | only holders of one of its listed roles; everyone else gets 403 |

Unauthenticated requests get 401.

### The rules map

Defined per environment in the client repo's `project.garden.yml`:

```yaml
      xpublish:
        # default: roles that can read anything not listed below.
        # Listed: only these roles; [] = nobody. See teehr-cloud-core/docs/icechunk-access-control.md
        accessRules:
          default: ["icechunk-read-only"]
          repos:                          # repo name = configuration name = dashboard dataset id
            <repo>: ["<realm-role>", ...]
          layers:                         # layer id = .pmtiles file name without the extension
            <layer>: ["<realm-role>", ...]
```

- Write it as YAML, not as a quoted JSON string.
- All three keys are optional. xpublish-api fails to start if the map is malformed, including an unknown key.
- A misspelled repo or layer name can't be caught at startup: the entry matches nothing, and the real repo keeps its default access. xpublish-api logs a WARNING for any rule naming a repo or layer it can't find, so check its logs after a deploy, and check names against `/api/dataset-keys` and `/api/vector-tiles`.
- A rule can be added before its repo is ingested; it only warns until the repo appears.
- One notebook credential policy holds about a dozen restricted repos. Beyond that, the credentials route returns a 500 "too many repo rules" error, and the restricted repos need their own sub-prefix (see the design doc).

---

## Example: restrict the Boise iSnobal repo to a project team

**Goal:** only members of a new "Boise snow team" can see `isnobal_boise` and a (hypothetical) `rfc-basins` polygon layer. Everyone else keeps their default access to everything else.

### Step 1: Keycloak role and group (teehr-cloud-core)

In `keycloak-bootstrap/manifests/realm-configmap.yaml.tpl`, add a realm role and a group that grants it:

```json
      "roles": {
        "realm": [
          ...
          { "name": "boise-snow-reader" }
        ]
      },
      "groups": [
        ...
        {
          "name": "boise-snow-team",
          "realmRoles": ["boise-snow-reader"]
        }
      ],
```

- Use a new role. The default roles (`icechunk-read-only`, `teehr-read-only`) are held by everyone, so they can't restrict anything.
- Don't start the role name with `teehr-`: Polaris maps every `teehr-*` realm role to a Polaris principal role.
- The Keycloak bootstrap job applies this file on every deploy, so the role and group exist in every core deployment (fved and hub). An unused role is harmless.
- This is a core change: commit it in teehr-cloud-core, then bump the submodule in the client repo.

### Step 2: The rule (client repo)

In teehr-fved's `project.garden.yml`, for each environment that should enforce it:

```yaml
      xpublish:
        accessRules:
          default: ["icechunk-read-only"]
          repos:
            isnobal_boise: ["boise-snow-reader"]
          layers:
            rfc-basins: ["boise-snow-reader"]
```

To let admins in too, list their role as well, e.g. `["boise-snow-reader", "admin"]`. There's no built-in admin exception.

### Step 3: Deploy

```bash
garden deploy xpublish-api --force
```

Use `--force` after a change to the rules only: Garden doesn't notice a variable change used only inside a manifest template, and would skip the deploy. The rules are also written to a pod annotation, so applying them restarts the xpublish-api pods, which read the rules at start.

### Step 4: Add users to the group

In the Keycloak admin console (realm `teehr` → Users → user → Groups → Join group), add each team member to `/boise-snow-team`.

- **Locally**, test personas are declared in `keycloak-bootstrap/manifests/local-users-configmap.yaml.tpl`; add `"/boise-snow-team"` to a persona's `groups` to test with it.
- Group changes take effect on the user's **next token** (access tokens last 5 minutes), with no sync. A dashboard reload or a new notebook credential request picks it up.

### Result

| User | `isnobal_boise` | `rfc-basins` | Other repos and layers |
|---|---|---|---|
| In `/boise-snow-team` and `/icechunk-read-only` | Dashboard and notebooks | Dashboard | Yes |
| In `/icechunk-read-only` only | Not listed; 403 if requested directly | Not listed; 403 | Yes |
| In `/boise-snow-team` only | Dashboard and notebooks | Dashboard | No |
| In neither | No | No | No |
| Not logged in | 401 | 401 | 401 |

### Removing access

Remove the user from the group. It takes effect on their next token, within 5 minutes. Until then, and for up to another 5 minutes, they may still see map tiles their browser has cached (`Cache-Control: private, max-age=300`). Notebook credentials already issued keep working until they expire, at most 15 minutes.

To lift the restriction entirely, delete the entry from `accessRules` and redeploy; the repo goes back to default access.

---

## Example: deny everyone

To hide a repo from all users, for example while a new dataset is being staged, give it an empty role list:

```yaml
      xpublish:
        accessRules:
          default: ["icechunk-read-only"]
          repos:
            isnobal_boise: []
```

No role matches an empty list, so every user, admins included, gets:
- no `isnobal_boise` in the dashboard's dataset list, and 403 if it's requested directly;
- no access through notebook credentials.

Not affected:
- **Prefect flows** keep reading and writing it, because they use their own IAM role rather than credentials from xpublish-api. You can keep ingesting into a hidden repo.
- **Anyone with their own IAM access** to the bucket (see "What this does not do").

To let admins in while keeping everyone else out, use `["admin"]` instead of `[]`. Don't invent a role nobody holds (e.g. `["nobody"]`): it reads unclearly, and it stops denying everyone if that role is ever created.

---

## Adding the default role to existing users

`defaultGroups` only applies when a user is **created**. When a deployment first sets `default: ["icechunk-read-only"]`, add its existing users to `/icechunk-read-only` in the Keycloak console **before** deploying the rules. Otherwise they lose access to every repo. Then check that a backfilled user's token has `icechunk-read-only` in `realm_access.roles`.

---

## Notebooks

```
Notebook ── Keycloak access token ──▶ xpublish-api POST /api/auth/icechunk-credentials
                                        ├─ reads realm_access.roles
                                        ├─ works out which repos this user can read
                                        └─ AWS STS: 15-minute read-only S3 credentials limited to those repos
          ◀── credentials + the list of repos the user can open
```

In a notebook (dev kernel):

```python
from icechunk_access import list_icechunk_repos, open_icechunk_dataset, open_icechunk_repo

list_icechunk_repos()                          # only repos this user may read
ds = open_icechunk_dataset("ua_swann_4km")     # the repo's data as an xarray Dataset
repo = open_icechunk_repo("isnobal_boise")     # PermissionError if not permitted
```

- `list_icechunk_repos()` uses the dashboard's filtered dataset list, so listing doesn't create AWS credentials.
- `open_icechunk_repo()` asks for credentials, and renews them every 15 minutes while reading, refreshing the user's Keycloak token each time.
- If the Keycloak session has expired (idle 30 minutes, or 10 hours in total), it raises `SessionExpiredError`; restart the server from the hub control panel.
- The client is the `teehr-icechunk-access` package in the JupyterHub image (`jupyterhub-docker/icechunk_access/`). It needs teehr with `teehr.utils.keycloak_auth`, so it fails to import in older-teehr kernels.

---

## What this does not do

- **Write access.** Notebook credentials are read-only; repos are written only by Prefect flows.
- **Vector tiles in notebooks.** PMTiles archives sit outside the Icechunk prefix, so `layers` rules only affect the dashboard.
- **Virtual references in notebooks that need source credentials.** iSnobal's virtual chunks are in a bucket that needs M3Works keys, so notebooks can read iSnobal only from `/raw_data`.
- **Hide repo names in S3 listings.** A default-role user's notebook credentials can list the Icechunk prefix and see restricted repo names, though not read them.
- **Govern other credentials.** Anyone with their own IAM access to the bucket (keys from `aws configure` in `~/.aws/credentials`, `AWS_*` env vars, admin roles) bypasses the rules map.
- **Derived Iceberg data.** Mean areal values and other tables written to Iceberg are governed by Polaris, not by these rules.
- **AWS enforcement locally.** Local S3 has no STS, so locally the dashboard checks and the notebook client's `PermissionError` are the only enforcement.
- **teehr-hub.** Hub sets `accessRules: {}` and no STS, so every logged-in hub user reads every repo in the dashboard, and hub notebooks get no Icechunk credentials.
