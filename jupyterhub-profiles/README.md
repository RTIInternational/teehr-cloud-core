# JupyterHub Profile List Contract

This directory defines the shared profile contract and baseline generators for
JupyterHub spawn options.

## Why this exists

`jupyterhub/garden.yaml` now supports loading profile definitions from the
`JUPYTERHUB_PROFILE_LIST_JSON` environment variable.

When this env var is set, the loaded profile list overrides the static
`singleuser.profileList` in Helm values. If it is not set, the static profile
list is used as a fallback.

## Contract (v1)

Top-level JSON can be either:

1. A list of JupyterHub profile objects.
2. An object with:
   - `version`: must be `1` when present.
   - `profiles`: a list of JupyterHub profile objects.

Minimum required key per profile:
- `display_name` (string)

Everything else is passed through to JupyterHub/KubeSpawner unchanged.

## Repository roles

This module is now split across repositories:

- `teehr-cloud-core/jupyterhub-profiles` owns:
   - Contract shape and validation expectations.
   - Generator logic (`generate_profile_list_remote.py`,
      `generate_profile_configmap_templates.py`).
   - Local payload (`profile-list.local.json`) and local ConfigMap template.
- Deployment repositories (for example `teehr-hub/jupyterhub-profiles`) own:
   - Remote project inputs (`profile-list.remote.projects.json`).
   - Generated remote payload/template used in their own remote deployment flow.

## Delivery pattern for deployment repos

Recommended pattern in a deployment repo:

1. Create a ConfigMap named `jupyterhub-profile-list`.
2. Add a key `profile-list.json` containing contract JSON.
3. Deploy that ConfigMap before `teehr-jupyterhub`.

Because the env var source is marked optional, deployments continue to work
without this ConfigMap.

## Example

See `profile-list.example.json` in this folder.

## Current in-repo payloads

For this transition period, this repo now includes environment-specific profile
payloads used by in-repo ConfigMaps:

- `profile-list.local.json`
- `profile-list.remote.json`

## Remote profile generation

`profile-list.remote.json` can be generated from a compact project spec file.
In this repo, the default source is:

- `profile-list.example.projects.json`

In deployment repos, the source is typically:

- `profile-list.remote.projects.json`

Generator:

- `generate_profile_list_remote.py`

From repo root:

```bash
python3 jupyterhub-profiles/generate_profile_list_remote.py
```

To add a new project in a deployment repo, add one object to
`profile-list.remote.projects.json` and regenerate. The generator handles
per-project `TEEHR_PROJECT_ID`, nodegroup suffixing, and optional FIRO HEFS
image choices.

## Render ConfigMap templates from JSON

The Kubernetes manifest templates in `manifests/` are generated from profile
JSON to avoid drift.

From repo root:

```bash
python3 jupyterhub-profiles/generate_profile_configmap_templates.py
```

## Pull flow for a new deployment repository

Use this sequence when a deployment repo starts from `teehr-cloud-core`:

1. Copy/pull the latest files from this directory:
   - `generate_profile_list_remote.py`
   - `generate_profile_configmap_templates.py`
   - Contract README updates relevant to profile format changes.
2. In the deployment repo, create or update
   `jupyterhub-profiles/profile-list.remote.projects.json` for that
   environment.
3. Run:
   - `python3 teehr-cloud-core/jupyterhub-profiles/generate_profile_list_remote.py --specs jupyterhub-profiles/profile-list.remote.projects.json --out jupyterhub-profiles/profile-list.remote.json`
   - `python3 teehr-cloud-core/jupyterhub-profiles/generate_profile_configmap_templates.py --base-dir jupyterhub-profiles`
4. Commit generated outputs with source changes:
   - `profile-list.remote.json`
   - `manifests/jupyterhub-profile-list-remote.configmap.yaml.tpl`
5. Run the deployment repo's remote Garden deployment flow.

## Notes

- `generate_profile_configmap_templates.py` renders local and/or remote
   templates based on whichever `profile-list.*.json` files exist.
- This lets remote-only deployment repos omit local profile assets.
