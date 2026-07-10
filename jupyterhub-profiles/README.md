# JupyterHub Profile Contract and Generators

This directory defines the profile JSON contract and contains the generator
scripts used by both local and remote deployments.

## What this affects

`jupyterhub/garden.yaml` can load profiles from
`JUPYTERHUB_PROFILE_LIST_JSON`.

- If set, it overrides static `singleuser.profileList`.
- If not set, static Helm values remain in effect.

## Contract (v1)

Top-level JSON can be either:

1. A list of JupyterHub profile objects.
2. An object with:
   - `version`: optional; if present, must be `1`.
   - `profiles`: required list of JupyterHub profile objects.

Minimum required key per profile:

- `display_name` (string)

All other keys pass through to JupyterHub/KubeSpawner.

## Ownership split

- This repo (`teehr-cloud-core/jupyterhub-profiles`):
  - Owns the contract.
  - Owns generator scripts.
  - Owns local profile assets.
- Deployment repo (for example `teehr-hub/jupyterhub-profiles`):
  - Owns remote project specs and generated remote assets.
  - Calls generator scripts through the submodule path.

## Update local deployment (in this repo)

Use this flow when changing local profiles for the local deployment assets in
this repo.

1. Update your project spec JSON (example input:
   `jupyterhub-profiles/profile-list.example.projects.json`).
2. Generate the profile list JSON:

```bash
python3 jupyterhub-profiles/generate_profile_list.py \
  --spec jupyterhub-profiles/profile-list.example.projects.json \
  --out jupyterhub-profiles/profile-list.local.json
```

3. Generate the ConfigMap template from that JSON:

```bash
python3 jupyterhub-profiles/generate_profile_configmap_templates.py \
  --spec jupyterhub-profiles/profile-list.local.json \
  --out jupyterhub-profiles/manifests/jupyterhub-profile-list-local.configmap.yaml.tpl
```

4. Commit source + generated files together.

## Update remote deployment (from a deployment repo)

Use this flow in the deployment repo, where remote configs live. The scripts
still come from this repo through the submodule path.

1. In the deployment repo, update
   `jupyterhub-profiles/profile-list.remote.projects.json`.
2. Generate remote profile list JSON:

```bash
python3 teehr-cloud-core/jupyterhub-profiles/generate_profile_list.py \
  --spec jupyterhub-profiles/profile-list.remote.projects.json \
  --out jupyterhub-profiles/profile-list.remote.json
```

3. Generate remote ConfigMap template:

```bash
python3 teehr-cloud-core/jupyterhub-profiles/generate_profile_configmap_templates.py \
  --spec jupyterhub-profiles/profile-list.remote.json \
  --out jupyterhub-profiles/manifests/jupyterhub-profile-list-remote.configmap.yaml.tpl
```

4. Commit in the deployment repo:
   - `jupyterhub-profiles/profile-list.remote.projects.json`
   - `jupyterhub-profiles/profile-list.remote.json`
   - `jupyterhub-profiles/manifests/jupyterhub-profile-list-remote.configmap.yaml.tpl`
5. Run the deployment repo's normal remote deploy flow.

## Notes

- `generate_profile_configmap_templates.py` renders one output template per run.
- The generated ConfigMap must provide `profile-list.json` before JupyterHub
  starts if you want custom profiles applied.
