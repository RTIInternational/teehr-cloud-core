# Changelog

## Unreleased (backfill users into a group)

### Summary
- `scripts/add-users-to-group.sh` adds every existing realm user to a Keycloak group (default
  `icechunk-read-only`), for deployments enabling icechunk access rules with existing users.

### What Changed
- New `scripts/add-users-to-group.sh`, with `DRY_RUN=1`. It skips users already in the group, so it's safe to
  re-run.
- `docs/icechunk-access-control.md` and `README.md` describe the backfill steps. `README.md` and
  `docs/polaris-access-control.md` note `GROUP=teehr-read-only` for Iceberg read-only access.

### Why
- `defaultGroups` only applies to new users. teehr-hub's users all predate `/icechunk-read-only`, too many to add
  in the Keycloak console by hand.

## Unreleased (deploy targets)

### Summary
- Cluster-specific deployment values now come from a per-cluster deploy target file, so one deployment can target
  more than one cluster (RTIInternational/teehr-hub#462).

### What Changed
- `docs/platform-app-deployment-contract.md`: deploy target files (`deploy-targets/<target>.yml`, a Garden varfile
  chosen by `DEPLOY_TARGET`) replace `configs/deploy.remote-aws-srvs.yaml`; lists required and optional keys.
- `scripts/validate-deploy-contract.sh` validates a target file and checks the context ARN against the region
  and cluster name. Deployment workflows calling it need updating.
- Polaris `AWS_REGION` comes from `var.aws.region` instead of a hardcoded `us-east-2`.
- us-east-2 defaults in the project template and Spark tests changed to us-east-1.

### Why
- Adding a Garden environment per cluster would mean editing every `environment.name == "remote"` check here,
  which affects all deployments.

## 2026-07-07

### Summary
- Completed post-migration cleanup after platform infrastructure code was moved to `teehr-cloud-platform`.
- Removed platform-oriented and remote-only artifacts that no longer belong in `teehr-hub`.
- Added Garden variables in `project.garden.yml` required to support deployment to other platforms (AWS infrastructure)
- Updated docs to clarify local app workflow vs. remote platform responsibilities.

### What Changed
- Removed `teehr-hub` Terraform directory after code was moved to the platform repo.
- Removed migrated platform modules for autoscaler and contour.
- Removed remote cert-manager for remote usage in this repo while leaving the local path.
- Cleaned stale references in docs/configs and removed no-longer-needed helper/config files tied to old flow.

### Why
- Split the application code from the platform code to allow the application to more cleanly be extended to other platforms. 
- Keep `teehr-hub` focused on application deployment and local development workflows.
