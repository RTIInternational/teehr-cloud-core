# Changelog

## Unreleased (Icechunk on Spark executors)

### Summary
- The Spark executor image can read Icechunk repos and Zarr stores, so client repos can run gridded mean areal
  tasks on executors (RTIInternational/teehr-hub#521).

### What Changed
- `spark/docker/Dockerfile.spark-executor` installs `icechunk~=2.1.2`, `xarray<2026.9` and `rioxarray~=0.23.0`
  with teehr, matching the client repos' `Dockerfile.prefect-teehr`, and checks the imports at build time.

### Why
- Large mean areal backfills (AORC, GEFS) run as Spark `mapInArrow` tasks that read the source grids directly.

## 2026-11-10 (#141 prefer IPv6 in images)

### Summary
- The xpublish-api, Jupyter driver and Spark executor images prefer IPv6 for hosts that offer it, so on a
  cluster with IPv6 egress (teehr-hub-use1) their GCS and other dual-stack traffic skips the NAT gateway
  ([#140](https://github.com/RTIInternational/teehr-cloud-core/issues/140)).

### What Changed
- New `gai.conf` in `xpublish-api/`, `jupyterhub-docker/` and `spark/docker/`, copied to `/etc/gai.conf`. The
  same file is in the teehr-hub Prefect image.

### Why
- glibc puts IPv4 first for pods, whose IPv6 address is private (`fd00::`). Clusters without IPv6 (FVED, kind)
  are unaffected.

## 2026-10-23 (backfill users into a group)

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

## 2026-10-19 (#98 deploy targets)

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
