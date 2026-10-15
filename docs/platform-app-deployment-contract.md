# Platform-App Deployment Contract

Last updated: 2026-10-15

This document defines what a deployment repository (teehr-hub, teehr-fved, ...) must provide to deploy this
repository's components to a remote cluster, and what the platform must provide first.

## Platform-Owned Remote Prerequisites

For remote environments, the following components are treated as platform-owned prerequisites:

- cert-manager installation and CRDs
- ClusterIssuer (for example `letsencrypt-prod`)
- Contour installation and CRDs

The app repository should not install these components in remote deploys. It only validates their presence and
deploys app resources that depend on them.

Current status:

- cert-manager remote ownership moved to platform repo
- contour remote ownership moved to platform repo
- autoscaler remote ownership moved to platform repo

## Deploy Targets

A deployment has one Garden environment for remote clusters, `remote`. Templates here check
`environment.name == "remote"`, so deployments shouldn't add environments to reach another cluster. Instead,
everything specific to one cluster lives in a **deploy target file**, `deploy-targets/<target>.yml` in the
deployment repo, and the deploy picks one with `DEPLOY_TARGET`:

```yaml
environments:
  - name: remote
    varfile: deploy-targets/${local.env.DEPLOY_TARGET || "<default-target>"}.yml
    variables:
      # values shared by every cluster
```

A target file is a Garden varfile. It is deep-merged over the project and environment variables and takes
precedence over the environment's `variables`, so it only needs the keys that differ between clusters.
Committing target files is fine; they hold ARNs and names, not secrets. Secrets stay in
`secrets/secrets.remote.yaml`.

### Required Keys

Garden variables consumed by this repository's remote templates:

| Key | Example |
|---|---|
| `aws.region` | `us-east-1` |
| `remoteCluster.contextArn` | `arn:aws:eks:us-east-1:111122223333:cluster/teehr-hub-use1` |
| `remoteCluster.ecrRegistry` | `111122223333.dkr.ecr.us-east-1.amazonaws.com` |
| `hostname` | `teehr.example.org` |
| `nfsServerDns` | `fs-0123456789abcdef0.efs.us-east-1.amazonaws.com` |
| `irsa.polarisRoleArn` | `arn:aws:iam::111122223333:role/teehr-hub-use1-polaris-irsa` |
| `irsa.prefectJobRoleArn` | `arn:aws:iam::111122223333:role/teehr-hub-use1-prefect-job-irsa` |
| `irsa.xpublishRoleArn` | `arn:aws:iam::111122223333:role/teehr-hub-use1-xpublish-api-irsa` |
| `polaris.catalogWarehouse` | `s3://teehr-iceberg-warehouse-use1/` |
| `polaris.catalogRoleArn` | `arn:aws:iam::111122223333:role/teehr-hub-use1-iceberg-s3-warehouse-access` |
| `icechunk.bucket` | `teehr-icechunk-warehouse-use1` |
| `pmtiles.bucket` | `teehr-icechunk-warehouse-use1` |

Values for deploy automation, under `deploy`:

| Key | Example |
|---|---|
| `deploy.clusterName` | `teehr-hub-use1` |
| `deploy.clusterRoleArn` | `arn:aws:iam::111122223333:role/teehr-hub-teehr-hub-admin` (kubectl access) |
| `deploy.actionRoleArn` | `arn:aws:iam::111122223333:role/teehr-hub-github-actions-garden-deploy` (CI OIDC role) |

The cluster ARN and region for `aws eks update-kubeconfig` are `remoteCluster.contextArn` and `aws.region`,
so they have one source.

### Optional Keys

| Key | Default behavior when unset |
|---|---|
| `irsa.jupyterRoleArn` | Jupyter's service account gets no IRSA role (no direct access to external, non-warehouse data). |

### Where the Values Come From

The teehr-cloud-platform Terraform outputs for the stack: `cluster_arn`, `ecr_registry`, `nfs_server_dns`,
`polaris_role_arn`, `prefect_job_role_arn`, `xpublish_api_role_arn`, `iceberg_s3_warehouse_rw_role_arn`,
`iceberg_s3_warehouse_readonly_role_arn`, `icechunk_warehouse_bucket`, and its `teehr-hub-*.tfvars` (bucket
names). The CI and admin roles are outputs of its `terraform/global` root.

## Validation

- `scripts/validate-deploy-contract.sh [deploy-targets/<target>.yml]` checks a target file for the required keys
  and that `remoteCluster.contextArn` matches `aws.region` and `deploy.clusterName`. With no argument it reads
  `deploy-targets/$DEPLOY_TARGET.yml`.
- `scripts/check-platform-prereqs.sh` checks the platform prerequisites (Contour/cert-manager/issuer) on the
  cluster.

The deploy workflow runs both before the Garden deployment.
