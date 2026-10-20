# cert-manager (Local Only)

This module exists only for local development in teehr-hub.

Scope:
- Local Kind/Garden workflow only
- Installs cert-manager locally
- Applies local issuer and certificate resources for local hostnames

Not in scope:
- Remote/shared cluster cert-manager installation
- Remote issuer lifecycle
- Platform-level certificate infrastructure ownership

Remote ownership:
- For remote environments, cert-manager is platform-owned in teehr-cloud-platform.
- Do not use this directory to manage remote cert-manager resources.

Operational note:
- This module is constrained to local environment targets in garden config.
- The chart version is kept in step with `teehr-cloud-platform/terraform/cert-manager.tf`.
  Bump both together, or local and remote drift.

Upgrading an existing local cluster:
- cert-manager does not support upgrading across multiple minor versions, so Garden's
  `helm upgrade` will not carry an old install forward across a large jump.
- The local cluster is disposable, so the simplest path is to recreate it:
  `kind delete cluster && ./kind/create_kind_cluster.sh`.
- To keep the cluster, uninstall first and let Garden reinstall:
  `helm uninstall cert-manager -n cert-manager`, then `garden deploy cert-manager`.
