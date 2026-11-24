# Gridded Repo Contract

Moved to teehr-icechunk: [`spec/gridded-repo-contract.md`](https://github.com/RTIInternational/teehr-icechunk/blob/main/spec/gridded-repo-contract.md),
versioned with the package that writes and reads the repos (ADR-0021).

Until xpublish-api (`xpublish-api/src/provider.py`) and `jupyterhub-docker/icechunk_access` import the package's
reader, they keep their own copies of the contract's names; change them together with the spec.
