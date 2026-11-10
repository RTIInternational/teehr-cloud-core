# ADR-0008: Repo & packaging topology

- **Status:** Proposed
- **Date:** 2026-11-10
- **Deciders:** TEEHR team
- **Related:** #133, [phased plan](https://claude.ai/artifact/NDYWNmX5biKrBvPuZAdsNE), ADR-0001 (layout), ADR-0007 (decomposition)

## Context

The current pattern is deployment repos (`teehr-hub`, `teehr-fved`) plus a `teehr-cloud-core`
submodule, alongside `teehr` and the `teehr-cloud-platform` Terraform repo. The refactor rebuilds
the schema and service layer greenfield and decomposes `teehr` (ADR-0007). Where should each layer
live — an existing repo, a new repo, or a new package in an existing repo?

Two constraints shape the answer:

- **No disruption to the running system.** The rebuild must proceed alongside production; legacy
  keeps shipping unchanged until cutover.
- **Many deployments.** The same application runs as more than one deployment (`teehr-hub`,
  `teehr-fved`), each with its own users, config, and warehouse.

## Decision

- **New repo only** where a layer needs an **independent release lifecycle** or a **parallel run
  against legacy** — namely the rebuilt **service layer** (canonical schema + write gateway + API)
  and a **light `teehr-core`**.
- **A new package, not a repo,** for organizational hygiene within an existing repo.
- **Nothing new** for the substrate and IaC we keep (Polaris, Keycloak, Trino, Spark, Garden,
  `teehr-cloud-platform`) — these are **shared, not duplicated**.
- The pattern must **build shared artifacts once and configure them per deployment**
  (build-once / configure-per-deployment), with **no deployment-specific fork** of the shared
  code. The repo/submodule mechanics may change, but any replacement must preserve this.

## Consequences

- **Positive:** clean parallel-run and independent lifecycle for the rebuilt layers; the running
  system is untouched until cutover; multi-deployment is preserved by construction.
- **Negative / cost:** each new repo is another CI pipeline, auth/Garden wiring, and a thing to
  version together; the team must consciously avoid duplicating the shared substrate.
- **Tradeoff accepted:** coordination overhead of new repos, taken **only** where independent
  lifecycle or parallel-run justifies it — not for mere tidiness.

## Alternatives considered

- **Everything in the existing repos.** Entangles greenfield code with legacy and blocks a clean
  parallel-run. Rejected for the rebuilt layers.
- **Brand-new everything, including substrate.** Duplicates infrastructure we are keeping — the
  trap this ADR exists to avoid. Rejected.

## Notes

- Depends on ADR-0007 for *what* gets packaged; this ADR decides *where* it lives.
- The safety property is testable at the Phase 0 gate: the chosen topology must keep legacy
  running untouched and must not fork shared code per deployment.
