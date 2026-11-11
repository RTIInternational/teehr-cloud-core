# Architecture Decision Records

This directory holds the Architecture Decision Records (ADRs) for the project-based
platform refactor tracked in [#133](https://github.com/RTIInternational/teehr-cloud-core/issues/133).

Each ADR captures one decision: its context, the call, and the consequences. They are the
**Phase 0 deliverable** of the [phased plan](https://claude.ai/artifact/NDYWNmX5biKrBvPuZAdsNE) —
the phase gate is "ADRs ratified by the team; all four spikes pass or have a documented fallback."
See [`../roadmap.md`](../roadmap.md) for the phase index (which ADR lands in which phase).

ADRs are cross-cutting architecture and live in `teehr-cloud-core` because it is the shared
core across deployments (`teehr-hub`, `teehr-fved`). An accepted ADR is not re-litigated; a
superseded one is marked `Superseded by ADR-NNNN` rather than edited away.

## Status values

- **Proposed** — written, awaiting team ratification.
- **Accepted** — ratified; build to it.
- **Deferred** — a deliberate decision to not decide yet, with the trigger that reopens it.
- **Superseded** — replaced by a later ADR.

## The decisions

| ADR | Title | Status |
| --- | --- | --- |
| [0001](0001-warehouse-layout.md) | Warehouse layout: project = namespace, table-per-dataset | Accepted (team call, 2026-11) |
| [0002](0002-metric-engine.md) | Metric engine: one definition, two backends | Proposed |
| [0003](0003-analytics-submission.md) | Analytics submission: Prefect job runner, inline deferred | Accepted (per #133) |
| [0004](0004-api-as-access-path.md) | API as the access path (writes now, reads later) | Proposed |
| [0005](0005-unit-handling.md) | Unit handling: canonical-on-ingest, metric-aware display | Accepted |
| [0006](0006-warehouse-migrations.md) | Warehouse migration strategy | Deferred |
| [0007](0007-package-decomposition.md) | Decompose `teehr` by dependency weight | Proposed |
| [0008](0008-repo-topology.md) | Repo & packaging topology | Proposed |
| [0009](0009-branching-and-deployment.md) | Branching & deployment strategy during the refactor | Accepted |
| [0010](0010-series-identity.md) | Series identity — `series_key`, locations as a linked layer | Proposed |
| [0011](0011-interface-parity-verirace.md) | Interface parity with VeriRace; deviate on substrate | Proposed |
| [0012](0012-temporal-extent.md) | Temporal extent — fixed periods & a general window model | Proposed |
| [0013](0013-evaluation-system-boundary.md) | Evaluation-system boundary & deployment ancillary services | Proposed |

## Phase 0 exit checklist

**ADRs ratified** — every ADR listed above moved to Accepted or Deferred by the team.

**Four spikes pass or have a documented fallback:**

1. End-user identity propagation to Trino/Spark against Polaris *(riskiest; fallback: route
   per-user reads through the job plane)*.
2. Prefix-scoped `allowed_locations` + STS vending proving S3-level isolation.
3. Analytics spec compiles and runs with vectorized-Python / Spark **parity** on a real spec.
4. Cost/weight estimator calibrated against a month of real metric runs.

**Two design artifacts drafted:**

- Analytics spec schema v0 (`../spec-schema-v0.md`).
- Target canonical warehouse data model (`../warehouse-data-model.md`) — normalized
  domains; separate variable / interval / statistic fields; canonical units.

See [`template.md`](template.md) for the ADR format.
