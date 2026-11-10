# ADR-0003: Analytics submission — Prefect job runner, inline deferred

- **Status:** Accepted (per #133)
- **Date:** 2026-11-10
- **Deciders:** TEEHR team
- **Related:** #133, [phased plan](https://claude.ai/artifact/NDYWNmX5biKrBvPuZAdsNE), ADR-0002 (metric engine), spike-1 (identity), spike-4 (cost/weight estimator)

## Context

Analytics today are either Jupyter one-offs (available to some users, not all) or large batch
jobs the team runs and precomputes into tables for dashboards. Neither accepts an arbitrary
request, runs it under the caller's identity, and tracks it. #133 adopts Prefect as the job
runner. An open sub-question was whether small evaluations should run **inline** (synchronously in
the API request) or always as a **job**.

## Decision

**Prefect is the job runner for user analytics.** For now, **every evaluation runs as a Prefect
Kubernetes job** under the caller's delegated identity. **Synchronous in-request ("inline")
execution is deferred** to a later phase (Phase 8), not abandoned: the spec and metric engine are
designed so inline can be added without rework.

Guardrails designed in from the start:

- Per-project concurrency limits and admission control (the shared pool runs one-at-a-time today).
- **Delegated per-user identity** for every job — not the shared `prefect-job` service identity.
- A cost/weight estimate at admission so one request cannot consume the cluster.

## Consequences

- **Positive:** one uniform, tracked submit → run → result path (staged / running / succeeded /
  failed); reuses Prefect, already operated here; no second execution path to maintain yet.
- **Negative / cost:** trivial requests pay job-startup latency until the inline path lands.
- **Tradeoff accepted:** latency on small runs now, in exchange for a single execution path and a
  smaller Phase 5. The engine groundwork (vectorized kernels + parity tests, ADR-0002) is
  front-loaded in Phase 4 so the later inline path is cheap to add.

## Alternatives considered

- **Build inline now, alongside jobs.** Rejected for now: executing metric code inside the API
  requires the engine-agnostic kernels to be mature and the API process to host compute safely —
  more than Phase 5 needs, and the kernels land in Phase 4 regardless.
- **Inline only.** Cannot cover large multi-terabyte runs; a job plane is required anyway.

## Notes

- Routing (inline vs job) and the "weight" metric are still open — see the plan's Still-open list
  and spike-4.
- Per-user identity on the job plane depends on spike-1 (identity propagation); its fallback is to
  route per-user reads through this same job plane.
