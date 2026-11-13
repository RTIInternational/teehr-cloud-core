# ADR-0013: Evaluation-system boundary and deployment ancillary services

- **Status:** Proposed
- **Date:** 2026-11-11
- **Deciders:** TEEHR team
- **Related:** #133, [phased plan](https://claude.ai/artifact/NDYWNmX5biKrBvPuZAdsNE), ADR-0001 (layout / isolation), ADR-0004 (API as access path), ADR-0008 (repo topology), ADR-0011 (contracts are the shared surface)

## Context

The platform runs as multiple deployments (`teehr-hub`, `teehr-fved`, ADR-0008) over one shared
**evaluation system** — the canonical warehouse, write gateway, API, spec engine, and metric engine.

A deployment will also want services that are specific to it and are **not** part of evaluation: a
system that **runs models** whose output is then evaluated, custom dashboards, bespoke ingests, and
other deployment-specific tools. These are convenient to run in the **same Kubernetes cluster**
(shared ingress, auth, nodes), which creates a risk: that they grow tendrils into the evaluation
system's internals — its tables, Spark jobs, Polaris, control-plane DB, internal libraries —
coupling that would erode the shared, portable core and let each deployment drift.

## Decision

**The evaluation system has a rigid boundary. Deployment-specific ancillary services are clients of
it, never parts of it — even when co-located in the same cluster.**

- **The evaluation system's only public surface is its contracts:** the API / SDK (ADR-0004), the
  evaluation spec / schema (ADR-0011), and defined extension points (e.g. metric kernels). Its
  internals — warehouse tables, Polaris, Spark jobs, the control-plane DB, internal packages — are
  private. Nothing outside reaches past the boundary.
- **Ancillary services are separate components**, owned by a deployment (they live in the deployment
  repo, ADR-0008), with their own identities, namespaces, and lifecycle. They are not built from, or
  linked against, the evaluation system's internals beyond its published client SDK. An ancillary
  service may be **shared by several deployments** (reused, not forked) — it is still a client, never
  part of the evaluation system.
- **They interact only across the boundary.** Data goes *in* through the write gateway / API —
  validated and attributed like any write (ADR-0004); results and timeseries come *out* through the
  API. A model-running service is the canonical case: it produces model output and **submits it as a
  dataset through the API**, exactly like any other data producer — the evaluation system neither
  knows nor cares that a model made it.
- **Co-location is operational, not architectural.** Sharing a cluster grants no special access. An
  ancillary service must be movable to another cluster with no change to the evaluation system.
- **Execution is separated too.** Evaluation flows (upload validation, on-demand analytics) and
  deployment flows (bespoke ingests, model runners) run on **separate Prefect work pools** on a
  shared Prefect server — so an ancillary service's jobs cannot run as, or interfere with, the
  evaluation system's own.
- **The boundary is enforced, not just documented:** the API as the sole access path (ADR-0004),
  per-service identity and project namespaces / isolation (ADR-0001), and no shared internal
  packages. An ancillary service that cannot do its job across the boundary is a signal to **extend
  the boundary** (a new API capability or extension point), not to pierce it.

## Consequences

- **Positive:** the shared core stays small, portable, and reasoned-about in isolation; deployments
  get freedom to add model runners, dashboards, and tools without forking or coupling to core; a
  clear blast radius (an ancillary service failing cannot corrupt evaluation data or state); the
  discipline that enables shared specs (ADR-0011) also keeps the core itself shareable.
- **Negative / cost:** ancillary services pay a boundary tax — they go through the API even when they
  could reach a co-located DB directly; the API and extension points must be rich enough to serve
  real ancillary needs (e.g. bulk model-output submission), or teams will be tempted to pierce the
  boundary; some per-service infra (auth wiring) is duplicated.
- **Tradeoff accepted:** boundary discipline over local convenience — a model runner that *could*
  write straight to a warehouse table instead submits through the gateway, which is exactly what
  keeps the core shared and the data validated and attributed.

## Alternatives considered

- **Let deployment services share the evaluation system's internals** (DB, tables, libraries)
  directly, since they are in the same cluster. Fast, but couples every deployment's extras to core
  internals, breaks the shared-core model, and lets deployments drift — the coupling this ADR exists
  to prevent. Rejected.
- **Require ancillary services to run in separate clusters** for hard isolation. Clean, but
  operationally wasteful and unnecessary — the boundary holds within one cluster through identity
  and the API. Rejected as over-rigid.
- **No boundary — one combined system per deployment.** Returns to per-deployment forks and kills the
  "one application, many deployments" model (ADR-0008). Rejected.

## Notes

- Builds on ADR-0004 (the API *is* the boundary), ADR-0008 (ancillary services live in the
  deployment repo, not core), ADR-0001 (project namespaces give per-service isolation), and ADR-0011
  (contracts, not internals, are the shared surface).
- A model-running system is explicitly **out of scope** for the evaluation system itself: evaluation
  ingests and scores data; *producing* that data (running models) is an ancillary concern a
  deployment may add, feeding results in through the API.
- Open: the catalogue of sanctioned **extension points** (metric kernels, dashboard embedding,
  bulk-submission endpoints) — enumerated as needed, each an addition to the boundary rather than a
  hole in it.
