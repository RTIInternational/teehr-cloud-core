# ADR-0007: Decompose `teehr` by dependency weight

- **Status:** Proposed
- **Date:** 2026-11-10
- **Deciders:** TEEHR team
- **Related:** #133, [phased plan](https://claude.ai/artifact/NDYWNmX5biKrBvPuZAdsNE), ADR-0002 (metric engine), ADR-0008 (repo topology)

## Context

`teehr` today does a great deal: warehouse management, migrations, fetch/ingest, grouping,
metrics, and embedded Kubernetes/Polaris wiring. It assumes one evaluation per process, so it
cannot host a multi-tenant service as-is. The metric **science** in it is the real asset and is in
better shape than the plumbing around it. The question is not whether the science is worth keeping
(it is) but whether the package stays whole or is split.

## Decision

**Decompose `teehr` by dependency weight**, rather than preserve it whole:

- A **dependency-light core** — the metric kernels (ADR-0002) and the canonical data model —
  numpy/pandas only, importable offline, with Spark an optional extra.
- A **thin platform SDK** over the API (spec-oriented), so clients do not re-implement the
  internal schema.
- **Spark/catalog I/O and heavy fetch/ingest move into the cloud repo**, out of the core.

The open question is **where each piece lands** (ADR-0008), not whether to keep the current
all-in-one package. Nothing is kept in its current form for its own sake.

## Consequences

- **Positive:** a light, offline-importable core (`LocalEvaluation` with no cloud deps); the
  service can host the science without dragging Spark in; clearer ownership lines.
- **Negative / cost:** real refactoring to carve the package and relocate code; downstream imports
  change; local and cloud must keep behaving the same throughout.
- **Tradeoff accepted:** refactor churn, in exchange for a package that can actually back a
  multi-tenant service and a standalone light install.

## Alternatives considered

- **Keep `teehr` whole.** Cannot host the service; heavy dependency footprint for standalone
  users. Rejected.
- **Rewrite the science from scratch.** Wasteful — the metric science is the asset; extract it
  clean (ADR-0002), do not rewrite. Rejected.

## Notes

- The teehr/cloud division of responsibility is **defined in Phase 0**; kernels are extracted in
  Phase 4; the light-core split ships in Phase 9.
- Packaging follows the architecture — the physical repo/package placement is ADR-0008.
