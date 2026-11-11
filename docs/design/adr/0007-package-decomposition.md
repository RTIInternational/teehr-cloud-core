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
- **Spark/catalog I/O moves into the cloud repo**, out of the core (Spark stays an optional extra
  for the kernels, ADR-0002).
- **Fetch/load stays available to the light core as an optional extra** (e.g. `teehr[fetch]`),
  **decoupled from Spark** — a standalone user needs it to get data in, so it must **not** be
  cloud-exclusive. Its heavy geospatial / gridded dependencies (GDAL, rioxarray, exactextract,
  icechunk, virtualizarr, …) live under that extra, not in the base core. In a **deployment**,
  fetching runs as a **Prefect workflow** that uses the same fetch code; a **standalone** user
  installs the extra and fetches locally.

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
- **Open: the local fetch/load UX for standalone users** (how a `LocalEvaluation` fetches and then
  loads the data) is not yet defined — it depends on the still-unsettled standalone/local patterns
  (part of the Phase 0 teehr/cloud division). The **deployment** fetch path is settled (Prefect
  workflow through the write gateway); the **standalone** fetch path is deferred, not blocking.
