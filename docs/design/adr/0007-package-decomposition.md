# ADR-0007: Decompose `teehr` by function, package by dependency weight

- **Status:** Proposed
- **Date:** 2026-11-10
- **Deciders:** TEEHR team
- **Related:** #133, [phased plan](https://claude.ai/artifact/NDYWNmX5biKrBvPuZAdsNE), ADR-0002 (metric engine), ADR-0008 (repo topology)

## Context

`teehr` today does a great deal: warehouse management, migrations, fetch/ingest, grouping,
metrics, and embedded Kubernetes/Polaris wiring. It assumes one evaluation per process, so it
cannot host a multi-tenant service as-is. The metric **science** in it is the real asset and is in
better shape than the plumbing around it. The question is not whether the science is worth keeping
(it is) but whether the package stays whole or is split — and if split, **along which axis**:
by *function* (what each part does) or by *dependency weight* (how heavy its imports are).

## Decision

**Decompose `teehr` by function first; let dependency weight decide packaging.** Two steps,
in order — not "decompose by dependency weight".

**Step 1 — draw the functional seams.** Split `teehr` into cohesive capabilities by *what they
do*, independent of what they import:

- **canonical data model** — the schema and types (series, locations, crosswalks, domains);
- **metric engine** — the kernels and dual-path execution (ADR-0002);
- **spec engine** — compile and run an evaluation spec (inputs → pair → fields → filter →
  aggregate → output), including grouping/pairing;
- **fetch / load** — ingest from external sources;
- **warehouse / catalog I/O** — read/write Iceberg via Polaris, Trino/Spark;
- **platform wiring** — multi-tenant service, migrations orchestration, K8s/Polaris/Prefect
  integration;
- **client SDK** — a thin, spec-oriented client over the API.

These are the stable units. They hold even if a dependency later gets heavier or lighter.

**Step 2 — package by dependency weight.** Apply dependency weight to those modules to decide
what is core, what is an optional extra, and what relocates to the service:

- **Dependency-light core** — the **data model** + **metric kernels** + **spec schema / state
  format** (and the pure-Python parts of the spec engine): numpy/pandas only, importable offline,
  with Spark an optional extra. This is also the **engine-neutral shared library TEEHR publishes**
  (arrays in / arrays out, no Spark/Iceberg coupling) for other engines — e.g. VeriRace — to adopt
  (ADR-0011). A pure-Python reference path is the contract; any Rust acceleration is optional and
  must not become required.
- **Optional extras** — the **Spark** execution path (ADR-0002), and **fetch/load**
  (e.g. `teehr[fetch]`), **decoupled from Spark** so a standalone user can get data in without
  the cloud. Its heavy geospatial / gridded deps (GDAL, rioxarray, exactextract, icechunk,
  virtualizarr, …) live under that extra, not in the base core. In a **deployment**, fetching
  runs as a **Prefect workflow** over the same code; a **standalone** user installs the extra
  and fetches locally.
- **Relocated to the service / cloud** — **warehouse/catalog I/O at scale** and **platform
  wiring**, out of the importable core.
- **Thin platform SDK** over the API, so clients do not re-implement the internal schema.

Function defines the modules; dependency weight is a *packaging* decision layered on top, not the
primary structure. *Where* each module physically lands (repo vs package) is ADR-0008. Nothing is
kept in its current form for its own sake.

## Consequences

- **Positive:** a light, offline-importable core (`LocalEvaluation` with no cloud deps); the
  service can host the science without dragging Spark in; clearer ownership lines. Because the
  seams are **functional**, they stay put even if a dependency's weight later changes — only the
  packaging (Step 2) would shift, not the module boundaries.
- **Negative / cost:** real refactoring to carve the package and relocate code; downstream imports
  change; local and cloud must keep behaving the same throughout.
- **Tradeoff accepted:** refactor churn, in exchange for a package that can actually back a
  multi-tenant service and a standalone light install.

## Alternatives considered

- **Keep `teehr` whole.** Cannot host the service; heavy dependency footprint for standalone
  users. Rejected.
- **Rewrite the science from scratch.** Wasteful — the metric science is the asset; extract it
  clean (ADR-0002), do not rewrite. Rejected.
- **Decompose by dependency weight as the *primary* axis.** Organizes packaging directly, but
  dependency weight is a *consequence* of function, not a structure: cutting by it first can split
  one coherent capability across the core/extra line or lump unrelated ones together, and the
  decomposition churns whenever a dependency's weight changes. Rejected as the primary axis —
  kept as the secondary packaging rule (Step 2).

## Notes

- The teehr/cloud division of responsibility is **defined in Phase 0**; kernels are extracted in
  Phase 4; the light-core split ships in Phase 9.
- Packaging follows the architecture — the physical repo/package placement is ADR-0008.
- The light core doubles as the **engine-neutral shared library** TEEHR owns and publishes for other
  engines to adopt (ADR-0011) — so its boundary must stay clean: arrays in / arrays out, no
  Spark/Iceberg/platform coupling, numpy-only reference path.
- **Open: the local fetch/load UX for standalone users** (how a `LocalEvaluation` fetches and then
  loads the data) is not yet defined — it depends on the still-unsettled standalone/local patterns
  (part of the Phase 0 teehr/cloud division). The **deployment** fetch path is settled (Prefect
  workflow through the write gateway); the **standalone** fetch path is deferred, not blocking.
