# Platform refactor roadmap (phase index)

- **Status:** Phase 0 gate met — ADRs ratified 2026-11-13; four spikes resolved; design artifacts drafted. Next: Phase 1.
- **Date:** 2026-11-11
- **Issue:** [#133](https://github.com/RTIInternational/teehr-cloud-core/issues/133)

> **Canonical source:** the full, team-facing plan — with goals, gates, risks, and open
> decisions in detail — lives in the **[Phased Plan artifact](https://claude.ai/artifact/NDYWNmX5biKrBvPuZAdsNE)**.
> This file is a concise, version-controlled index so the ADRs' "Phase N" references resolve
> inside the repo. Where the two differ, the artifact is authoritative.

## Approach

Ten gated phases, each usable on its own and each unlocking the next. Breaking changes are
accepted (per the team's call in #133); the one hard constraint is **we do not lose the
existing warehouse data** — it is preserved and re-homed as the public `core` project.

## The phases

| Phase | Title | In one line |
| --- | --- | --- |
| **0** | Foundations and decisions | Ratify the ADRs; run the four de-risking spikes; draft the spec schema + canonical data model. *(This phase.)* |
| **1** | Project control plane | Project namespaces + prefix-scoped storage. |
| **2** | Identity propagation & write gateway | Data plane knows who is asking; API/SDK is the only write path; one schema owner. **Hard prerequisite for 3 and 5.** |
| **3** | User data upload & registration | Async upload + dataset registration. |
| **4** | Analytics spec & dual-path metrics | One dual-path metrics engine (vectorized-Python default, Spark for large) + the evaluation spec. |
| **5** | On-demand analytics via Prefect | Submit → track → results; every run is a Prefect K8s job under the caller's delegated identity; per-project quotas. |
| **6** | Clients & Jupyter reconciliation | SDK / CLI / MCP; Jupyter goes through the gateway. |
| **7** | Migrate core, harden, cut over | Port data (not code) to the new model; run legacy alongside until parity; retire legacy. |
| **8** | Synchronous (inline) submission | Add in-request execution + size-based routing (inline vs job). Deferred until here by design. |
| **9** | Lighten & split `teehr` | Publish a dependency-light core (metric kernels + data model, Spark optional); heavy pieces move to the platform. |

## Sequencing

- **Critical path:** 0 → 1 → 2 → 5 → 6 → 7.
- **Hard prerequisites:** Phase 0 and Phase 2 (the backbone — 3 and 5 should not ship until 2 holds).
- **Parallelism:** after Phase 0, the tenancy track (1 → 2 → 3) and the analytics track (4) run
  largely independently and converge at Phase 5; Phase 4 can start as soon as Phase 0 fixes the spec
  schema (it touches `teehr`, not the control plane).
- **Effort shape:** a multi-quarter program. Phase 0 short but gating; 1, 2, 5 are the heavy lifts;
  3, 4, 6 moderate; 7 is migration/cleanup. No calendar dates — sprint sizing is the team's step
  once the open decisions are settled.

## Cross-cutting (run through every phase)

- **Schema migrations, automated** — versioned, applied via CI/CD or a job, never at app startup or
  by hand from a notebook (control-plane DB → Alembic; Iceberg per-namespace migration or the
  Phase 0 decision to not migrate — see ADR-0006).
- **Backward compatibility** — breaking changes accepted, data loss not; legacy retired in Phase 7.

## Phase ↔ ADR map

The ADRs ([`adr/`](adr/)) are the Phase 0 deliverable; each lands in a later phase:

| Phase | ADRs it realizes |
| --- | --- |
| 0 | All ADRs ratified; spikes run |
| 2 | ADR-0004 (write gateway / API access path) |
| 4 | ADR-0002 (metric engine), ADR-0011 + ADR-0012 + ADR-0010 (spec & data model), `spec-schema-v0.md`, `warehouse-data-model.md` |
| 5 | ADR-0003 (Prefect job runner) |
| 7 | ADR-0006 (migrations), ADR-0009 (branching & cutover) |
| 8 | ADR-0002 / ADR-0003 (inline submission + routing) |
| 9 | ADR-0007 (package decomposition), ADR-0002 (light-core kernels) |

Cross-cutting: ADR-0001 (warehouse layout), ADR-0005 (units), ADR-0013 (evaluation-system boundary).
