# ADR-0006: Warehouse migration strategy

- **Status:** Deferred
- **Date:** 2026-11-10
- **Deciders:** TEEHR team
- **Related:** #133, [phased plan](https://claude.ai/artifact/NDYWNmX5biKrBvPuZAdsNE), ADR-0001 (project = namespace)

## Context

Once a project is a Polaris namespace (ADR-0001), a change to the `core` schema may need to
propagate across many project namespaces. Two options:

1. **Cross-namespace Iceberg migrations** — track an applied-migration version per namespace and
   apply changes programmatically across all of them.
2. **No warehouse migrations at all** — evolve `core` with Iceberg schema evolution, and
   **regenerate** derived tables rather than migrate them.

The cost of building a cross-namespace migration framework up front is high, and it is unclear how
often a `core` schema change actually has to reach tenant namespaces.

## Decision

**Deliberately defer** the cross-namespace migration framework. For now:

- Automate the migration path we already have — run through CI/CD or a job, never a human logging
  into Jupyter (per the "Schema migrations, automated" cross-cutting rule).
- Evolve `core` via **Iceberg schema evolution**; **regenerate derived tables** (joined
  timeseries, metrics, summaries) rather than migrate them.

**Reopen trigger:** the first `core` schema change that must land in tenant namespaces, or
evidence that such propagation is frequent. At that point, decide between option 1 and a narrower
mechanism.

## Consequences

- **Positive:** avoids building a speculative framework; leans on Iceberg evolution plus
  regenerate-derived, which the team already does; keeps Phase 0 scope small.
- **Negative / cost:** if cross-namespace propagation turns out to be common, the framework gets
  built later, under more pressure.
- **Tradeoff accepted:** a possible later retrofit, in exchange for not building an unproven
  framework now.

## Alternatives considered

- **Build per-namespace applied-migration tracking now.** Speculative; may never be needed if
  tenant schemas rarely track `core`. Rejected for now.
- **No migration discipline at all.** Rejected — see note; the application/metadata DB still needs
  real migrations regardless.

## Notes

- **Separate from this ADR:** the application/metadata control-plane DB moves off
  create-table-on-startup to **Alembic**, run as a deploy step (init container / Job / CI), not at
  app boot. That is **not** deferred — it is a Phase 7 (and earlier where the control plane lands)
  commitment under the same "Schema migrations, automated" rule.
- Consistent with the hard rule that all cluster state changes go through code, not ad-hoc `kubectl`.
