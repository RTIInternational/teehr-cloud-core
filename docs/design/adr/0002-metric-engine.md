# ADR-0002: Metric engine — one definition, two backends

- **Status:** Accepted (ratified by the team, 2026-11-13)
- **Date:** 2026-11-10
- **Deciders:** TEEHR team
- **Related:** #133, [phased plan](https://claude.ai/artifact/NDYWNmX5biKrBvPuZAdsNE), ADR-0003 (submission), ADR-0005 (units), ADR-0006 (regenerate not migrate), ADR-0007 (package decomposition), `../warehouse-data-model.md` (`<eval>__state`), spike-3 (parity)

## Context

Metric computation today is coupled to Spark, with several overlapping paths and no
engine-agnostic route callable outside a Spark job. The platform needs one metric definition that
can run both as a distributed job and (later) inline in an API request, with identical results.

Empirically, the vectorized-Python path **outperforms** Spark on complex runs that mix bootstrap
and non-bootstrap metrics (e.g. `nwm_diagnostics` warehouse processing); Spark still wins for very
large jobs that are all-Spark. So the Spark-native path is less central than once assumed, but
still needed at the top of the size range.

## Decision

- **One metric definition** expressed as **pure vectorized-Python kernels** (numpy/pandas, no
  Spark) — the single source of truth for the science.
- **Two interchangeable backends** call those kernels: **vectorized Python in-process** (the
  default) and **Spark** (very large jobs). The backend is selected by data size/weight, not by
  the spec.
- **Spark is an optional extra** (`teehr[spark]`) so the core package stays light and
  offline-importable.
- Kernels **compose from additive sufficient statistics** where the metric allows (counts, sums,
  squared sums, cross-products) so the same per-chunk state serves incremental runs and block
  bootstrapping. This is the **core state path** — the moment-based metrics (mean, bias, RMSE,
  variance, correlation, NSE, KGE). **Order-statistic** metrics (median, percentiles) **default to
  raw resampling** (correct, just less compact) and **may optionally** use a mergeable **DDSketch**
  (log-bucket counts, exactly mergeable; see `../shared-contract-v0.md`) as an efficiency enhancement
  where bootstrap cost over quantiles warrants it — a sketch is not a prerequisite for the state
  design. (DDSketch over t-digest: its merge is exact and order-independent, which the state design
  requires.)
- **This per-chunk state is persisted, not just computed in memory.** The sufficient statistics and
  sketches are written to a durable, per-evaluation artifact — the `<eval>__state` table in the
  warehouse data model — so an incremental run recomputes only new chunks and a block bootstrap
  resamples stored state rather than raw data. A run must therefore **emit this state**, not only
  final metric values. The state is regenerated, not migrated, on a schema change (ADR-0006).
- Each metric **declares its unit semantics** next to its calculation (ADR-0005).
- **Parity tests** assert the two backends produce identical results on the same data.

## Consequences

- **Positive:** one science definition, no fork between inline and distributed; lighter package;
  inline-ready for Phase 8; per-chunk state reused for both incremental recompute and bootstrap.
- **Negative / cost:** two execution backends to keep in parity; kernels must be written against
  the array / sufficient-statistics contract, which is stricter than "take a timeseries slice"; the
  persisted `<eval>__state` is additional data to store and keep in step with the evaluation.
- **Tradeoff accepted:** maintaining parity tests and a stricter kernel contract, in exchange for
  a single definition that runs everywhere.

## Alternatives considered

- **Spark-only (today).** Cannot run inline or offline; heavy dependency; already slower on
  complex bootstrap-heavy runs. Rejected.
- **Python-only.** Cannot scale to the largest jobs. Rejected — Spark stays as the large-job
  backend.

## Notes

- De-risked by spike-3 (#145): the moment-based additive-state path — paired data, several metrics,
  and a **seeded block bootstrap over per-chunk state** — showed **exact parity** across the
  vectorized-Python and Spark backends (the same kernels run in both via `applyInPandas`; Spark only
  distributes). Finding: stats-reconstructed metrics need **tolerance** vs direct computation
  (variance cancellation), so the parity contract uses a tolerance for that path. The
  order-statistic / sketch path is optional and not yet exercised.
- Kernels are extracted in Phase 4; the light-core packaging lands in Phase 9 (ADR-0007).
- The additive-statistics / sketch contract is the "per-chunk metric state" design constraint in
  the plan, persisted as the `<eval>__state` table (warehouse data model) — it must not be blocked by
  run outputs that are only final metric values.
