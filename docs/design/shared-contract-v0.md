# Shared contract v0 — mergeable metric state + expression language

- **Status:** v0 (jointly agreed with the VeriRace team, 2026-11-13; both open items decided — VeriRace adapts)
- **Date:** 2026-11-13
- **contract_version:** 0
- **Related:** ADR-0011 (interface parity; TEEHR owns/publishes the shared package), ADR-0002
  (metric engine), ADR-0012 (periods), `spec-schema-v0.md`, `warehouse-data-model.md`

The engine-independent format that TEEHR and VeriRace (and any future engine) both implement, so a
metric gives the same number and a saved spec runs on either substrate (ADR-0011). Each engine
writes its **own** producer/consumer; this document fixes only the **format**. TEEHR owns and
publishes the reference package; others adopt it. Versioned together, semver; a metric-value change
is a release with the reason recorded.

## A. Mergeable metric state

**Principle (ADR-0002, validated by TEEHR Spike 3).** Any slice of a group's pairs yields a state
row from the same arrays; rows **merge by addition** (with a mean-correction for centered moments);
a metric computes identically from raw pairs, from merged state, or from a bootstrap drawn from
state. The **exact columns are generated from the shared metric registry** — each metric declares
the components it needs — so agreeing the registry plus the encoding below fixes the columns.

**Row identity:** `group_id` (plus any partition / shard key the engine merges away),
`contract_version`, and `n` (valid-pair count). Three column families (VeriRace's naming, adopted):

### 1. `components` — centered moment sufficient statistics

Additive via the Chan parallel-merge; **bit-exact**. Base set for the moment metrics:

| Column | Meaning |
| --- | --- |
| `mean_primary`, `mean_secondary` | running means |
| `M2_primary`, `M2_secondary` | centered sum of squares |
| `C_primary_secondary` | centered co-moment |
| `mean_error`, `M2_error` | error = `secondary − primary` |
| `sum_abs_error` | for MAE (plain additive) |
| `min_primary`, `max_primary`, `min_secondary`, `max_secondary` | extremes |

**Merge** (combine parts A and B; δ = `mean_B − mean_A`, `n = n_A + n_B`):

```
mean = mean_A + δ · n_B / n
M2   = M2_A + M2_B + δ²  · n_A · n_B / n
C    = C_A  + C_B  + δ_primary · δ_secondary · n_A · n_B / n
```

Extremes merge by min/max; `n` and `sum_abs_error` by plain addition. From these, exactly: bias,
MSE/RMSE, MAE, variance/std, covariance, Pearson r, NSE, KGE.

### 2. `+<key>` — additive sums keyed by metric + parameters

Plain addition. E.g. contingency counts for threshold/categorical metrics
(`+hits@<threshold>`, `+misses@<threshold>`, `+false_alarms@<threshold>`,
`+correct_negatives@<threshold>`) and per-case score sums. `int64` for counts, `float64` for score
sums. Merge = sum.

### 3. `~primary` / `~secondary` — quantile sketches (DDSketch)

For order-statistic metrics (quantiles, percentile errors). The sketch is a **DDSketch**
(log-bucket counts with a relative-value-error guarantee) — chosen because it **merges by adding
bucket counts, exactly and order-independently**, which is exactly what the path-independence
invariant needs. (t-digest was rejected: its merge is approximate and order-dependent, which would
break that invariant.) v0 parameters (TEEHR-owned; VeriRace adapts — and already uses DDSketch):

- **Relative accuracy** α = **0.001** (0.1%), so γ = (1+α)/(1−α) ≈ **1.002002**. A positive value
  `x` maps to bucket index `i = ⌈log_γ(x)⌉`; the quantile estimate for a bucket is `2·γ^i/(γ+1)`,
  guaranteeing the reported value within **0.1% relative error** — matching the family-3 golden
  tolerance.
- **Zero / sub-floor:** a dedicated `zero` counter; magnitudes below the configured `min` floor fold
  into it (a relative-error sketch cannot bound error at zero — relevant for near-zero streamflow).
- **Signed quantities** (e.g. error-quantile metrics): a sign-mirrored bucket set indexed on `|x|`
  for the negative side.
- **Bounded size:** a fixed index range from the configured `[min, max]` magnitudes; counts outside
  collapse into the boundary buckets. γ, the range, and the collapsing are **pinned by
  `contract_version`** so both engines produce identical, exactly-mergeable sketches.
- Stored as a fixed-length `int64` array in index order. **Merge = add counts per index.**

### Encoding & invariants

- `float64` for moments, `int64` for counts, each sketch a fixed-length `int64` array (or bin→count
  map). `NaN` where a metric is undefined; null = absent component.
- **Path independence (shared golden tests, both engines run them):** `pairs == merged-state ==
  bootstrap-from-state` is **exact for all three families** — DDSketch buckets merge exactly, so a
  sketch metric reads **identically** on every path. A sketch quantile additionally carries its
  inherent **≤0.1%** error versus the true value; the golden tests assert path-identity exactly and
  sketch-vs-truth within 0.1%.

## B. Expression language (`filter` / `expr`)

Used by the spec's `filter` (row filter) and `fields[].expr` (derived column) — **not raw SQL**.

- **Form:** a restricted SQL-expression subset, validated against an **allow-list**. No subqueries,
  no non-listed functions, no DDL/DML.
- **Canonical dialect:** expressions are stored in the spec in an **engine-neutral canonical form**
  (standard-SQL subset) — **decided** over transpile-from-DuckDB; each engine transpiles with
  **sqlglot** to its dialect (Trino/Spark for TEEHR, DuckDB for VeriRace).
- **Identifiers available** (the paired-table columns): `primary_value`, `secondary_value`,
  `value_time`, `reference_time`, `primary_series_key`, `secondary_series_key`, `secondary_dataset`,
  `member`, `quantile`, `period_start`, `period_end`, plus any `fields` already defined.
- **Allow-list v0:**
  - operators: `= != < <= > >=`, `AND OR NOT`, `+ - * /`, `IN`, `BETWEEN`, `IS [NOT] NULL`, `LIKE`
  - functions: `abs`, `coalesce`, `least`, `greatest`; time: `hour`, `day`, `month`, `year`,
    `date_trunc`; math: `log`, `exp`, `sqrt`, `pow`; conditional: `CASE WHEN`
  - literals: numbers, quoted strings, ISO-8601 timestamps
- Rejected off-list **at submit**, before any run. Versioned with the spec schema; the allow-list
  ships with the shared package.

## Decided (2026-11-13)

Both previously-open items are settled; **VeriRace adapts to this definition** (TEEHR owns the
shared package, ADR-0011):

1. **Sketch** — **DDSketch** (A.3): α = 0.1% (γ ≈ 1.002002), exactly mergeable by bucket-count
   addition; γ/range/collapsing pinned by `contract_version`. Chosen over t-digest for
   order-independent exact merges (VeriRace already uses DDSketch).
2. **Expression dialect** — engine-neutral canonical form, transpiled per engine via sqlglot.
