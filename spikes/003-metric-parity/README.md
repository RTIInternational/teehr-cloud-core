# Spike 3 — Metric engine parity (#145)

Time-boxed spike validating **ADR-0002**: one metric definition (pure-Python kernels)
driven by **two backends** — vectorized Python (in-process pandas) and Spark
(`applyInPandas`) — producing identical results. Spark only *distributes* the shared
kernels; it does not reimplement the science.

## Result: **PASS**

| Check | Result |
| --- | --- |
| Point-metric parity (Python vs Spark) | **exact** (bit-for-bit, `0.0`) |
| Per-chunk sufficient-statistics state parity | **exact** (`0.0`) |
| Seeded block-bootstrap CI parity across backends | **exact** (`0.0`) |
| Stats-reconstructed vs direct metrics | `~1e-13` (within tolerance — fp cancellation) |

Metrics exercised: `count`, `mean_error`, `mae`, `rmse`, `nse`, `kge` (all moment-based).

## Why it matters (findings for ADR-0002)

- The same kernels run in both backends, so parity is **exact**, not merely "within
  tolerance" — the "one definition, two backends" claim holds.
- The **`<eval>__state` path is backend-agnostic**: the bootstrap resamples per-chunk
  **additive sufficient statistics** (counts/sums/squared-sums/cross-products), never raw
  data, and gives bit-identical CIs regardless of which backend produced the state.
- **Numerical caveat:** metrics reconstructed from additive stats differ from direct
  computation by ~`1e-13` (variance via `Σx² − N·mean²` vs `Σ(x−mean)²` — cancellation).
  Cross-backend parity is exact; the stats-vs-direct path needs a **tolerance**, and a
  numerically-stable form (centered sums / Welford) may be worth it at large N.
- **Not covered:** order-statistic metrics (median, percentiles) via mergeable sketches
  (t-digest). Optional per ADR-0002 — order-statistic metrics default to raw resampling.

## Files

- `spike3_metric_parity.py` — point-metric parity across the two backends.
- `spike3_bootstrap_parity.py` — per-chunk sufficient statistics + seeded block bootstrap,
  with three parity checks (state, bootstrap CIs, stats-vs-direct).

## Run

Needs `pyspark` + Java, and `PYSPARK_PYTHON` must point at the same interpreter as the
driver (pandas-UDF workers fail otherwise). Example using the `teehr` venv
(pyspark 4.0.1, Python 3.12, Java 17):

```bash
PYSPARK_PYTHON=/path/to/teehr/.venv/bin/python \
PYSPARK_DRIVER_PYTHON=/path/to/teehr/.venv/bin/python \
/path/to/teehr/.venv/bin/python spike3_bootstrap_parity.py
```

Spike code — kept for reference and as a seed for the real parity tests; not production code.
