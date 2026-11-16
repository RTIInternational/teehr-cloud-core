# teehr-core (Phase 4)

- **Status:** draft 2026-11-16, for review before the repo is created.
- **Builds on:** ADR-0002 (metric engine), ADR-0007 (package decomposition), ADR-0008 (repo
  topology), ADR-0011 (VeriRace parity); teehr-shared-eval-spec (`standards/metrics.md`,
  `metric-state.md`, `bootstrap.md`, `units.md`, `spec/`).
- **Repo:** `RTIInternational/teehr-core`, public, MIT; on PyPI as `teehr-core`.

## What it is

The dependency-light core of ADR-0007: the canonical data model, the metric kernels, metric
state, the bootstrap, the spec schema, and the engine that runs a spec. numpy and pyarrow, no
Spark, no JVM, importable offline. It's the engine-neutral library both `teehr` and VeriRace
depend on (ADR-0011), so one definition of each metric gives the same numbers everywhere.

| Depends on it | For |
| --- | --- |
| `teehr` | its metrics (replacing `teehr/metrics`), local evaluations, and a Spark path for the largest jobs |
| VeriRace | its metrics, state and bootstrap (replacing its own copies) |
| the control plane | the data model's dataset types; Phase 5 runs evaluations with it |

## Where each piece comes from

Both codebases were read for this (2026-11-16). In short: VeriRace is the base. Its metrics package is
already the engine-neutral design the shared spec standardized (most of the standards were
written from it), it has the larger metric set, and its spec patterns are cleaner (skill scores,
guards, typed fields, grouping sets). teehr brings a few metrics and options VeriRace lacks, its
Spark path for the largest jobs, and production experience (NWM-scale runs, regression tests);
its metric code can't be imported without Spark today (`metrics/models/base.py` imports pyspark
and `teehr.querying`).

| Piece | Start from | Bring from the other | Change |
| --- | --- | --- | --- |
| **Metric registry** (name, aliases, family, params, needs, finalizer) | VeriRace `metrics/registry.py` (`Metric`, `METRICS`, `ALIASES`, `MetricRequest`) | teehr's attributes per metric (`display_name`, `value_range`, `optimal_value`, `category`) | canonical names and aliases as `metrics.md` (VeriRace has renamed already) |
| **Kernels** | VeriRace: shared components (`stats.py` `GroupStats`, `@component`) and finalizers over flat arrays + group ids + replicate weights | teehr-only metrics: `center_of_timing`, `standard_deviation_of_timing`; KGE weights `sr`/`sa`/`sb`; `add_epsilon`; transforms `cube`, `exp`, `abs` | transforms and `add_epsilon` as `metrics.md` |
| **Metric state** | VeriRace `state.py` (`group_state`, `StateStats`, `compute_from_state`, `mergeable`) and `sketch.py` | | a pure-numpy merge alongside the SQL one (VeriRace merges only in DuckDB SQL), so any engine can merge |
| **Bootstrap** | VeriRace `bootstrap.py`: the procedure `standards/bootstrap.md` pins | teehr's vectorized `(reps, n)` evaluation was tuned on real NWM runs: compare speed on teehr's benchmarks before release | `gumboot` alias of `year_block` |
| **Significance** | VeriRace `significance.py` (Diebold–Mariano, mergeable) | | |
| **Skill scores** | VeriRace: a stage-level `skill` (`reference` dataset, `metrics`, `form: skill \| ratio`) giving `<metric>_skill = 1 − score / reference score` on rows matching every group key but `secondary_dataset`; metrics flagged `skill` (error scores) in the registry, and the spec refuses skill on any other | | replaces teehr's per-metric `reference_configuration` |
| **Guards** | VeriRace stage `guards`: null a metric on too little or too flat data, with a `null_reason` | teehr's bootstrap minimums (sample size, mean, variance) as guard kinds where they aren't already covered | |
| **Fields** (derived columns) | VeriRace's typed fields: `Exceedance`, `Events`, `Change`, `NaiveScale`, `Percentile`, `LeadTimeBins`, `Calendar`, `Conditions`, `Attribute`, `Within` | teehr's derived-field calculations (e.g. month, water year, flow categories) not covered by these | |
| **Grouping and pairing options** | VeriRace: `rollup`, `grouping_sets`, `common_sample`, `time.exact` | | the spec's open pairing questions (`member` / `reference_time` flow, crosswalk sides) are settled with VeriRace before the schema is frozen |
| **Data model** | the control plane's `dataset_types.py` (warehouse data model v1: timeseries, series, layer, crosswalk) | | as pyarrow schemas; the control plane then imports them from here. teehr's pandera schemas are the pre-v1 model and don't move |
| **Units** | VeriRace `units.py` (affine conversion, dimension checks) | the control plane's UCUM unit table (domains) | conversion takes the unit table as data, so each system supplies its own registry |
| **Spec schema** | VeriRace `spec/models.py` (Pydantic, v1) | | the decided shape (`DECISIONS.md`): `version` 1, ISO 8601 durations, bootstrap on stages, one pair of window columns, `<name>` / `public.<name>` references; JSON Schema generated from it |
| **Expressions** | VeriRace `spec/expressions.py` and the control plane's `expressions.py` (both sqlglot) | | one allow-list module; the control plane's `/data` filters import it |
| **Spec engine** | VeriRace `engine/` (DuckDB SQL for inputs, pairing, fields, filter, grouping; numpy for metrics) | | see "The engine" |
| **Spark path** | teehr `metrics/format.py`, `spark_native.py` | | stays in `teehr` (below) |
| **Tests** | VeriRace's golden vectors (`metric_vectors.json`, `hydro_metrics.json`, cross-checked against other libraries) | teehr's metric regression tests (`tests/query/test_metrics_*`), rerun through the core | the shared spec's conformance cases are built here |

## The engine

The steps before metrics (read inputs, pair, derive fields, filter, group) are relational: joins,
windows, grouping sets. VeriRace runs them as DuckDB SQL, then hands each group's arrays to the
numpy kernels. Two ways for the core:

1. **DuckDB, as an optional extra** (`teehr-core[engine]`): port VeriRace's backend. DuckDB is a
   pip wheel (no JVM, no server), runs the expression language natively (sqlglot's canonical
   dialect), reads Parquet, Arrow and Iceberg. The kernels and state stay importable without it.
2. **pyarrow only**: rewrite pairing and grouping on pyarrow's joins and `group_by`. No extra
   dependency, but a second implementation of everything VeriRace's SQL already does, and no
   expression engine (the control plane evaluates expressions on Arrow itself today, for filters
   only).

**Proposed: 1.** It is the only option that doesn't rewrite working, tested code, and it settles
the read-engine question (teehr-cloud-core #197) the same way: if the DuckDB spike shows it can read
Iceberg through Polaris as the caller, the control plane's reads and Phase 5's runs use the same
engine. If the spike fails, the engine still runs on files and Arrow handed to it, and only the
control plane's reads stay on PyIceberg.

**Spark** stays in `teehr`, for jobs too large for one machine (ADR-0002): Spark computes each
partition's metric state with the core's `group_state` (in `applyInPandas`), merges by addition (a
Spark aggregate), and finalizes with the core's `compute_from_state`. Same formulas, same state,
so pairs, Spark and DuckDB agree by construction. `spark_native.py`'s second implementation of the
metrics goes away.

## Layout

```
teehr_core/
  model/        dataset types (pyarrow schemas), units (conversion over a unit table)
  metrics/      registry, components, finalizers, transforms
  state/        group_state, merge (numpy), compute_from_state, sketch
  bootstrap/    draws, intervals (standards/bootstrap.md)
  stats/        significance (Diebold-Mariano)
  spec/         models, JSON Schema, expressions (allow-list, sqlglot)
  engine/       [engine extra] plan + DuckDB backend
  conformance/  runner for the shared spec's cases
```

Python ≥ 3.12. Base dependencies: numpy, pyarrow, pydantic, sqlglot. Extra `engine`: duckdb.

## Steps

1. **Repo and packaging:** scaffold (pyproject, ruff, pytest, CI, PyPI trusted publishing), MIT.
2. **Kernels, state, bootstrap** from VeriRace with the agreed names; add the teehr-only metrics
   and options; a numpy state merge. Golden vectors pass.
3. **Data model and units** from the control plane and VeriRace.
4. **Spec schema and expressions** (the decided v1), JSON Schema.
5. **Conformance cases** (shared spec) and the runner; both systems run them in CI.
6. **Engine** (DuckDB extra), ported from VeriRace.
7. **Adoption:** `teehr` replaces `teehr/metrics` (Python path) with the core and rebuilds its
   Spark path on state (teehr#871); VeriRace replaces its metrics package; the control plane
   imports the dataset types and expressions.

Steps 2–5 are the release that matters to both systems; 6–7 follow.

## Open

1. **Publishing VeriRace's code.** VeriRace is MIT (its `pyproject.toml`) but private
   (`mgdenno/verirace`); most of steps 2, 4 and 6 is its code, made public here. OK to publish?
2. **Who maintains it:** TEEHR (final say, as for the shared spec) with VeriRace as a second
   maintainer?
3. **Formula drift:** before step 2 ships, compare every shared metric between teehr and VeriRace
   on the same inputs (ddof, undefined cases, KGE variants), and record any differences as
   decisions in the shared spec.
4. **teehr's `LICENSE` (MIT) and `pyproject.toml` ("GNU v3") disagree;** fix in teehr.
