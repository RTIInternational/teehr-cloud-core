# Analytics spec schema (v0 draft)

- **Status:** Draft v0 — Phase 0 design artifact, input to ratification and spikes
- **Date:** 2026-11-10
- **Related:** #133, [phased plan](https://claude.ai/artifact/NDYWNmX5biKrBvPuZAdsNE), ADR-0002 (metric engine), ADR-0003 (submission), ADR-0005 (units), `warehouse-data-model.md`

The analytics spec is a **serializable, versioned** description of an evaluation. It is **parsed
and validated into a typed object, never string-interpolated into SQL**. One spec drives every
surface (web, SDK, CLI, MCP) and runs on either backend (vectorized Python or Spark) with the same
result. This is a draft to react to.

## Design rules

- **Declarative.** The spec names *what* to compute; the engine chooses *how* (backend, inline vs
  job per ADR-0003).
- **Validated against the data model.** Every `variable` / `interval` / `statistic` / `unit` /
  `configuration` reference is checked against the vocabularies (`warehouse-data-model.md`).
- **Units are explicit.** Any parameter with a dimension carries its unit and is canonicalized on
  submit (ADR-0005).
- **Versioned.** `spec_version` is recorded on every run for reproducibility.

## Top-level structure

```yaml
spec_version: 0
name: usgs-vs-nwm30-daily-kge

sources:                      # datasets from core and/or the project
  observed:
    dataset: core.timeseries
    configuration: usgs_observations
  predicted:
    dataset: core.timeseries
    configuration: nwm30_retrospective

filter:                      # row filters applied before pairing
  variable: streamflow
  interval: daily
  statistic: mean
  value_time: { gte: "2010-10-01", lt: "2020-10-01" }

pairing:                     # how observed and predicted rows are matched
  on: location_id            # resolved through crosswalks when id spaces differ
  crosswalk: core.crosswalks
  time: exact                # exact | nearest | interval-aggregate
  # tolerance: PT1H          # for nearest

joins:                       # optional auxiliary attributes to attach
  - dataset: core.locations
    on: location_id
    attributes: [drainage_area, stream_order]

derived_fields:              # computed columns, declarative expressions (not raw SQL)
  - name: log_value
    expr: "log(value + 1)"

group_by: [location_id, configuration]

metrics:                     # each references the metric kernel registry
  - name: kling_gupta_efficiency
  - name: relative_bias
  - name: nash_sutcliffe_efficiency
  - name: flow_threshold_exceedance
    params:
      threshold: { value: 100.0, unit: m3/s }   # units explicit, canonicalized on submit

bootstrap:                   # optional; omit for point estimates
  method: block              # block bootstrap over per-chunk state (ADR-0002)
  block: P1M                 # monthly blocks, matching per-chunk state granularity
  iterations: 1000
  seed: 42
  ci_level: 0.90

output:
  units:                     # display units; read-time render, metric-aware
    streamflow: ft3/s
  format: table              # table | geojson (when grouped by a geospatial key)
```

## Field notes

- **`sources`** — each role (`observed`, `predicted`, and any others) names a dataset and the
  `configuration` within it. Datasets resolve to `core.<type>` or `<project>.<type>`.
- **`pairing.time`** — `exact`, `nearest` (with tolerance), or `interval-aggregate` (aggregate the
  finer series to the coarser interval before pairing).
- **`derived_fields.expr`** — a small, validated expression grammar, **not** passthrough SQL, so
  it is safe on either backend.
- **`metrics[].name`** — must exist in the kernel registry; each kernel declares its unit
  semantics (ADR-0005), so `output.units` conversion is metric-aware.
- **`bootstrap.block`** should match the per-chunk state granularity so replicates resample stored
  state rather than recomputing from raw (ADR-0002).

## Validation (reject at submit, before any run)

- Unknown `variable` / `interval` / `statistic` / `configuration` / `unit`.
- A `metric` not in the registry, or a param with a missing/incompatible unit.
- A `pairing.crosswalk` that does not connect the two sources' id spaces.
- A `group_by` column not present after joins/derived fields.

## Versioning

`spec_version` starts at 0 (this draft). Breaking changes bump it; the loader supports the current
and previous versions so saved specs keep running across one migration window.

## Open questions

- Expression grammar for `derived_fields` — adopt an existing mini-language or define a small one?
- How ensemble members and reference_time flow through pairing for forecast verification.
- Whether `output.format: geojson` belongs in the spec or is purely an API content-negotiation
  concern.
