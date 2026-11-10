# Analytics spec schema (v0 draft)

- **Status:** Draft v0 — Phase 0 design artifact, input to ratification and spikes
- **Date:** 2026-11-10
- **Related:** #133, [phased plan](https://claude.ai/artifact/NDYWNmX5biKrBvPuZAdsNE), ADR-0002 (metric engine), ADR-0003 (submission), ADR-0005 (units), ADR-0011 (interface parity), `warehouse-data-model.md`

The analytics spec is a **serializable, versioned** description of an evaluation. It is **parsed
and validated into a typed object, never string-interpolated into SQL**. One spec drives every
surface (web, SDK, CLI, MCP) and runs on either backend (vectorized Python or Spark) with the same
result.

The structure follows the [VeriRace](https://github.com/mgdenno/verirace) evaluation spec (v1),
adapted to TEEHR naming. It will ship with a machine-readable JSON Schema (as VeriRace does with
`evaluation-spec-v1.schema.json`) to validate specs before running. Expect to iterate — names here
are v0.

## Design rules

- **Declarative.** The spec names *what* to compute; the engine chooses *how* (backend, inline vs
  job per ADR-0003).
- **A source is just a dataset.** Each dataset is its own table, homogeneous in
  configuration/variable/statistic (`interval` is per-series; `warehouse-data-model.md`), so an
  input names the dataset directly — choosing the dataset *is* choosing configuration, variable and
  statistic. A **`filter`** narrows within it by time, location, member, or quality — never by
  variable or configuration.
- **Validated against the domain tables.** Every referenced dataset, column, metric, and unit is
  checked against the warehouse domains (`warehouse-data-model.md`).
- **Units are explicit** and canonicalized on submit (ADR-0005).
- **Versioned.** `version` is recorded on every run for reproducibility.

## Steps (always run in this order)

`inputs → pair → fields → filter → aggregate → output`

## Example

```yaml
version: 0
name: nwm30-vs-usgs-by-lead-time

inputs:
  observed: {dataset: core.usgs_streamflow}
  predicted:
    - {dataset: core.nwm30_short_range}
    - {dataset: core.nwm30_medium_range_mem1, filter: "hour(reference_time) = 0"}

pair:
  relation: core.usgs_nwm30_crosswalk      # or: on_attribute: <shared series attribute>

fields:
  - {name: lead_time_bin, fn: lead_time_bins, width: 6h}
  - {name: high_flow, expr: "obs_value > 1000"}

filter: "value_time >= '2010-10-01' AND value_time < '2020-10-01'"   # row filter before aggregate

aggregate:
  - group_by: [obs_series_key, pred_dataset, lead_time_bin]
    metrics: [count, relative_bias, rmse, nash_sutcliffe_efficiency, kling_gupta_efficiency]
    skill: {reference: core.nwm30_medium_range_mem1, metrics: [rmse]}

bootstrap: {method: block, block: P1M, iterations: 1000, seed: 42, ci_level: 0.90}

output:
  units: {streamflow: ft3/s}      # display units; read-time, metric-aware
  include_pairs: false
```

## Step notes

- **`inputs`** — `observed` is one dataset; `predicted` is one or more. Each input is
  `{dataset: <ref>, filter: <expr>?}`; the dataset's name becomes the value of the `pred_dataset`
  column. `filter` is applied **before** pairing, per input — narrow by time, location, member, or
  quality here; variable and configuration are already fixed by the dataset choice.
- **`pair`** — `relation: core.<crosswalk>` pairs observed and predicted series by `series_key` via
  a crosswalk, or `on_attribute: <attr>` when both sides share a series attribute (e.g. `zone`).
  Observed becomes the `obs_*` side, predicted the `pred_*` side; the paired table carries
  `obs_series_key`, `pred_series_key`, `value_time`, `reference_time`, `obs_value`, `pred_value`,
  `pred_dataset`, plus `member`/`quantile` when present.
- **`fields`** — derived columns, either `expr: <validated expression>` (not raw SQL) or a named
  `fn:` (e.g. `lead_time_bins`, `within` a geospatial layer, `attribute` of a location).
- **`filter`** — one row filter applied after fields, before aggregation.
- **`aggregate`** — one or more blocks, each `group_by` columns + `metrics`. A metric is a name
  (`rmse`) or an object (`{name: nash_sutcliffe_efficiency, transform: log}`). `skill` computes a
  skill score against a `reference` predicted dataset. Metric names resolve to the kernel registry;
  each kernel declares its unit semantics (ADR-0005).
- **`bootstrap`** — optional; `method: block` resamples the per-`<eval>__state` chunks (ADR-0002)
  rather than recomputing from raw. `block` should match the state granularity.
- **`output`** — `units` (display render), `include_pairs`, and for a geospatial group key
  `geometry` / `attributes` to attach polygons/names for the API.

## Dataset references

A reference is a name: lowercase letters, digits and `_`, starting with a letter, up to 60
characters, with an optional `core.` prefix. No prefix = the project's own namespace; `core.` =
the public `core` project. One spec may mix both (your forecast against `core.usgs_streamflow`).
This covers `dataset`, `relation`, and any `reference`.

## Validation (reject at submit, before any run)

- An unknown dataset, crosswalk, column, metric, or unit.
- A `filter`/`expr` that references a column not present at that step.
- A `pair.relation` that does not connect the two inputs' id spaces.
- A `group_by` column not present after inputs/pair/fields.
- A metric parameter with a missing or incompatible unit.

## Versioning

`version` starts at 0 (this draft). Breaking changes bump it; the loader supports the current and
previous versions so saved specs keep running across one migration window.

## Open questions

- Paired column prefix: VeriRace's `obs_*` / `pred_*` (used here) vs TEEHR's
  `primary_*` / `secondary_*` — confirm at ratification.
- Expression grammar for `expr` / `fn` — adopt VeriRace's or define our own small set.
- Generated predicted inputs (persistence, climatology, blend) — VeriRace has these; do we want
  them in v0 or later?
- Where ensemble `member` and `reference_time` flow through pairing for forecast verification.
- Whether `output` geometry/format belongs in the spec or is API content negotiation.
