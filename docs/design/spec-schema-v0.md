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

**Keeping the schema common is deliberate (ADR-0011).** If TEEHR, VeriRace, and any other adopting
system share one spec schema, an evaluation spec becomes **portable across engines** — the same spec
runs on DuckDB (VeriRace) and Iceberg/Spark (TEEHR) — so specs can be exchanged, published, and
reproduced between systems. That interoperability is a reason to adapt names carefully rather than
diverge the schema.

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
  primary: {dataset: public.usgs_streamflow}
  secondary:
    - {dataset: public.nwm30_short_range}
    - {dataset: public.nwm30_medium_range_mem1, filter: "hour(reference_time) = 0"}

pair:
  relation: public.usgs_nwm30_crosswalk      # or: on_attribute: <shared series attribute>

fields:
  - {name: lead_time_bin, fn: lead_time_bins, width: 6h}
  - {name: high_flow, expr: "primary_value > 1000"}

filter: "value_time >= '2010-10-01' AND value_time < '2020-10-01'"   # row filter before aggregate

aggregate:
  - group_by: [primary_series_key, secondary_dataset, lead_time_bin]
    metrics: [count, relative_bias, rmse, nash_sutcliffe_efficiency, kling_gupta_efficiency]
    skill: {reference: public.nwm30_medium_range_mem1, metrics: [rmse]}

bootstrap: {method: block, block: P1M, iterations: 1000, seed: 42, ci_level: 0.90}

output:
  units: {streamflow: ft3/s}      # display units; read-time, metric-aware
  include_pairs: false
```

## Step notes

- **`inputs`** — `primary` is one dataset (the reference / baseline side); `secondary` is one or
  more. Each input is `{dataset: <ref>, filter: <expr>?}`; the dataset's name becomes the value of
  the `secondary_dataset` column. `filter` is applied **before** pairing, per input — narrow by time,
  location, member, or quality here; variable and configuration are already fixed by the dataset
  choice.
- **`pair`** — `relation: core.<crosswalk>` pairs the primary and secondary series by `series_key`
  via a crosswalk, or `on_attribute: <attr>` when both sides share a series attribute (e.g. `zone`).
  The primary input becomes the `primary_*` side, the secondary the `secondary_*` side; the paired
  table carries `primary_series_key`, `secondary_series_key`, `value_time`, `reference_time`,
  `primary_value`, `secondary_value`, `secondary_dataset`, plus `member`/`quantile` when present.
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
characters, with an optional system-project prefix: `public.` (open input data) or `platform.`
(platform-owned evaluations). No prefix = the project's own namespace. One spec may mix them (your
forecast against `public.usgs_streamflow`).
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

- **Expression grammar — direction set:** a **TEEHR-owned** allow-list — a small, checked SQL
  subset for `filter` / `expr` — expressed in an engine-neutral canonical form and transpiled per
  engine (Trino / Spark) via sqlglot. Re-implemented to VeriRace's pattern, not adopted from it
  (ADR-0011); it ships with the shared spec schema. (Open: canonical dialect vs. transpile-from-one.)
- Generated secondary inputs (persistence, climatology, blend) — VeriRace has these; do we want
  them in v0 or later?
- Where ensemble `member` and `reference_time` flow through pairing for forecast verification.
- Whether `output` geometry/format belongs in the spec or is API content negotiation.
