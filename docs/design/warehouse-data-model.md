# Target warehouse data model (v0 draft)

- **Status:** Draft v0 — Phase 0 design artifact, input to ratification and spikes
- **Date:** 2026-11-10
- **Related:** #133, [phased plan](https://claude.ai/artifact/NDYWNmX5biKrBvPuZAdsNE), ADR-0001 (layout), ADR-0005 (units), ADR-0002 (metric engine), ADR-0010 (series identity), ADR-0012 (temporal extent)

This is the greenfield, canonical schema the platform is built against. Existing data is
**ported** into it once (validated backfill), not carried forward. It is a draft to react to, not
a settled schema.

## Principles

- **Greenfield, canonical.** Redesign clean; do not inherit the compound `variable_name`, string
  units, or denormalized domains.
- **Star, not full 3NF.** Normalize the *domains* into small validated domain tables, but
  keep the *fact* tables wide for scan performance.
- **Table-per-dataset within a project namespace** (ADR-0001). A dataset has a type and lives in
  its own Iceberg table; a project is a Polaris namespace.
- **Write gateway enforces the schema.** No writer bypasses it (ADR-0004).
- **Canonical units + provenance** (ADR-0005). Store canonical; keep the original value and unit.

## Namespaces and datasets

- `core` namespace holds the public TEEHR data and the **shared domain tables**.
- Each project is its own namespace; its datasets may reference `core` domains and data.
- A **dataset** is one typed Iceberg table. Dataset types:

| Type | Holds | Notes |
| --- | --- | --- |
| `timeseries` | observed / simulated / forecast values | the fact table; keyed by `series_key` |
| `series` | per-series attributes (the series catalog) | `series_key` → attributes, incl. an optional `(location_layer, location_id)`; one per `timeseries` dataset |
| `locations` | geo layer: points, lines or polygons + inline attributes | `location_id` → `geometry`; shareable, linked from the series catalog |
| `crosswalks` | series ↔ series links | observed `series_key` ↔ predicted `series_key`, for pairing |

**Series are the identity.** A `timeseries` row is keyed by `series_key`, not a location — a gage,
an energy zone, or a grid node are all just series (ADR-0010). A series's attributes, including an
optional `location_id`, live in the `series` catalog; geometry lives in a `locations` layer the
catalog points to. So `location_id` is a catalog attribute where a series is geographic, not an
assumption baked into the fact key.

**Datasets are homogeneous.** A `timeseries` dataset holds exactly one `configuration`, one
`variable`, and one `statistic`; **any variation requires a separate table**. These — and the
canonical `unit` that follows from variable + statistic — are dataset-level metadata (in the dataset
registry and the Iceberg table properties, each an FK to its domain table), not stored per row.
**`interval` is the exception: it is per-series** — set in the `series` catalog, **nullable** (null
for instantaneous data), with an optional dataset-level default — because one network can mix
reporting intervals (hourly, 15-minute, daily) under the same variable and statistic. This keeps
the fact rows minimal while letting a dataset hold mixed-interval series.

Derived outputs are **owned by an evaluation** and scoped by its id within the namespace — see
"Derived tables — owned by an evaluation" below. They are regenerated, not migrated (ADR-0006).

## Domain tables (controlled, in `core`)

Small validated domain tables; every fact-table code column is a foreign key into one of these.

**"Foreign key" here is logical, not enforced by storage.** Iceberg does not support foreign-key
(or primary-key / uniqueness) constraints, and neither Spark nor Trino enforce them on Iceberg
tables. Every FK in this document — domain references, `(location_layer, location_id)`, crosswalk
series links — is validated and enforced by the **write gateway** at write time (ADR-0004), not by
the catalog or query engine. A reader must not assume the engine rejects a dangling reference.

- **`variable`** — the measured quantity only (e.g. `streamflow`, `precipitation`,
  `air_temperature`). No interval or statistic baked in.
- **`interval`** — the time step / accumulation window as a *duration* (e.g. `instantaneous`,
  `hourly`, `daily`, `PT1H`, `P1D`). A duration is an unanchored length; it is **not** a fixed
  calendar span like April–July — see the fixed-period open question below.
- **`statistic`** — how the value is reduced over the interval (e.g. `instantaneous`, `mean`,
  `sum`, `max`, `min`).
- **`unit`** — canonical unit plus its dimension signature (for the registry, ADR-0005).
- **`configuration`** — the data source / model configuration (e.g. `usgs_observations`,
  `nwm30_retrospective`, `nwm30_short_range`), with its kind (observed / simulated / forecast).
- **`quality`** — per-observation quality / qualifier codes (e.g. `approved`, `provisional`,
  `estimated`, `ice`), mapping the source's native code where it has one. Referenced by the
  `timeseries` fact `quality` column, not by the `series` catalog.

### The compound-name split

The legacy `variable_name` (e.g. `streamflow_hourly_inst`) is split into **three descriptors**:
`variable` + `interval` + `statistic`, each an FK to its domain. `variable` and `statistic` are
dataset-level; `interval` is per-series (nullable). This is the single most load-bearing schema
change — it makes grouping, unit semantics, and cross-dataset joins well-defined instead of
string-parsed.

## Fact table — `timeseries`

One row per value. A dataset table is **homogeneous** in `configuration`, `variable`, `interval`,
and `statistic` (the rule above), so those — and the canonical `unit` that follows from them — are
**dataset-level metadata**, not row columns. Row columns (v0):

| Column | Type | Notes |
| --- | --- | --- |
| `series_key` | string | the series identity; its attributes (incl. an optional `location_id`) live in the `series` catalog |
| `value_time` | timestamp | the time the value is for |
| `reference_time` | timestamp, nullable | forecast issue time; null for observed/retrospective |
| `member` | string, nullable | ensemble member; null if deterministic |
| `value` | double | in the dataset's canonical unit |
| `quality` | string, nullable | per-observation quality / qualifier code (FK to the `quality` domain); e.g. USGS approved / provisional / estimated. Null where the source carries no quality (typically simulated / forecast) |
| `original_value` | double, nullable | provenance |
| `original_unit` | string, nullable | provenance (FK to `unit`); dataset-level if the source is uniform |

`quality` is **per-observation**, not per-series — it varies row by row (a gage reading can be
provisional today, approved later), so it is a fact column, not a `series` catalog attribute. It is
nullable because simulated / forecast values usually have none.

The engine materializes `configuration` / `variable` / `statistic` / `unit` (dataset-level) and
`interval` (per-series, from the `series` catalog) as columns when it composes or pairs datasets
(VeriRace's `pred_dataset` pattern), so they are available for `group_by` and output without being
stored per row.

Partitioning / sort: by time (and `series_key` where it helps) — to be confirmed by the storage
spike against real query shapes.

## Series catalog — `series`

One row per `series_key` in a `timeseries` dataset, holding its attributes:

- `series_key` (string, stable) — the series identity, matching the `timeseries` fact.
- `location_id` (string, nullable) — the id within a `locations` layer; null for non-geographic
  series.
- `location_layer` (string, nullable) — names the `locations` dataset that `location_id` resolves
  into, so the FK is the pair `(location_layer, location_id)`. This is **explicit**, not inferred
  from an id prefix, so a series resolves unambiguously when a namespace holds several location
  layers. Null whenever `location_id` is null.
- `interval` (string, nullable) — FK into the `interval` domain; the series' reporting interval,
  null for instantaneous data, falling back to the dataset's default interval when set.
- any free-form attributes used for grouping or pairing (e.g. `zone`, basin id, vendor).

Constant per series. A geographic attribute resolves `series_key → series → location_id →
locations` (the spec's `of: location`); a spatial label resolves the series's geometry against a
polygon layer (`within`).

## Locations layer — `locations`

Geo layer, keyed by `location_id`:

- `location_id` (string, stable), `name`, `geometry` (`GEOMETRY`), plus **arbitrary attribute
  columns** inline (drainage area, stream order, …) rather than a separate attributes table
  (ADR-0001). Id conventions carried forward where they encode meaning (e.g. `usgs-` points vs
  `usgsbasin-` polygons).
- Linked from the `series` catalog (`series.location_id`), not from `timeseries` directly. Can be
  shared (in `core`) and referenced by many datasets' series.
- **Many layers per namespace.** `locations` is a dataset *type*, so a project can hold several —
  gage points, basin polygons, reach lines, grid nodes — each its own table, each shareable. A
  `series` catalog resolves into one of them by its explicit `location_layer` reference (below).

## Crosswalks — `crosswalks`

- Observed `series_key` ↔ predicted `series_key` (+ optional `weight` for many-to-one aggregation).
  Used to pair observed and predicted series for an evaluation — a series↔series link, not a
  location link.
- **Many crosswalks per namespace.** One `crosswalk` table per observed↔predicted pairing you need
  (e.g. `usgs_nwm30`, `usgs_rfc`); a spec's `pair.relation` names which one to use.

## Derived tables — owned by an evaluation

Derived outputs are **not** global shared tables. Each **evaluation** (a saved, versioned spec and
its runs) owns its own set of derived tables, scoped by the evaluation's id/slug within the project
namespace. This extends the table-per-dataset principle (ADR-0001) to derived data: an evaluation's
tables get their own snapshots and lifecycle, and dropping the evaluation drops its tables.

Illustrative set for an evaluation `<eval>` (names and suffixes indicative, not final):

| Table | Holds |
| --- | --- |
| `<eval>__paired` | observed paired with predicted (via crosswalk), time-aligned, plus derived fields |
| `<eval>__groups` | the resolved grouping — group keys and membership |
| `<eval>__state` | per-chunk sufficient statistics / sketches that feed the metrics |
| `<eval>__metrics` | metric results + bootstrap CIs, keyed by the group-by columns |

- **`<eval>__state`** carries the per-`(group, month)` additive statistics (counts, sums, squared
  sums, cross-products) for moment-based metrics and mergeable sketches (histogram / t-digest) for
  order-statistic metrics, so later runs of the same evaluation recompute only new units and
  bootstrap resamples stored state (ADR-0002). Run outputs must **not** be only final metric
  values, or this is blocked.
- **`<eval>__metrics`** columns: the group-by key columns + `metric` + `value` + `unit`
  (metric-aware, ADR-0005) + `ci_lower`, `ci_upper`, `ci_level`, `seed`. The group-by columns are
  its uniqueness key.
- **Regenerated, not migrated** (ADR-0006): a run (re)builds these from the source datasets; a
  schema change regenerates them rather than migrating in place.
- **Control-plane linkage:** the evaluation's identity, spec versions, and run history live in the
  metadata control-plane DB (not the warehouse); that record points to this set of warehouse
  tables. The warehouse holds the data; the control plane holds the evaluation and its lineage.

## Lineage (every derived row / run)

`run_id`, `spec_version`, `engine_version`, and the **input table snapshot** (Iceberg snapshot id)
of each source — so any result is reproducible.

## Open questions

- Partitioning/sort keys for `timeseries` — defer to the storage spike on real query shapes.
- Do ensemble members live as a column (above) or as a separate dataset type? (v0: column.)
- Where polygon geometries that are too large for GeoJSON transport are summarized for the API.
- Exact `configuration` taxonomy and whether `kind` (observed/simulated/forecast) is its own
  domain column.
- Evaluation table scoping — three candidates: eval-first `<eval>__paired`, type-first
  `paired__<eval>` (VeriRace-style, groups like tables together when listing), or a per-evaluation
  sub-namespace. Plus whether a new `version` gets a fresh table set or regenerates in place.
- Which derived tables are mandatory vs optional per evaluation (not every spec needs `__groups`
  or `__state`).
- Provenance granularity: is `original_unit` (and the source) always uniform per dataset — making
  it dataset-level too — or can one dataset mix source units, keeping them row columns?
- `interval` is per-series in the `series` catalog (nullable, with a dataset default), and pairing
  requires matching `interval` per pair for non-`inst` statistics (as VeriRace does) — decided,
  pending ratification (ADR-0010).
- Is the `series` catalog one table per `timeseries` dataset, or a shared catalog per namespace?
- Quality codes are source-specific (USGS approval + qualifiers differ from RFC / NWPS). One shared
  `quality` domain with namespaced codes, or a per-`configuration` quality vocabulary? And can one
  observation carry **multiple** qualifiers at once — v0 models a single `quality` code per row.
- Multiple `locations` layers in a namespace: a `series` row names its layer with an explicit
  `location_layer` column — the FK is `(location_layer, location_id)`, not an id-prefix convention —
  decided, pending ratification.
- **Fixed calendar periods (water-supply seasonal values).** FVED needs values defined over an
  *anchored* calendar span — e.g. an April–July volume, encoded `0407` — which is not an ISO-8601
  duration and does not belong in `interval` (a floating length). VeriRace did not model this.
  Options: (a) overload `interval` to also hold a range — rejected, it re-creates the compound-field
  parsing this redesign removes; (b) a distinct, nullable **`period`** concept, per-series like
  `interval`, FK to a `period` domain that resolves a code (`0407`) to an explicit start/end;
  (c) explicit `window_start` / `window_end` fact columns — most general, but adds columns to every
  row for an edge case. **Proposed: (b)** — `variable`=volume, `statistic`=sum, `period`=`0407`
  decomposes with no overloading (the period is the explicit Apr 1 – Jul 31 window, not the legacy
  `0407` code), `value_time` distinguishes the water year, `interval` stays duration-only, and
  pairing then also matches on `period`. Decide generality at ratification
  (one-off FVED need vs a core concept) — the field is null for every non-period series, so it costs
  the common case nothing. **Written up as ADR-0012**, which goes further: it treats coverage as an
  *anchored window* (anchored to `value_time`, to issuance/`reference_time`, to the calendar, or
  absolute), so a forecast window "an interval from issuance" and a "fixed period" are one model, and
  it decomposes the old `interval` into coverage (`period`) + frequency (`resolution`). See ADR-0012
  for the full decision and open questions.
