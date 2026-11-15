# Warehouse data model (v1)

- **Status:** v1, decided 2026-11-15 (#182). Supersedes the v0 draft (2026-11-10).
- **Related:** ADR-0001 (layout), ADR-0005 (units), ADR-0010 (series identity), ADR-0011 (VeriRace
  parity), ADR-0012 (temporal extent), `spec-schema-v0.md`, `shared-contract-v0.md`

The canonical model the platform is built against. Existing data is **ported** into it once
(validated backfill, #165), not carried forward. v1 adopts VeriRace's typed datasets as implemented
wherever no ratified TEEHR ADR decides otherwise (ADR-0011); the differences are listed at the end.

## Principles

- **Typed datasets only.** A dataset is one of a few types with a fixed shape. The untyped `table`
  kind in the API is a test path (platform admins only), not part of the model.
- **Metadata at the dataset, values in the rows.** Role, variable, unit, statistic and period are
  dataset-level (fixed for the dataset); rows hold only what varies per value.
- **Controlled vocabularies.** Units, periods and variables are registries; a dataset's metadata must
  name registered entries.
- **Canonical units.** Values are stored in the variable's canonical unit (ADR-0005).
- **One write path enforces it.** The API and its load jobs validate every write (ADR-0004);
  Iceberg enforces no foreign keys, so nothing else does.

## Where things live

- **Data:** one Iceberg table per dataset, in its project's namespace (ADR-0001). The system
  project `public` holds open input data; `platform` holds platform-owned evaluations.
- **Dataset registry and vocabularies:** the control-plane Postgres (`control_plane` schema), the
  authority; each dataset's metadata is mirrored into its Iceberg table properties.
- **Vocabulary rows:** public rows (managed by platform admins) plus each project's own rows
  (added by its writers). A name is defined once across both, so it means the same everywhere.

## Dataset types

| Type | Holds | Key |
| --- | --- | --- |
| `timeseries` | observed, simulated or forecast values | `series_key`, [`reference_time`], `value_time`, [`member`], [`quantile`] |
| `layer` | locations: geometry + attributes | `location_key` |
| `crosswalk` | links between two datasets' series | `left_key`, `right_key` |

Each `timeseries` dataset has a companion **series table** (its series catalog), not a dataset type
of its own.

## Vocabularies

| Registry | Columns | Notes |
| --- | --- | --- |
| **units** | `symbol` (UCUM), `dimension`, `to_si`, `offset` | affine conversion `si = value × to_si + offset` (ADR-0005) |
| **periods** | `name`, `form`, `length`, `value_time_at`, `start`, `end`, `description` | exactly ADR-0012's table; shared rows `instant`, `hour_starting`, `hour_ending`, `day_starting`, `day_ending` ship fixed in both systems |
| **variables** | `name`, `canonical_unit`, `description` | the canonical unit is what values of this variable are stored in; shared public rows are copied into both systems' migrations (ADR-0005) |

Code enums, not tables: `role` (`observed` \| `simulated` \| `forecast`), `statistic`
(`inst` \| `mean` \| `sum` \| `min` \| `max`), period `form`.

There is **no `configuration` vocabulary**: a dataset's name identifies its source/configuration
(`usgs_observations`, `nwm30_retrospective`), and `role` says what kind it is. There is **no
`quality` vocabulary** in v1 (see below).

## `timeseries`

### Registration (dataset-level)

| Field | Rule |
| --- | --- |
| `name` | `[a-z][a-z0-9_]{0,59}`, unique in the project |
| `type` | `timeseries` |
| `role` | `observed` \| `simulated` \| `forecast` |
| `variable` | a registered variable |
| `unit` | the variable's canonical unit (uploads in another unit of the same dimension are converted) |
| `statistic` | `inst` \| `mean` \| `sum` \| `min` \| `max` |
| `period` | a registered period. Default `instant` for `inst`; required otherwise; `instant` only with `inst` (ADR-0012 rule 4) |
| optional columns | which of `reference_time` (always on for `forecast`), `member`, `quantile`, `quality` are present; `period_start`/`period_end` are on exactly when the period's form is `absolute` |
| extra columns | named and typed at registration |
| `series_table` | default: the dataset's own; may name an existing one shared by a family (e.g. NWM forecast ranges) |
| `location_layer` | optional default layer for the dataset's series |
| `time_partition` | `month` (default) \| `year` \| `none` |
| `description`, `source` | free text |

`role`, `variable`, `unit`, `statistic` and `period` are fixed once registered: appending data that
claims different values is refused.

### Columns

| Column | Type | Presence |
| --- | --- | --- |
| `series_key` | string, not null | always |
| `value_time` | timestamptz (UTC, µs), not null | always |
| `value` | double | always (null allowed: a missing value) |
| `reference_time` | timestamptz, not null | forecasts (issue time) |
| `member` | string, not null | ensembles |
| `quantile` | decimal(5,4), not null | quantile forecasts; 4 decimal places in both systems |
| `quality` | string | optional: the source's own code(s), several joined with commas |
| `period_start`, `period_end` | timestamptz, not null | only for `absolute` periods |
| extra columns | as declared | as declared |

An optional column either doesn't exist in the dataset or is never null. That is what lets it be
part of the key: Iceberg identifier fields must be required and can't be floating point (hence
`quantile` is a decimal, not a double).

### Key, writes, layout

- **Key:** `series_key`, [`reference_time`], `value_time`, [`member`], [`quantile`] (the columns
  present). A batch with duplicate keys is refused; writes **upsert** on the key, so a revised
  record (e.g. provisional → approved) replaces the old values.
- **Validation on write:** required columns present and not null; types convert losslessly (times
  need a zone or an explicit `timezone`); values converted to the canonical unit; every new
  `series_key` added to the series table; every problem in a file reported at once.
- **Partitioning:** by `time_partition` on `reference_time` for forecasts, else `value_time`;
  sorted by `series_key`, [`reference_time`], `value_time`. Tuned later by the storage spike;
  doesn't block registration.

### Series table

One row per `series_key`:

| Column | Notes |
| --- | --- |
| `series_key` | unique, not null |
| `location_key`, `location_layer` | where the series is; the layer defaults to the dataset's (ADR-0010) |
| `period` | overrides the dataset's period for this series (same statistic rule) |
| `resolution` | optional, descriptive: how often values recur (ADR-0012) |
| free attributes | for grouping and pairing (basin, zone, operator, …) |

## `layer`

Locations, keyed by `location_key`, with EPSG:4326 `geometry` (points, lines or polygons), an
optional `name`, and arbitrary attribute columns inline. Large polygon layers are served to maps as
vector tiles (PMTiles), not GeoJSON. A series points into a layer through `(location_layer,
location_key)`; an evaluation can also group series by the polygon they fall within (`within`).

## `crosswalk`

`left_key` → `right_key` (series keys of two datasets), plus optional free columns (e.g. `weight`
for many-to-one aggregation). Role-neutral: a spec's inputs decide which side is primary.

## Units and provenance

Values are stored in the variable's canonical unit (ADR-0005). The unit a write arrived in is
recorded **once per write** in the write lineage (control-plane DB), not per row; the original values
follow by reversing the affine conversion. No `original_value` / `original_unit` row columns.

## Evaluation outputs

An evaluation version gets its own tables in its project namespace:
`{pairs|results|groups|state}_{slug}_v{version}`. `results` always; `pairs` optional; `groups` and
`state` when the spec supports incremental runs (ADR-0002). A new version gets a fresh set; they're
regenerated, never migrated (ADR-0006). The evaluation, its spec versions and its runs live in the
control-plane DB.

## Lineage

Every write: who, when, mode, rows, original unit, and the Iceberg snapshot it produced. Every
derived row/run: `run_id`, `spec_version`, `engine_version`, and each input's snapshot id.

## Differences from VeriRace (ADR-0011)

| | TEEHR | VeriRace |
| --- | --- | --- |
| Stored unit | canonical, converted on write | as uploaded, converted at pairing |
| Variables | registry with canonical unit | same registry; canonical unit used for validation only |
| Location layer | per series, dataset default | per dataset |
| `quantile` type | decimal(5,4) | double (same values at 4 decimal places) |

Specs mean the same on both because every unit-bearing spec parameter carries its unit
(`{value, unit}`, ADR-0005).

## Deferred

- Partition/sort tuning (storage spike, real query shapes).
- A quality vocabulary (per-source codes, multiple qualifiers per value).
- Multiple `layer` geometries per location, and gridded data.
