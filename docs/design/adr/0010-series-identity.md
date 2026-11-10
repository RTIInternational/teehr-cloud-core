# ADR-0010: Series identity — `series_key`, locations as a linked layer

- **Status:** Proposed (adopted as the design direction; ratify at the Phase 0 gate)
- **Date:** 2026-11-10
- **Deciders:** TEEHR team
- **Related:** #133, [phased plan](https://claude.ai/artifact/NDYWNmX5biKrBvPuZAdsNE), ADR-0001 (layout), `../warehouse-data-model.md`, `../spec-schema-v0.md`

## Context

Timeseries today are keyed by `location_id`, which assumes every series is a location. The
project-based platform brings arbitrary, multi-domain data — energy demand, grid nodes, zonal
series — where the series identifier is **not** a location and does not reference the `locations`
table. `location_id` is a misnomer in the general case, and it conflates the timeseries identity
with geography.

[VeriRace](https://github.com/mgdenno/verirace) models this cleanly: the atomic unit is a
**series** (`series_key`); a location is a linked attribute, not the identity — "a gage is just one
kind of series."

## Decision

- The `timeseries` fact identity is **`series_key`** (not `location_id`).
- A **`series` catalog** table holds per-series attributes, including an **optional `location_id`**
  that is an FK into a `locations` layer, plus free-form attributes used for grouping/pairing.
- **`locations`** is a geo layer (`location_id → geometry`), linked **from the series catalog**, not
  referenced by the fact table. It can be shared (in `core`) across datasets.
- **Pairing is series↔series:** a `crosswalk` relates observed `series_key` ↔ predicted
  `series_key`, or `on_attribute` matches a shared series attribute. Never a location join.
- `location_id` survives as the **catalog FK** where a series is geographic — correct where it is
  actually a location, absent where it is not.

## Consequences

- **Positive:** general across domains (hydrology, energy, anything observed-vs-predicted); honest
  naming; geometry is shareable and resolved on demand (`series → location → geometry`). Hydrology
  is unaffected — `series_key` = the gage id and `location_id` lives in the catalog, still available
  to `group_by`.
- **Negative / cost:** a conceptual shift from the location-centric model the team knows; one extra
  catalog join to pull geographic attributes; `location_id` moves out of the fact rows.
- **Tradeoff accepted:** re-learning the identity and an extra join, in exchange for a model that
  does not assume every series is a place.

## Alternatives considered

- **Keep `location_id` as the fact key.** Breaks for non-geographic series and bakes a hydrology
  assumption into the core model. Rejected.
- **Keep `location_id` but document that it "may not be a location."** Same conflation, dishonest
  name. Rejected.

## Notes

- Follows ADR-0001 (table-per-dataset): `series`, `locations`, and `crosswalks` are their own
  typed datasets.
- `interval` is per-series (in the `series` catalog), nullable for instantaneous data, with a
  dataset default — decided here. Open: whether the `series` catalog is per-dataset or
  per-namespace; paired column prefix (`obs_*`/`pred_*` vs `primary_*`/`secondary_*`).
