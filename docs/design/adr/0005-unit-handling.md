# ADR-0005: Unit handling — canonical-on-ingest, metric-aware display

- **Status:** Accepted
- **Date:** 2026-11-10
- **Deciders:** TEEHR team
- **Related:** #133, [phased plan](https://claude.ai/artifact/NDYWNmX5biKrBvPuZAdsNE), ADR-0002 (metric kernels declare unit semantics), ADR-0011 (interface parity — unit standard), `../warehouse-data-model.md`

## Context

Units today are carried as free-form strings, which blocks reliable conversion and correct math.
The platform must display different units for both timeseries **and** derived metrics, and a
metric's unit is not a simple scalar conversion of its inputs' units.

## Decision

- **Canonical on ingest.** Store every value in a canonical unit, keeping the **original value and
  unit as provenance**. Math and storage are canonical; display is a read-time render.
- **Unit registry with a small algebra** — dimensions composed by powers and ratios, affine-aware
  (offsets for temperature-like units). **Unit identifiers follow UCUM** (the Unified Code for Units
  of Measure: `[cft_i]/s`, `m3/s`, `Cel`, `[degF]`, `[ft_i]`, `MW`, …) — the standard VeriRace uses,
  adopted for spec / interface portability (ADR-0011), so a spec's unit strings mean the same thing
  in both systems. Each registry row carries `to_si` and `offset`: conversion is affine —
  `si = value × to_si + offset`, and between units `x' = a·x + b` with `a = to_si(u)/to_si(v)` and
  `b = (offset(u) − offset(v))/to_si(v)` (`b = 0` for every ratio-scale unit — flow, length, power;
  non-zero only for temperature).
- **Per-metric unit semantics, as a unit-kind taxonomy.** Each metric declares one *kind* that fixes
  how its result converts: `value` (a·x+b), `sum` (a·x+n·b), `difference` (a·x, offsets cancel —
  bias, RMSE), `difference²` (a²·x — variance, MSE), `invariant` (unchanged — KGE, NSE,
  correlations), `ratio` (unchanged when b=0, else recomputed in the target unit — relative bias,
  KGE's β), `time` (unchanged). A metric does **not** convert by a blanket value × factor; display
  conversion is **metric-aware**. This taxonomy is VeriRace's, validated there across its full metric
  set (ADR-0011).
- **Ingest accepts source conventions, maps to UCUM.** Data labeled in CF / UDUNITS (netCDF, NWM,
  gridded) or free-form source strings is mapped to the canonical UCUM unit at the write gateway, the
  original kept as provenance. So CF / netCDF sources stay first-class without changing the canonical
  standard.
- **Spec parameters carry units** (e.g. a flow threshold), canonicalized on submit.
- **Unit-correctness tests** ship alongside the engine parity tests.

## Consequences

- **Positive:** consistent math regardless of source units; clean, correct display conversion for
  both timeseries and metrics; original values retained for audit and reprocessing.
- **Negative / cost:** a unit registry and per-metric unit declarations to build and maintain;
  each new metric must state its unit semantics.
- **Tradeoff accepted:** affine subtlety is real and must be handled — a mean temperature converts
  with the offset, but a temperature **difference** (bias, std dev) drops it. The registry encodes
  this rather than leaving it to callers.

## Alternatives considered

- **Store original, convert on read.** Every read path re-derives canonical values and must know
  each source's units; math becomes unit-dependent and error-prone. Rejected.
- **Blanket value × factor for metric display.** Wrong for dimensionless, squared, and relative
  metrics. Rejected in favor of per-metric declarations.
- **UDUNITS / CF-conventions as the canonical standard** (native to netCDF / NWM / gridded data, and
  familiar to hydrometeorologists). Diverges from VeriRace's unit grammar, which would break the
  portable-spec goal (ADR-0011/0012) — a spec's threshold units would no longer be shared. Rejected
  as the *canonical / spec* standard; CF / UDUNITS is instead accepted at *ingest* and mapped to
  UCUM.

## Notes

- The metric kernels (ADR-0002) are where each metric's unit semantics are declared, next to the
  calculation.
- Canonical units and the unit domain table are part of the warehouse data model artifact.
- **Unit standard reviewed against UDUNITS / CF** (the hydromet-native convention): UCUM chosen for
  spec portability with VeriRace, with CF / UDUNITS handled as an ingest mapping so NWM / netCDF
  sources remain first-class. Confirm at ratification. Implementation note: VeriRace carries its own
  UCUM unit registry (no `pint` / `cf-units` dependency); whether we reuse a small registry or a
  UCUM-aware library is an implementation choice, not a standard choice.
