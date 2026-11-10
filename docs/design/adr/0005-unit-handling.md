# ADR-0005: Unit handling — canonical-on-ingest, metric-aware display

- **Status:** Accepted
- **Date:** 2026-11-10
- **Deciders:** TEEHR team
- **Related:** #133, [phased plan](https://claude.ai/artifact/NDYWNmX5biKrBvPuZAdsNE), ADR-0002 (metric kernels declare unit semantics), `../warehouse-data-model.md`

## Context

Units today are carried as free-form strings, which blocks reliable conversion and correct math.
The platform must display different units for both timeseries **and** derived metrics, and a
metric's unit is not a simple scalar conversion of its inputs' units.

## Decision

- **Canonical on ingest.** Store every value in a canonical unit, keeping the **original value and
  unit as provenance**. Math and storage are canonical; display is a read-time render.
- **Unit registry with a small algebra** — dimensions composed by powers and ratios, affine-aware
  (offsets for temperature-like units).
- **Per-metric unit semantics.** A derived metric declares how its unit relates to its inputs:
  dimensionless (KGE), same-as-input (bias), input-squared (variance), or relative/ratio. A metric
  does **not** convert by a blanket value × factor; display conversion is **metric-aware**.
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

## Notes

- The metric kernels (ADR-0002) are where each metric's unit semantics are declared, next to the
  calculation.
- Canonical units and the unit domain table are part of the warehouse data model artifact.
