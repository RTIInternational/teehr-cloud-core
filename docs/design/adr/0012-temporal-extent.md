# ADR-0012: Temporal extent of a value — fixed periods and a general window model

- **Status:** Proposed (general direction preferred; ratify at the Phase 0 gate)
- **Date:** 2026-11-11
- **Deciders:** TEEHR team
- **Related:** #133, [phased plan](https://claude.ai/artifact/NDYWNmX5biKrBvPuZAdsNE), ADR-0010 (series identity & interval), ADR-0002 (metric engine), `../warehouse-data-model.md`, `../spec-schema-v0.md`

## Context

`interval` (ADR-0010, per-series) describes the time step / accumulation window of a value as a
**duration** — an unanchored length (`PT1H`, `P1D`). That covers instantaneous readings and rolling
accumulations, the bulk of hydrologic data.

It does **not** cover values defined over a **fixed, anchored calendar span**. TEEHR-FVED (water
supply) needs these: a seasonal volume such as the April–July runoff (encoded `0407` today) is a
value for the calendar window Apr 1 – Jul 31 of a given water year. The span is anchored to specific
months, recurs each year, and is not expressible as an ISO-8601 duration (leap years alone defeat
`P122D`). VeriRace's spec did not model anchored periods at all.

The known requirement is small — a handful of seasonal windows (NRCS / CBRFC forecast periods). But
the team would rather solve the general problem — "what span of time does a value cover?" — so the
system is extensible to future period shapes (multi-month, water-year, custom windows) without
another schema change.

A further subtlety: **non-instantaneous values already apply to a period** — a daily mean covers a
day, an hourly accumulation covers an hour — so `interval` (in its coverage sense) and a `period`
overlap. The old `interval` term in fact conflates **two independent things**: how much time a value
*covers*, and how often values are *reported* (spacing). They are genuinely independent — streamflow
*hourly instantaneous* has coverage = an instant but resolution = hourly, whereas precipitation
*hourly accumulation* has coverage = 1h and resolution = 1h coinciding, which is how a single field
got away with conflating them. A general model must separate these dimensions.

## Decision

Model a value's **temporal extent (coverage)** as the single load-bearing temporal concept — a
per-series **`period`** — and separate out the dimensions the old `interval` conflated.

**The independent temporal dimensions:**

- **Issuance** — when a forecast was made: already `reference_time`. (`interval` is *not* issuance.)
- **`value_time`** — the time anchor a value is reported at.
- **Coverage (temporal extent)** — the span a single value represents. **This is `period`.**
- **`statistic`** — how the value reduces over its coverage (instantaneous / mean / sum / max).
- **Resolution (frequency / spacing)** — how often values recur in the series (hourly, 15-minute).
  Largely derivable from `value_time` spacing; descriptive metadata, not load-bearing for pairing or
  metrics.

**`period` is a window `[start, end]` whose endpoints are anchored.** The anchor is what makes the
model general — a coverage window can be defined relative to a reference *or* fixed:

  - **instant** — zero-width at `value_time` (point values).
  - **duration (anchored to `value_time`)** — the day a daily mean covers, the hour an hourly
    accumulation covers.
  - **lead interval (anchored to issuance / `reference_time`)** — a window a lead-time offset from
    when the forecast was issued; the normal forecast-accumulation case.
  - **fixed calendar span** — a month–day start/end (e.g. `--04-01` … `--07-31`), the year taken
    from the occurrence; the FVED `0407` case.
  - **absolute** — explicit `[start, end]` timestamps when none of the above fits.

  So a forecast value's coverage **can be either an interval from issuance time or a fixed period** —
  both are just anchored windows, expressed uniformly instead of as special cases.

- The **resolved** `[start, end]` of each value is materialized (or trivially derivable) so queries,
  grouping, and metrics read a concrete window — never parse a code or re-derive from an anchor.
- **`interval` decomposes.** Its coverage meaning folds into `period` (the `duration` / `instant`
  form); its spacing meaning becomes the optional **`resolution`** attribute. The old conflated
  `interval` field is retired in favor of these two — this refines the interval treatment in
  ADR-0010.
- `period` is **per-series** (a series has one coverage definition), nullable / defaulted, so the
  common case (instant or simple duration) carries nothing extra.
- **Pairing** matches on the resolved coverage: two series pair only when their windows are
  comparable (subsumes the `interval`-match rule for non-`inst` statistics).
- **FVED's `0407` becomes a named period, not a parsed code.** Today `0407` is an opaque string
  meaning "April through July." Under this model the period is defined by its real window — start
  Apr 1, end Jul 31 — and that resolved window is what the system stores and computes on (grouping,
  pairing, metrics). `0407` is kept only as the period's **display name / alias**, so FVED users and
  exports still see the familiar code, but nothing has to decode the digits to know what span it
  means.

## Consequences

- **Positive:** one general concept for "the time a value covers"; the `interval`/`period` overlap
  is eliminated by separating coverage from frequency rather than keeping two fields; extensible to
  new window shapes (lead intervals, multi-month, water-year) without a schema change; preserves
  FVED functionality (and its `0407` label) losslessly; metrics / queries read a resolved window,
  never a parsed code.
- **Negative / cost:** more than a flat code enum — we must define and validate the small anchor set
  and its resolution to `[start, end]`; this **refines ADR-0010's `interval`** (coverage → `period`,
  spacing → `resolution`), so that decision is reframed, not merely extended; `value_time` semantics
  for an anchored span (it fixes the occurrence / water year, not the instant) must be documented;
  the common case still needs a trivial default so instant/duration series stay effortless.
- **Tradeoff accepted:** a slightly richer temporal model now, in exchange for not re-opening the
  schema the next time a non-duration window (a forecast lead window, a seasonal period) appears.

## Alternatives considered

- **Overload `interval` to also hold a range.** Re-creates the compound-field, string-parsing
  problem this redesign removes (the `variable_name` split exists precisely to avoid it). Rejected.
- **A minimal `period` code domain (closed enum of seasonal codes).** Simplest, and matches today's
  `0407`, but not extensible — every new window shape is a new code with hidden semantics, and
  arbitrary absolute windows do not fit. Rejected in favor of the general form model; kept as the
  **fallback** if the general model proves heavier than the need.
- **Explicit `window_start` / `window_end` columns on every fact row, nothing else.** General, but
  taxes every row for an edge case, drops the `0407` ergonomics, and makes recurring seasonal
  windows absolute (losing the "every water year" meaning). Rejected as the *sole* mechanism —
  `absolute` is folded in as one form instead.

## Notes

- Open: whether `resolution` (frequency / spacing) is a **stored** attribute or always derived from
  `value_time` spacing. It is only needed where users filter by sampling density (hourly vs 15-min);
  derive-on-read may be enough. Worked example to preserve: streamflow *hourly instantaneous* =
  coverage `instant`, resolution `hourly` — the two must not collapse back together.
- Open: exact encoding of the anchor endpoints — relative offsets from `value_time` /
  `reference_time`, month–day strings for calendar spans, absolute timestamps — and the water-year
  convention tying `value_time` to the occurrence of a fixed period.
- Open: is `period` genuinely one-per-series, or can a series carry values of mixed coverage? (v0:
  one per series, like `interval` was.)
- Metric semantics (ADR-0002): a period value is typically `statistic = sum` over the window;
  sufficient statistics / sketches still apply per resolved window.
- Decide generality at ratification — the team may still choose the minimal code domain (enumerated
  seasonal codes only) if the general anchored-window model is judged overkill for the known need.
