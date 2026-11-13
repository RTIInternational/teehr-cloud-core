# ADR-0012: Temporal extent of a value — fixed periods and a general window model

- **Status:** Accepted (ratified by the team, 2026-11-13; the concrete encoding is a joint TEEHR / VeriRace design)
- **Date:** 2026-11-11
- **Deciders:** TEEHR team (encoding developed jointly with the VeriRace team, 2026-11-11)
- **Related:** #133, [phased plan](https://claude.ai/artifact/NDYWNmX5biKrBvPuZAdsNE), ADR-0010 (series identity & interval), ADR-0002 (metric engine), ADR-0011 (interface parity — shared encoding), `../warehouse-data-model.md`, `../spec-schema-v0.md`

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

**`period` describes the coverage window and resolves to a concrete `[start, end]`.** Coverage is
always anchored to the value's `value_time`; issuance is a separate dimension (`reference_time`), and
a forecast's lead time is simply `value_time − reference_time` (derived, not a coverage form). There
are four **forms**:

  - **instant** — zero-width at `value_time` (point values).
  - **duration** — a length anchored to `value_time` (a daily mean, an hourly accumulation).
  - **calendar** — a fixed month–day span (e.g. `--04-01` … `--07-31`); the year is taken from the
    occurrence of the span that contains `value_time`. The FVED April–July case.
  - **absolute** — explicit `[start, end]` timestamps when none of the above fits.

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
- **The legacy `0407` code is not preserved.** FVED's April–July volume becomes a period defined by
  its real window — start Apr 1, end Jul 31 — and that resolved window is what the system stores and
  computes on (grouping, pairing, metrics). The opaque `0407` encoding is dropped on port, consistent
  with the greenfield principle of not inheriting compound string codes; the team adopts the explicit
  period. A period may carry a plain human-readable name (e.g. `apr_jul_volume`) for reference in
  specs, but that is a name, not a code to be parsed.

### Concrete encoding (joint TEEHR / VeriRace, 2026-11-11)

The VeriRace team is adopting this model; the following encoding is agreed jointly so a spec stores
and pairs a value's time identically in both systems (the portable-spec goal, ADR-0011).

**Periods are rows in a small definitions table, referenced by name (never parsed):**

| Column | Type | Used by form | Meaning |
| --- | --- | --- | --- |
| `name` | text | all | unique key, e.g. `instant`, `hour_starting`, `apr_jul_volume` |
| `form` | text | all | `instant` / `duration` / `calendar` / `absolute` |
| `length` | ISO-8601 duration | duration | how long each value covers |
| `value_time_at` | `start` / `end` | duration | whether `value_time` is the window's start or end |
| `start`, `end` | `--MM-DD` | calendar | first and last day of the span, both inclusive |
| `description` | text | all | free text for people |

**Each form resolves to a window:**

- `instant` → `[value_time, value_time]`
- `duration` → `[value_time, value_time + length)` when `value_time_at = start`, or
  `[value_time − length, value_time)` when `end`
- `calendar` → the occurrence of `[start 00:00, day-after-end 00:00)` that contains `value_time`
- `absolute` → the value's own `period_start` / `period_end`

**Rules:**

1. **One period per series** — a series never mixes coverages; another coverage is another series.
2. **Calendar occurrence** — a calendar value belongs to the occurrence of its span that contains
   `value_time`. This fixes the year **without a separate water-year convention**, and spans crossing
   Jan 1 (e.g. `--10-01` … `--09-30`) work the same way. At ingest, set `value_time` to any instant in
   the window (the start is the natural choice).
3. **Inclusive month–day ends, half-open windows** — `--04-01` … `--07-31` covers Apr 1 00:00 up to
   Aug 1 00:00; resolved windows are `[start, end)`.
4. **Default + validation** — `instant` is the default for `inst` values; any other `statistic` must
   name a period. `instant` with a non-`inst` statistic, or a non-`instant` period with `inst`, is an
   error.
5. **Row checks** — a period row fills exactly the columns its form uses; durations must parse,
   month–days must exist.
6. **Only `absolute` adds per-row columns** (`period_start`, `period_end`); every other form derives
   its window on read.
7. **Resolved windows are exposed by name** — wherever values are read or paired, the derived window
   appears as `period_start` / `period_end`, so filters, grouping, and metrics use a concrete window.

**Shared common rows** ship fixed in both systems so the names mean the same everywhere: `instant`,
`hour_starting`, `hour_ending`, `day_starting`, `day_ending`. Users add their own beyond these (e.g.
seasonal volumes).

**Pairing** — two values pair when their series are linked, their `statistic`s are equal, and their
**resolved windows are equal**. For `instant` this is the familiar "same `value_time`" rule; for
durations it also stops a window-start mean pairing with a window-end mean. *Optimization:* when both
series use the same period, equal `value_time`s already imply equal windows, so the join stays on the
stored `value_time` column (keeping Iceberg partition / file pruning); only cross-period pairs join on
the derived windows. Comparing different-length windows (a daily mean vs hourly values) needs an
explicit aggregation first — pairing never does it implicitly. An observation is **complete at
`period_end`** (e.g. for building a persistence reference).

## Consequences

- **Positive:** one general concept for "the time a value covers"; the `interval`/`period` overlap
  is eliminated by separating coverage from frequency rather than keeping two fields; extensible to
  new window shapes (multi-month, water-year, custom seasonal) without a schema change; preserves
  FVED functionality (the April–July volume) as an explicit `calendar` period; metrics / queries
  read a resolved window, never a parsed code; and — because the encoding is **shared with
  VeriRace** — a spec's temporal fields are portable across both systems (ADR-0011).
- **Negative / cost:** more than a flat code enum — we must define and validate the period forms
  and their resolution to `[start, end]`; this **refines ADR-0010's `interval`** (coverage →
  `period`, spacing → `resolution`), so that decision is reframed, not merely extended; the common
  case still needs a trivial default (`instant`) so instant/duration series stay effortless; and the
  encoding is now a **shared contract with VeriRace** — the common-row set and column names must be
  versioned and kept in sync across both systems.
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

- **Resolved by the concrete encoding:** anchor endpoints (`--MM-DD`, ISO-8601 `length`,
  `value_time_at`) and the water-year question (calendar occurrence contains `value_time` — no
  separate convention); and `period` is **one-per-series** (Rule 1). These were open in the first
  draft.
- **VeriRace's two questions — TEEHR's answer:** (a) **accept** the column names and `--MM-DD`;
  (b) **yes** to a fixed shared common-row set (`instant`, `hour_starting`, `hour_ending`,
  `day_starting`, `day_ending`), versioned as part of the shared schema.
- **Resolved (with VeriRace):** portability of **user-defined** periods. Periods belong to a
  dataset's series and specs reference datasets, so a user-defined period **travels with the
  dataset** — an exported / shared dataset carries the definitions of any non-shared periods its
  series use. If a spec itself names a period (e.g. aggregating to `apr_jul_volume` windows before
  pairing), it carries the definition **inline in a `periods:` block** (same columns as the
  definitions table). A name that collides with a differing definition on the receiving side is
  **rejected, never silently overridden**. (Shared common rows are fixed in both systems; user-defined
  period definitions are dataset / project-scoped metadata.)
- Open: whether `resolution` (frequency / spacing) is a **stored** attribute or always derived from
  `value_time` spacing. Only needed where users filter by sampling density (hourly vs 15-min);
  derive-on-read may be enough. Worked example to preserve: streamflow *hourly instantaneous* =
  coverage `instant`, resolution `hourly` — the two must not collapse back together.
- Metric semantics (ADR-0002): a period value is typically `statistic = sum` over the window;
  sufficient statistics / sketches still apply per resolved window.
- Decide generality at ratification — the team may still choose the minimal code domain (enumerated
  seasonal codes only) if the general anchored-window model is judged overkill for the known need.
