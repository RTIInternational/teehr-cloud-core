# ADR-0019: Platform producers write the warehouse directly with a shared writer; runs find changes in Iceberg metadata

- **Status:** Accepted
- **Date:** 2026-11-21
- **Deciders:** TEEHR team
- **Related:** #148, #251, ADR-0011 (teehr-core), ADR-0013 (the boundary), ADR-0014 (jobs),
  ADR-0015 (legacy `teehr` frozen; fetch package), ADR-0016 (jobs write in-process), ADR-0017
  (runs across pods), ADR-0018 (credentials)

## Context

Two kinds of scheduled work have to be rebuilt for v1 before cutover, both in Prefect, which the
team wants to keep for scheduled work:

- **Derived datasets.** Some spec features need a dataset derived from an observed one, which
  TEEHR can't make, so planning refuses them (#251):

  | Derived dataset | Used by | Rows (USGS, ~8.4k gages) |
  | --- | --- | --- |
  | **climatology**: mean per series, day of year and hour, over a smoothed window | `generate: climatology`, `generate: blend` | ~75 M |
  | **percentiles**: each series' percentiles over its record | `fn: percentile` fields (thresholds) | ~0.9 M |
  | **naive_scale**: each series' mean \|y(t) - y(t - lag)\| | `fn: naive_scale` fields, so `mase` | tiny |

  teehr-core's engine already uses them (role `auxiliary`, kind `climatology` / `percentiles` /
  `naive_scale`, the observed dataset's variable and a convertible unit, keyed by its
  `series_key`). VeriRace has the builders (`verirace/warehouse/{climatology,percentiles,
  naive_scale,derived}.py`): DuckDB SQL over hourly means, a batch of series at a time, swapped
  in at the end, rebuilt monthly by Prefect.
- **Ingests.** Today's flows use the legacy `teehr` package (Spark) and write legacy tables; that
  package goes away at cutover (ADR-0015), so every ingest is rewritten by its maintainer.

Both are **bulk, scheduled, platform-owned** writes: years of USGS and NWM, rebuilt or appended
every day or month. Sending them through the API costs an HTTP hop per batch, caps a request at
64 MiB, needs an API identity for Prefect (it has a catalog identity, not an API one), and can't
commit a large result in one transaction without a new job kind.

The reason everything went through the API was **incremental runs**: they learn which months
changed from the API's write records (`dataset_writes`), so a write made elsewhere (like the v1
copy notebook's) is invisible to them. A spike (`teehr-hub/warehouse/remote/07_change_detection_spike`)
tested deriving the same thing from Iceberg's own snapshots instead, reading each snapshot's
added and deleted manifest entries and their `value_time` bounds:

- On KinD, a controlled sequence of writes (an upload, an upsert across a month boundary, a
  filtered overwrite, a failed load, a later month) gave **the same months as the write records**
  for every recorded write, months for the overwrite the records have none for, and no snapshot
  for the failed load.
- On use1, `usgs_streamflow`, `nwm30_medium_range` and `usgs_nwm30` (56, 70 and 5 snapshots):
  every sort and compaction (`replace`, which changes no rows) was told apart and ignored, the
  Spark copy, which has no write record, **was found**, and reading costs ~0.05 s per snapshot
  (0.16 s at most).

So the warehouse itself can say what changed, whoever wrote it. Replaying snapshots, though,
needs them kept until the next run, and nightly rewrites and ingests expire a run's snapshot
within days; the live files' metadata says the same without history (below).

## Decision

**Runs find what changed from Iceberg's metadata, not from write records. Platform producers
(ingests and derived-dataset builds) run as Prefect flows and write the warehouse directly with a
shared writer, `teehr_core.warehouse`, which carries the data model's rules. Users keep writing
through the API, which uses the same writer.**

### Change detection from table state

- A run records each input's **state** at the snapshot it pinned (`result.input_states`): the
  snapshot's sequence number and, per partition, its rows and the months its files' `value_time`
  bounds span (a few hundred small entries). The next run, once pod 0 has pinned the inputs,
  compares each input's live files with that state:
  - a file written by a later commit (a higher sequence number) holds new or rewritten rows: its
    months changed;
  - a partition whose older files now hold fewer rows than recorded lost rows (deleted, or
    rewritten into new files): its recorded months changed;
  - a series table where every recorded row is still there only gained series, which changes
    nothing by itself (their rows arrive as a timeseries change); any other change to a series
    table, crosswalk or location layer makes the run full, as today;
  - a state that can't be told (delete files, files without bounds) makes the run full.
- **No history is needed:** live files keep their sequence numbers, partitions and row counts
  however many snapshots were expired since, so snapshot retention stays a storage choice. The
  first draft of this decision replayed the snapshots in between instead; nightly rewrites and
  ingests expire the last run's snapshot within days, which made most runs full.
- **Rewrites over-count, nothing is missed:** a sorted rewrite or compaction after the recorded
  state makes the files it wrote look new, so the months it rewrote are recomputed (maintenance
  rewrites only recently changed partitions, which usually changed anyway). Any change to rows
  adds a newer file or lowers a partition's count, so none goes unseen.
- The months are as of the snapshots the run reads: writes landing while it runs are after them,
  so the next run sees them once (with write records they could be counted twice).
- The API's `decide` keeps the decisions that don't need the warehouse (the spec's blockers, a
  changed manifest, missing state); the job decides the months.
- Write records stay, as history and attribution (`datasets writes`), not as the source of truth
  for runs. Producers' writes are attributed by snapshot summary properties (`teehr.producer`, the
  flow run).
- Producers should rewrite only what changed: the writer's location merge skips series whose
  locations didn't change, so an ingest setting locations each batch doesn't make runs full.

### The shared writer: `teehr_core.warehouse`

- **Where:** teehr-core, as a module behind an extra (`teehr-core[iceberg]`, PyIceberg). The
  engine stays storage-free: importing `teehr_core` doesn't import PyIceberg, so VeriRace and
  local evaluation are unaffected (ADR-0011). The control plane already depends on teehr-core;
  it moves its write code there and uses it, so there is one copy of the rules.
- **What it holds** (moved from the control plane's `tables.py` and `jobs/load.py`):
  - table layout: schema from declared columns, key columns, partitioning, sort order, layout
    properties;
  - conforming rows to a dataset's columns (`conform`), units, duplicate keys;
  - writes: append, upsert (the `key_range` overwrite), insert-missing for series, merge of
    provided columns (series' locations), and writing files for one commit (`write_file`,
    `commit_files`);
  - a timeseries write's whole step: check, add missing series, write, set locations;
  - change detection (`table_state`, `changes(table, since_state, snapshot)` → months, or why
    it can't be told);
  - snapshot summary properties for attribution.
- **What it doesn't hold:** catalogs, credentials, the registry. The caller passes a loaded table
  (or catalog); the dataset's declaration (type, variable, unit, keys, series table) is read
  from the table's `teehr.*` properties, which the registry mirrors at creation.

### Who writes where

| Writer | Path | Identity |
| --- | --- | --- |
| Users (uploads, `/data`, runs) | the API and its jobs, as today | the user (ADR-0018) |
| Ingest flows | Prefect → `teehr_core.warehouse` → Polaris | the flows' catalog identity |
| Derived-dataset builds | Prefect → `teehr_core.warehouse` → Polaris | the flows' catalog identity |
| Table maintenance | Prefect (Spark or PyIceberg) → Polaris, rewrites only | the flows' catalog identity |

- **Polaris still guards every write**: flows can write only the namespaces their principal role
  grants (`public`), never a user project.
- **Datasets are still registered through the API**, rarely and by an admin or a deploy step: a
  producer writes to a dataset that exists, and refuses one whose table has no `teehr.*`
  declaration.
- ADR-0013's boundary becomes: **users** reach data only through the API; **platform producers**
  are trusted to write `public` directly, with the shared writer enforcing the same rules the API
  does.

### Derived datasets

- **Kinds.** `climatology`, `percentiles` and `naive_scale` become dataset kinds, with columns
  and keys in teehr-core (`model.dataset_types`):
  - climatology: `series_key`, `day_of_year` (1-366), `hour` (0-23, UTC), `value`, `count`; key
    (`series_key`, `day_of_year`, `hour`);
  - percentiles: `series_key`, `percentile` (0-100), `value`, `count`, `years`; key (`series_key`,
    `percentile`);
  - naive_scale: `series_key`, `lag`, `lag_seconds`, `scale`, `count`; key (`series_key`,
    `lag_seconds`).

  Their metadata: role `auxiliary`, the source's variable and unit, and **lineage** in the build's
  snapshot summary: the source dataset and snapshot, the period, the parameters, when. The control
  plane gives them to the engine; planning stops refusing the features.
- **Builders in teehr-core** (`teehr_core.derived`), ported from VeriRace: storage-free functions
  from a batch of series' rows (Arrow) to derived rows, over hourly means. VeriRace can adopt them
  back.
- **A build flow** reads the source at one snapshot in series batches, builds each batch, writes
  its files, and commits them in **one transaction** replacing the dataset; a failed build leaves
  the old one. Monthly, after the ingests catch up, one at a time per dataset.
- **Staleness is visible:** `plan` warns when a derived dataset was built from an older snapshot
  than its source has now.
- A spec using a derived dataset runs full, as today, until derived datasets update by month.

### Flows and what ingest maintainers get

- **v1 flows** live apart from the legacy ones (`prefect-workflows/v1/`), on their own work pool,
  in an image without legacy `teehr`, Spark or a JVM: `teehr-core[iceberg]`, the fetch package,
  Prefect, DuckDB.
- **For each legacy ingest**, its maintainer gets:
  1. **The fetch package** (ADR-0015), extracted from `teehr/fetching`: sources yield v1
     timeseries and series rows (Arrow), with no Spark and no tables.
  2. **The writer** (`teehr_core.warehouse`): one call per batch to check and write a timeseries
     (series, locations, upsert), idempotent, attributed.
  3. **A template flow** in `prefect-workflows/v1/`: fetch → write, in batches, resumable, logging
     its resume point.
  4. **A migration guide:** legacy tables → v1 datasets (`primary_timeseries` → an observed
     timeseries, `secondary_timeseries` → a forecast or simulated one, `location_crosswalks` → a
     crosswalk, location attributes → series table or layer attributes, configurations →
     datasets), backfilling from the legacy warehouse, and running old and new side by side.
- The `teehr` package can offer one install for them (`teehr[ingest]`: `teehr-core[iceberg]` and
  the fetch package); the code lives in teehr-core and the fetch package.

## Consequences

- **Positive:** bulk writes skip the API and land in one commit at any size; Prefect keeps the
  scheduled work with the identity it already has; incremental runs see every write, including
  backfills, copies and fixes made by hand; one copy of the write rules serves the API, jobs and
  flows; no new job kind, no platform user or personal token.
- **Negative / cost:** moving the control plane's write code into teehr-core (a refactor, and
  teehr-core takes PyIceberg as an optional dependency). Partitions rewritten since the last run
  are recomputed though unchanged. Producers aren't checked by the API, only by the writer and
  Polaris: a producer that skips the writer can write bad rows to `public`.
- **Tradeoff accepted:** two write paths (users through the API, producers direct), in exchange
  for bulk writes that don't fight the API's limits. Change detection depends on Iceberg
  metadata, which every writer produces, instead of on every writer remembering to report.

## Alternatives considered

- **Every producer writes through the API** (this ADR's first draft: a platform user with a
  personal token, builds as a control-plane job kind). Keeps one write path, but bulk ingests pay
  an HTTP hop per batch and a 64 MiB request cap, large results need a new job kind to commit
  atomically, and Prefect needs an API identity. It existed to keep write records complete; the
  tables' own metadata makes that unnecessary.
- **Replay the snapshots between runs** (this ADR's second draft). Exact while the snapshots
  exist, but they expire within days on `public` (nightly rewrites and ingests), so it needs long
  retention (storage for every rewritten copy) or a log of what each snapshot changed, kept by
  table maintenance before it expires them. Comparing states needs neither.
- **Give jobs and flows database access** to the registry. Spreads the registry's rules across
  processes; the registry mirrors what producers need into table properties instead.
- **The writer in its own repo**, or in the `teehr` package. A new repo for a module two
  existing packages need; the `teehr` package is the user-facing client, and the control plane
  shouldn't depend on it for its own writes.
- **Build with Spark in Prefect.** Brings back the JVM; the work is per series and small per
  batch, which DuckDB handles.

## Notes

- Phasing: (1) `teehr_core.warehouse` with the writer and change detection; the control plane uses
  it; runs find months by comparing their inputs' states. (2) Derived kinds and
  builders; one flow (USGS percentiles); climatology and naive scale; staleness warnings. (3) The
  fetch package, the template flow and the migration guide; USGS observations first.
- Open: whether `datasets writes` should list producers' writes from snapshot summaries; whether
  builds run in shards from the start (USGS's whole record is ~2.4 B rows; VeriRace's
  climatology took ~26 min in one pod).
