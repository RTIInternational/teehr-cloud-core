# ADR-0019: Derived datasets are built by control-plane jobs on a schedule; v1 flows are API clients

- **Status:** Proposed
- **Date:** 2026-11-21
- **Deciders:** TEEHR team
- **Related:** #148, #251, ADR-0013 (the boundary), ADR-0014 (jobs), ADR-0015 (legacy `teehr` frozen;
  fetch package), ADR-0016 (jobs write in-process), ADR-0017 (runs across pods), ADR-0018
  (credentials)

## Context

Some spec features need a dataset derived from an observed one, which TEEHR can't make, so
planning refuses them (#251):

| Derived dataset | Used by | Rows (USGS, ~8.4k gages) |
| --- | --- | --- |
| **climatology**: mean per series, day of year and hour, over a smoothed window | `generate: climatology`, `generate: blend` | ~75 M |
| **percentiles**: each series' percentiles over its record | `fn: percentile` fields (thresholds) | ~0.9 M |
| **naive_scale**: each series' mean \|y(t) - y(t - lag)\| | `fn: naive_scale` fields, so `mase` | tiny |

teehr-core's engine already uses them: an engine dataset with role `auxiliary`, kind `climatology`
/ `percentiles` / `naive_scale`, the observed dataset's variable and a convertible unit, keyed by
the observed dataset's `series_key`. teehr-core has no builders; VeriRace has them
(`verirace/warehouse/{climatology,percentiles,naive_scale,derived}.py`): DuckDB SQL over hourly
means, a batch of series at a time, swapped in at the end, rebuilt monthly by Prefect flows.

The same question arises for ingests. Today's Prefect flows use the legacy `teehr` package (Spark)
and write legacy tables; that package is frozen and goes away at cutover (ADR-0015), so every flow
is rewritten for v1 before then. What we learned building v1 so far constrains how:

- **Writes must go through the evaluation system.** Incremental runs know what changed from the
  write records (`dataset_writes`); a write straight to an Iceberg table is invisible to them, so
  runs silently miss it. (The v1 copy notebook does this today.)
- **Reading a whole record at scale is solved inside the control plane** (run jobs: PyIceberg plans,
  DuckDB reads, files pruned by series range), and private to it (ADR-0013).
- **Big results need one commit.** A climatology is far beyond one `/data` request (64 MiB); written
  as many requests, a run could read half of one. Jobs commit files in one transaction
  (ADR-0016/0017).
- **No flow can act on the API today:** the Prefect service account's tokens aren't accepted by the
  control plane, and a realm role doesn't grant API write on `public`.

## Decision

**Derived datasets are part of the evaluation system: registered as dataset kinds of their own,
computed by teehr-core builders, and built by control-plane jobs, which Prefect flows start on a
schedule through the API. Every v1 flow (builds, ingests) is an API client, acting as a platform
user with a personal token, and doesn't use the legacy `teehr` package.**

### Derived datasets

- **Kinds.** `climatology`, `percentiles` and `naive_scale` are dataset kinds, with their columns and
  keys defined in teehr-core (`model.dataset_types`, beside `crosswalk_columns`):
  - climatology: `series_key`, `day_of_year` (1-366), `hour` (0-23, UTC), `value`, `count`; key
    (`series_key`, `day_of_year`, `hour`);
  - percentiles: `series_key`, `percentile` (0-100), `value`, `count`, `years`; key (`series_key`,
    `percentile`);
  - naive_scale: `series_key`, `lag`, `lag_seconds`, `scale`, `count`; key (`series_key`,
    `lag_seconds`).

  Their metadata: role `auxiliary`, the source dataset's variable and unit, and the build's
  **lineage**: the source dataset, the snapshot it was built from, the period covered, the
  parameters (window, levels, minimum years, lags), when it was built. The control plane gives them
  to the engine with their variable and unit; planning stops refusing the features.
- **Builders in teehr-core** (`teehr_core.derived`), ported from VeriRace's: functions from a batch
  of an observed series' rows (Arrow) to the derived rows, over hourly means where VeriRace uses
  them, so a change in reporting interval doesn't skew weights. Storage-free, so VeriRace can adopt
  them back (ADR-0011's direction: TEEHR publishes the shared core).
- **A build is a job** (ADR-0014's third kind, beside uploads and runs):
  `POST /v1/projects/{p}/datasets/{name}/build` registers the dataset if new and starts it; the job
  reads the source at one pinned snapshot in series batches (the run jobs' reader), builds each with
  teehr-core, writes the batches' files, and commits them in **one transaction** replacing the
  dataset (ADR-0017's `commit_files`), then reports the write with its lineage. A failed build leaves
  the old dataset as it was. Large sources spread over pods as runs do (ADR-0017), later if needed.
- **Staleness is visible.** `plan` warns when a derived dataset was built from an older snapshot than
  its source has now, by how many months of writes.
- **Rebuilds and incremental runs.** A spec using a derived dataset already runs full (its inputs
  aren't timeseries); that stays until derived datasets can be updated by month.

### Flows

- **Prefect schedules; the evaluation system computes and writes.** A build flow is a few lines:
  `client.build_dataset(...)`, wait, report. Scheduled monthly after the ingests catch up, one at a
  time per dataset (`concurrency_limit`, cancel new).
- **Ingests become API clients too** (ADR-0013, ADR-0015): fetch with the fetch package, write with
  the `teehr` SDK: `/data` for small batches (one commit each), an upload for large ones (one
  commit, up to 5 GB). Every write is recorded, so incremental runs see it.
- **Identity: a platform user with a personal token.** A Keycloak user (`teehr-platform`), a member
  of `public` and `platform` with the write role, makes a long-lived personal token (ADR-0018) once;
  it's stored as a Kubernetes secret the v1 flows read (`TEEHR_TOKEN`). Its writes are attributed to
  it, its jobs act through its session, and it can be revoked. No new kind of credential.
- **A v1 flow image without legacy `teehr`, Spark or a JVM:** the `teehr` SDK, the fetch package,
  Prefect. Spark stays available to flows that need it (table maintenance), not as the default. v1
  flows live apart from the legacy ones (`prefect-workflows/v1/`), on their own work pool (ADR-0013).
- **Nothing writes v1 tables directly**, except table maintenance that changes no rows (sorted
  rewrites, expiring snapshots). The copy notebook's direct writes are replaced by writes through
  the API, or reported (ADR-0016).

### Migrating ingests: what ingest maintainers get

Each legacy ingest is rewritten by whoever maintains it, against:

1. **The fetch package** (ADR-0015), extracted from `teehr/fetching`: sources yield v1 timeseries
   and series rows (Arrow), with no Spark and no tables.
2. **The `teehr` SDK's write path:** create or find the dataset, write a batch (it picks `/data` or
   an upload by size), add series and their locations, idempotent (upserts), with the write
   recorded.
3. **A template flow** in `prefect-workflows/v1/`: fetch → write, in batches, resumable, logging its
   resume point; the platform token; the v1 image; the v1 work pool.
4. **A migration guide:** the legacy table → v1 dataset mapping (`primary_timeseries` → an observed
   timeseries, `secondary_timeseries` → a forecast or simulated one, `location_crosswalks` → a
   crosswalk, location attributes → series table or layer attributes, configurations → datasets),
   how to backfill from the legacy warehouse (the copy notebook's job, through the API), and how to
   run old and new side by side and compare before switching.

## Consequences

- **Positive:** derived datasets reuse what runs already do well (reading at scale, one atomic
  commit, lineage, acting as a user), with no Spark and no legacy `teehr`. Flows stay thin and
  replaceable, and every write is visible to incremental runs. The builders are shared with VeriRace.
  Ingest maintainers get one pattern and its tools.
- **Negative / cost:** a third job kind and a build route in the control plane. A platform user and
  its token to manage (expiry: renew yearly, or the offline-session cap is raised for it). Ingests
  pay an HTTP hop per batch (bounded by batching; uploads for bulk).
- **Tradeoff accepted:** builds run on the evaluation system's compute, not Prefect's. A build is a
  read-heavy job like a run, so it's sized and scaled the same way.

## Alternatives considered

- **Build in Prefect flows directly** (read Iceberg with PyIceberg/DuckDB, write via the API or
  Polaris). Duplicates the run jobs' reader outside the boundary (ADR-0013), can't commit a large
  result atomically through `/data`, and writing to Polaris directly skips lineage.
- **Build with Spark in Prefect.** Brings back the JVM and the legacy session setup the v1 path is
  leaving; the work is per series and small per batch, which DuckDB handles.
- **Flows authenticate as a Keycloak service account (client credentials).** Needs the control
  plane to accept another audience and system roles to grant API writes, and jobs to act as a
  service session (token exchange of a client-credentials token): new code paths for one caller. A
  platform user with a personal token uses what exists.
- **Derived datasets as plain tables** (`kind: table`). The engine needs their variable and unit,
  and untyped tables are admin-only; typed kinds also let planning and builds check columns.

## Notes

- Phasing: (1) this ADR; the kinds and builders in teehr-core; the build job and route; the
  platform user; one flow (USGS percentiles). (2) Climatology and naive scale; staleness warnings.
  (3) The fetch package, the SDK write path, the template flow and the migration guide; USGS
  observations first.
- Open: the platform token's lifetime (one year, the offline-session cap); whether builds run in
  shards from the start (USGS's whole record is ~2.4 B rows; VeriRace's climatology took ~26 min in
  one pod).
