# ADR-0021: Gridded data products live in teehr-icechunk, outside the evaluation system

- **Status:** Proposed
- **Date:** 2026-11-22
- **Deciders:** TEEHR team
- **Related:** #263, ADR-0007 (decompose by function), ADR-0008 (repo topology), ADR-0013 (the
  boundary), ADR-0015 (legacy `teehr` frozen; fetch package), ADR-0017 (runs across pods),
  ADR-0019 (platform producers write directly), RTIInternational/teehr-icechunk#1

## Context

The deployments build Icechunk repos of gridded data (NWM forcing, snow, soon the GEFS v12
reforecast and NWM streamflow) and compute mean areal values from them. The warehouse data model
defers gridded data, and no ADR places this code:

- teehr-hub and teehr-fved each carry a byte-identical copy of it, ported by hand.
- xpublish-api and the Jupyter `icechunk_access` helper repeat the repo contract's names.
- ADR-0015's fetch package lists "NWM grids, gridded weights", which overlaps it.
- It must scale to backfills of 1B+ references and mean areal values for ~10k basins, rerun when
  the polygons change.

The fork: keep the gridded code in the deployment repos (or fold it into the fetch package), or
give it its own package with its own contract.

## Decision

**Gridded data products (Icechunk repos of N-D arrays, and zonal statistics from them) live in
a standalone package, teehr-icechunk. It is a data producer outside the evaluation system and
knows nothing of TEEHR's data model.**

- **Scope: arrays only.** Arrays may be gridded or per feature (e.g. NWM streamflow over
  `feature_id`). The package builds and updates repos from source files (references, an optional
  materialized copy, map overviews), reads any spec repo or external one, and computes zonal
  statistics. In and out are arrays and generic Arrow batches (key, times, value).
  `series_key`, teehr variables and units, configurations, location layers and warehouse writes
  are never in it.
- **One-way dependencies.** The fetch package and the deployment flows depend on teehr-icechunk,
  never the reverse:
  - Mean areal *ingest* belongs to them: polygons from a location layer, mapping to v1 dataset
    fields, deaccumulation into a period, and the write. Under ADR-0019 that is a Prefect flow
    writing through `teehr_core.warehouse`; until cutover, legacy `teehr`.
  - NWM streamflow reaches the warehouse by reading the NWM point repos, not by parsing the
    files again.
  - The NWM configuration catalog (cycles, leads, members, URLs) lives in teehr-icechunk, since
    it defines the repos; the fetch package imports it.
- **Lean, standalone package.** The base install has no Prefect, teehr or Spark. File formats
  (`[hdf]`, `[tiff]`, `[grib]`), zonal statistics (`[mean-areal]`) and orchestrators (`[prefect]`;
  others later) are extras. Work is planned as units and run through a runner interface:
  in-process, Prefect child runs, or later a Kubernetes Indexed Job like ADR-0017's.
- **The contract is versioned with the package.** `teehr-icechunk/spec/gridded-repo-contract.md`
  replaces `docs/gridded-repo-contract.md` here. Golden fixtures in the package pin what each repo
  shape stores. xpublish-api and `icechunk_access` keep their own copies of the names until they
  import the package's reader.
- **Deployments pin it by SHA** (`devTeehrIcechunkVersion`), as they pin `teehr`; the public-data
  sources (NWM, GEFS) ship in the package, and deployment-specific sources stay in the
  deployment repos.

## Consequences

- **Positive:** one copy of the gridded code and one contract; a package the fetch package,
  core's provider and outside users can share; the evaluation system stays free of gridded
  dependencies (icechunk, VirtualiZarr, GRIB codecs).
- **Negative / cost:** another repo to version and pin; an API change in it and its use in a
  deployment are two PRs; until xpublish-api imports the reader, the contract's names live in two
  places.
- **Tradeoff accepted:** a package boundary that sends warehouse-facing steps (mean areal ingest)
  back to the producers, in exchange for an array library with no TEEHR coupling.

## Alternatives considered

- **Keep the copies in teehr-hub and teehr-fved.** They drift (the 2026-09 workflow-tree review
  found drift in the shared flows), and every change is made twice. Rejected.
- **Put it in the fetch package.** That package produces v1 tables and carries the data model;
  gridded repos and their readers serve consumers that never touch the warehouse (tiles, outside
  users), and would pull icechunk and GRIB dependencies into every fetch install. Rejected; the
  fetch package depends on teehr-icechunk instead.
- **Put it in teehr-core.** teehr-core is dependency-light by design (ADR-0007). Rejected.
- **A package inside teehr-cloud-core.** Core is the deployed substrate; this is a library with
  its own release cadence and users outside a deployment (ADR-0008's test for a new repo).
  Rejected.

## Notes

- Phase 1 (2026-11-22): the code moved with no behaviour change (teehr-icechunk#2, teehr-hub#552,
  teehr-fved#219); the same inputs write identical repos before and after.
- Since then: the run model with Prefect fan-out and the GEFS v12 reforecast source (teehr-icechunk#4),
  and overviews and NWM forcing references on it (teehr-icechunk#9). The NWM sources still live in
  teehr-hub, pending the package's own NWM configuration catalog.
- Open: whether core's provider imports the package's reader; a formal spec repo with
  conformance cases if a second implementation appears.
