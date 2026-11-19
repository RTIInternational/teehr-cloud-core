# ADR-0015: Decompose `teehr` by extraction; legacy `teehr` frozen until cutover

- **Status:** Proposed
- **Date:** 2026-11-17
- **Deciders:** TEEHR team
- **Related:** #133, #148, #246, ADR-0018 (`teehr auth login`), [phased plan](https://claude.ai/artifact/NDYWNmX5biKrBvPuZAdsNE), ADR-0002, ADR-0007, ADR-0008, ADR-0013, [`../teehr-core.md`](../teehr-core.md), teehr#871

## Context

ADR-0007 split `teehr` by function (data model, metric engine, spec engine, fetch/load,
warehouse I/O, platform wiring, client SDK) and planned to carve the package along those seams
(Phase 9, "lighten & split `teehr`"). Since then:

- **teehr-core exists** (Phase 4): the data model, metric kernels and state, the bootstrap, the
  spec and its DuckDB engine, dependency-light and shared with VeriRace.
- **The new platform doesn't use `teehr`.** The control plane depends only on teehr-core:
  datasets in the v1 data model, runs as jobs with teehr-core's engine, incremental by month.
  Neither it nor the data API imports `teehr`.
- **`teehr` is the legacy system.** Every part of it runs on Spark against the pre-v1 tables
  (`primary_timeseries`, `location_crosswalks`, `joined_timeseries`, ...), as do its users:
  every teehr-hub Prefect flow but one, the warehouse notebooks, the Jupyter images. Of its
  ~37k lines, ~19k (evaluation classes, table/view layer, migrations, Spark/Polaris session,
  pandera schemas, download) exist only for that model; ~7k (metrics) are replaced by
  teehr-core.

So the fork: keep refactoring `teehr` toward the seams (integrate teehr-core into it, then
split it), or leave it as the legacy system and build the seams as new packages.

## Decision

We will **decompose `teehr` by extraction**, not by refactoring it in place. ADR-0007's
functional seams stand; this changes the route.

- **Legacy `teehr` is frozen** (bug fixes only) and keeps serving the running system unchanged
  (ADR-0008) until Phase 7 cuts over; then it retires. It does not adopt teehr-core.
- **Each seam gets a new home**, built against the v1 data model:

  | Capability (ADR-0007) | Home |
  | --- | --- |
  | Data model, metric engine, spec engine | **teehr-core** (done) |
  | Fetch / load (USGS, NWM points and grids, gridded weights) | a **fetch package**, extracted from `teehr/fetching` (already Spark-free): produces v1 timeseries and series tables and submits them through the API (ADR-0013). Deployment ingests run it as Prefect flows; standalone users install it |
  | Very large evaluations (Spark) | **if needed**, a small backend over teehr-core's mergeable state (ADR-0002: per-partition state, merge, finalize), as a teehr-core extra or in the run job image; decided by real run sizes and the DuckDB-on-Iceberg spike (#197) |
  | Derived fields not in teehr-core (baseflow separation, some event and threshold variants) | ported into teehr-core, each through the shared spec |
  | Warehouse / catalog I/O, platform wiring | the control plane and its jobs (done for v1) |
  | Client SDK / standalone use | the Phase 6 SDK and the CLI, plus local evaluation with teehr-core's engine |

- **The `teehr` name** moves to the user package at cutover: `teehr` 1.0 is the SDK, the CLI
  and local evaluation on teehr-core, with the fetch package as an extra. Legacy `teehr` ends at
  0.x.
- **`teehr` 1.0 is built on a long-lived `v1` branch** of the `teehr` repo; `main` stays the
  frozen legacy package until cutover, when `v1` replaces it. Until then it installs from the
  branch (`uv tool install "git+https://github.com/RTIInternational/teehr.git@v1"`).
  - **The SDK and CLI come first:** a typed client for the control plane's API, and the `teehr`
    command as a thin layer on it (`teehr auth login`, ADR-0018; then uploads, runs, datasets).
    Light dependencies: no Spark, DuckDB or teehr-core.
  - **Local evaluation and the fetch package come later** (how much local evaluation matters is
    still to be decided). Each is its own module and optional extra (e.g. `teehr[local]`,
    `teehr[fetch]`), using the SDK's types; nothing in the SDK or CLI assumes a run happens in
    the cloud beyond its API calls. An MCP server (ADR-0011) would fit the same way.
  - A client need the API doesn't meet is fixed in the API first.
- **Legacy data** reaches v1 by a copy job (legacy tables → v1 datasets in `public`), runnable
  again while the legacy warehouse keeps changing, as Phase 7 plans ("port data, not code").
- teehr-core stays off PyPI until cutover; adopters pin a commit.

## Consequences

- **Positive:** no effort spent moving the legacy model onto teehr-core only to retire it; the
  running system isn't touched; each new package is built for v1 from the start; the user-facing
  name survives.
- **Negative / cost:** two systems run side by side until cutover (fixes may be needed in both);
  an API change and its client use are two PRs in two repos, with `v1` trailing the deployed API;
  the fetch code is copied, not shared, so legacy ingests and v1 ingests diverge; users move from
  `teehr.Evaluation` + Spark to the SDK, a break for notebooks.
- **Tradeoff accepted:** a hard break at cutover instead of a gradual refactor users ride along
  with.

## Alternatives considered

- **Refactor `teehr` in place** (ADR-0007's original route: teehr-core into `teehr`, then
  split). Moves code and data model that are being retired; every step has to keep the running
  system working; delays the v1 pieces. Rejected.
- **Keep `teehr` as the Spark host** (its Spark path rebuilt on teehr-core's state, rest left
  as is). Keeps the legacy table layer alive for one capability that may not be needed, and that
  is small to build fresh. Rejected; revisit if a Spark backend proves necessary and is easier
  to host there.

## Notes

- teehr#871 (adopt teehr-core in `teehr`) is closed or rewritten to the freeze.
- [`../teehr-core.md`](../teehr-core.md) step 7 is amended: VeriRace and the control plane adopt
  teehr-core; `teehr` does not.
- Open: the fetch package's name and first sources (USGS, NWM operational points); whether a
  Spark backend is needed at all; which legacy derived fields get ported.
