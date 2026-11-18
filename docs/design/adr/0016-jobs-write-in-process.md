# ADR-0016: Jobs write their data in-process; the control plane keeps the record

- **Status:** Proposed
- **Date:** 2026-11-18
- **Deciders:** TEEHR team
- **Related:** #148, #228, ADR-0004 (amended by this), ADR-0014 (jobs), #223 (jobs read
  in-process), #226

## Context

ADR-0004 makes the API "the only write path": one gateway, so the schema stays consistent and
every write is attributable and schema-checked. It was written against direct writes from
Jupyter, and it holds there.

Jobs the control plane starts (ADR-0014) send everything they write to the control plane's
`POST .../data` route over HTTP, so all of a job's output passes through the control plane's
memory. That route serves the API too, from one small shared pod. Run results and uploads were
built this way, and it doesn't scale:

- Reading inputs through `/data` OOMed the control plane on a month of forecasts. Jobs now read
  the warehouse themselves (#223).
- On use1 the control plane idles near 300 MiB, and a 7,053-row results write took it past its
  512 MiB limit. The run that sent it failed, after its work was done (#226).
- Pairs tables (millions of rows) and multi-gigabyte uploads go the same way, in 48 MiB chunks.

The hop adds no checking. Job pods run the control plane's own image (ADR-0014), so the code
behind `/data` (conforming types, converting units, adding new series, writing to Iceberg) is
already in the pod. And a job already acts as its caller, so Polaris applies the same
permissions whether the job commits or the control plane commits for it.

## Decision

**Jobs the control plane starts write their data themselves, with the gateway's own write code,
as their caller. The control plane keeps the record.** This amends ADR-0004: the "write gateway"
is the write code plus the control plane's records, not a network hop that every byte crosses.

- **The data, in the job:** it uses the same functions the `/data` route uses (conform, unit
  conversion, series added, PyIceberg write or merge), with the caller's token and the storage
  credentials Polaris vends for the table, as reads do (#223).
- **The record, in the control plane:** the job reports each commit through its job grant
  (dataset, snapshot, mode, rows, months touched, series added, notes). The control plane checks
  that the snapshot exists on that table, then writes the lineage (`dataset_writes`), exactly as
  for a write through `/data`. Jobs still have no database access.
- **Creating and registering tables stays in the control plane:** run tables through the
  grant's `run-table` call, uploads' datasets through the API, as now.
- **Only jobs.** Users, the SDK and every other client keep writing through `/data`. ADR-0004 is
  otherwise unchanged.
- **Order:** run tables first (#228), then upload loads.

## Consequences

- **Positive:** run and upload data never passes through the control plane, so its memory no
  longer depends on how big a job's output is. The checks and lineage are the same, from the same
  code. One pattern for reads and writes in jobs.
- **Negative / cost:** a job reports its writes after committing them. A job that crashes in
  between leaves a commit with no lineage, which incremental runs would miss. Mitigation: before
  trusting lineage, the control plane compares the table's snapshots with what it recorded, and
  treats an unrecorded snapshot as "unknown changes" (a full run). The job image also carries
  more of the write code's dependencies, but it is already the same image.
- **Tradeoff accepted:** the control plane trusts what a job reports about its own commit (rows,
  months) after checking that the snapshot exists. A job runs our code from our image under a
  grant, never user code.

## Alternatives considered

- **Keep writing through `/data` and give the control plane more memory.** Its size would then
  have to follow the largest job's output, and one job's write still competes with every API
  request. (A modest increase is made anyway, #226, as headroom for the API itself.)
- **Scale the control plane out, or add a separate write service.** More moving parts to carry
  bytes that don't need to move.
- **The job writes data files and the control plane commits them** (add files to the table).
  This keeps commits central, but splits each write across two processes and still needs the
  control plane to act as the caller for every commit. Not worth it while jobs are our own code.

## Notes

- To the user, a run's tables are the same as before: created and registered as the caller, with
  lineage per write.
