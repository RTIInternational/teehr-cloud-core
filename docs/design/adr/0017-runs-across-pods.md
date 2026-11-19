# ADR-0017: A run's shards run across pods, from a queue the control plane keeps

- **Status:** Proposed
- **Date:** 2026-11-18
- **Deciders:** TEEHR team
- **Related:** #148, #228, ADR-0014 (jobs; amended by this), ADR-0016 (jobs write in-process),
  ADR-0018 (credentials), ADR-0011 (interface parity with VeriRace)

## Context

A run splits its work into shards (ranges of observed series keys, sized to the pod's memory) and
runs them one after another in a single Kubernetes Job pod (ADR-0014). On use1 a month of NWM
medium range against USGS (246M forecast rows) is 144 shards and about 7 minutes, with the pod
using less than one core. The whole record (36 months and more, more configurations) is tens of
times that. One pod, however large, runs shards in sequence; more pods can run them at once.

VeriRace runs a run's shards across pods already (its `docs/design/multi-pod-runs.md`, built
2026-11-10): the run's process queues its shards in its app database, starts worker pods, and
every pod, itself included, claims shards, writes their outputs as files, and records them; the
run's process then merges and commits as before. Its pods reach the queue with a database login.
Ours have no database access: a job holds only its grant and talks to the control plane
(ADR-0014, ADR-0016).

The fork: how pods share a run's shards, and how they reach that shared state.

## Decision

**A run that needs more than one pod runs as one Kubernetes Indexed Job of N pods. The control
plane keeps the run's shard queue in its database; pods claim, heartbeat and finish shards
through the job grant. Shards write their outputs as files; pod 0 merges them and commits.**

- **One Job, N pods.** The control plane creates the run's Job with `completionMode: Indexed`
  and `parallelism: N` (one pod, as now, below a threshold). Every pod runs the same command and
  holds the same grant. Pod 0 (`JOB_COMPLETION_INDEX=0`) plans: it pins the inputs' snapshots,
  computes the shards, and queues them. Every pod, pod 0 included, then works the queue until it
  is empty. Pod 0 merges the outputs, commits the run's tables (ADR-0016) and reports the
  outcome; the others exit when the queue is done.
- **The queue is the control plane's.** New tables in its schema: the run's shard plan (what
  every pod needs: spec, manifest, pinned snapshots, output settings) and one row per shard
  (keys, status, attempt, worker, heartbeat, outputs, error). Grant calls: plan (pod 0), claim,
  heartbeat, done, release. A claim takes the next queued shard, or one whose heartbeat has
  lapsed, by raising its attempt; every later call names that attempt, so a pod that lost its
  shard can't record it. A shard that fails is retried, up to three attempts; then the run fails
  with its error. (VeriRace's rules.)
- **Shard outputs are files, not request bodies.** A shard writes its pairs and its state rows as
  Parquet with the caller's vended credentials: pairs into the pairs table's location, state
  parts under the run's work prefix. "Done" carries only the paths (per attempt, so an abandoned
  attempt's files never count). Pod 0 reads the state parts, merges them, and commits: results
  and state as now, pairs by registering the shards' files in one commit (no partial pairs table
  after a failure). The work prefix is deleted when the run ends; orphan cleanup catches the rest.
  No run data passes through the control plane.
- **Heartbeats tolerate a control-plane restart.** A shard is reclaimed only after its heartbeat
  has lapsed for longer than a control-plane rollout plus the job client's retry window (#227):
  minutes, not seconds.
- **Settings, per deployment:** the shard count at which a run fans out (off by default), the
  pods per run, the heartbeat interval. The per-project job limit counts pods, not Jobs.
- **Credentials:** all pods use the run's one grant. Access tokens come from the control plane,
  cached and refreshed under a lock (ADR-0018), so N pods don't mean N refreshes.

This amends ADR-0014: a run's work may span several pods of one Job; everything else (Jobs the
control plane creates, grants, the caller's identity) is unchanged.

## Consequences

- **Positive:** runs scale with pods, not pod size. Uneven shards balance themselves (pods claim
  until the queue is empty). A pod that dies loses one shard's work, which another pod redoes.
  Pairs are committed once per run. Progress is visible (shards done) from the control plane,
  which helps logging (#225). Still one Job, one grant, one place to check for dead pods.
- **Negative / cost:** the control plane is on a fanned-out run's path: if it is down longer than
  the heartbeat window, shards are retried, and a long outage fails the run. A queue,
  heartbeats, reclaiming and per-attempt outputs to build and test. Pod 0 is a single point of
  failure for its run (as the one pod is today): if it dies, the run fails and is re-run.
- **Tradeoff accepted:** a call to the control plane per claim and heartbeat, instead of direct
  database access, in exchange for jobs holding only a grant.

## Alternatives considered

- **Pods read and write the queue in the database directly** (VeriRace). Fewer moving parts and
  no dependency on the control plane's availability, but every run pod would hold credentials to
  the control plane's database, which records every project. Jobs run as the caller with a grant
  and nothing more (ADR-0014); this keeps it so.
- **The coordinator starts worker Jobs** (VeriRace, via its API). One Job per worker, each with
  its own grant: more Jobs and grants to track per run for nothing an Indexed Job lacks.
- **Static assignment in an Indexed Job** (pod i takes shards i, i+N, ...). No queue, but no
  rebalancing and no retry of a dead pod's shards. VeriRace rejected it for that reason; the
  queue keeps both.
- **Larger pods only.** Helps until one node; the whole record needs more than one.
- **Spark for API runs.** Runs use the DuckDB backend per shard (ADR-0002); moving them onto a
  Spark cluster would give jobs a second execution model and a cluster to manage per run, where
  key-range shards already divide the work.

## Notes

- Build order: shard outputs as files in one pod first (the same code, results compared with
  today's), then the queue with one pod, then N pods (KinD with several nodes, then use1).
- Registering the shards' pairs files: PyIceberg `add_files` (files written without Iceberg
  field ids, matched by name) or writing data files with PyIceberg's writer and committing them;
  pick in the first step.
- Incremental runs are unchanged: the plan covers the changed months' shards only.
- The run lock (one run per version) stays with the control plane.
