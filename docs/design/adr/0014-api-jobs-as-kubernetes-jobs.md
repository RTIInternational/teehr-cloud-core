# ADR-0014: Work the API starts runs as Kubernetes Jobs, not Prefect

- **Status:** Accepted; a run across pods amended by ADR-0017 (2026-11-18)
- **Date:** 2026-11-15
- **Deciders:** TEEHR team
- **Related:** #148, ADR-0003 (amended by this), ADR-0004 (caller identity), ADR-0011 (VeriRace
  parity; VeriRace's own ADR-0011 "Evaluation runs are Kubernetes Jobs the API starts and
  schedules"), `../system-layout.md`

## Context

ADR-0003 made Prefect the job runner for user analytics, and `system-layout.md` made staged-upload
loads Prefect jobs too. Building the upload path (Phase 3) is the first time the API has to start
work that outlives a request, so the runner gets decided now.

Prefect would be a second source of truth for a job's state, next to the control-plane database,
and a service the API depends on. Its flows are deployed by each deployment repo today, so a
core-owned load flow would need new deployment plumbing in core. VeriRace ran evaluations as
Prefect flow runs and moved them to Kubernetes Jobs the API starts (their ADR-0011), for the same
reasons: one source of truth, crash reasons a user can act on, no extra hop or service. Their
uploads are next.

## Decision

**Work a user starts through the API runs as a Kubernetes Job the control plane creates.**
Uploads first; evaluation runs (Phase 5) the same way.

- **State lives in the control-plane database** (a `jobs` record per job: kind, project, who
  started it, status, progress, problems, result). The Kubernetes Job is only where it executes.
  Reading an active job checks its Kubernetes Job, so a pod killed for memory fails the job with
  that reason instead of leaving it running.
- **The control plane creates Jobs** with its own service account, through a Role that allows
  creating, reading and deleting Jobs and reading their pods in its namespace, nothing else.
- **Job pods** run the control-plane image (`python -m src.jobs run <job id>`): non-root, no
  privilege escalation, no Kubernetes token, a memory limit, never restarted (`backoffLimit: 0`),
  kept a day after finishing for their logs.
- **A job acts as its caller (ADR-0004)**, never with the control plane's data access:
  - When the job is started, the control plane uses Keycloak's standard token exchange to turn
    the caller's access token into a refresh token bound to the caller's login session. It's
    stored encrypted with the job.
  - The job pod gets only a **job grant**: an opaque token (stored hashed) that it exchanges with
    the control plane for a fresh access token as the caller, as often as it needs. The pod
    never holds the refresh token.
  - A grant stops working when its job ends, is cancelled, or expires; a job also ends when the
    caller's login session does.
- **Jobs can be cancelled** (deletes the Kubernetes Job, marks the job cancelled).
- **Prefect stays for data flows** the platform or a deployment schedules and owns (ingests,
  backfills, maintenance), where its retries, schedules and run history earn their place and
  nothing is user-facing.
- **Unchanged from ADR-0003:** every job under the caller's own identity; per-project
  concurrency limits and an admission check before a job is accepted (added with evaluation
  runs, Phase 5).

## Consequences

- **Positive:** one source of truth for job state; failure reasons a user can act on; no
  dependency of the API on Prefect; per-job memory limits, so a bad file can't take down the
  control plane; the same pattern as VeriRace.
- **Negative / cost:** the control plane needs Kubernetes rights (Jobs only, in its namespace) and
  mounts a service-account token; job logs live in the cluster for a day (then in cluster logging
  once deployed), not in a Prefect UI; scheduling user jobs, if wanted, is ours to build.
- **Tradeoff accepted:** pod start-up latency per job (seconds), in exchange for isolation and a
  single execution path.

## Alternatives considered

- **Prefect (ADR-0003 as written).** A second source of truth, an extra service on the path, and
  core-owned deployment plumbing to build. Rejected for API-started work.
- **A worker Deployment that runs jobs in-process.** Simpler (no Kubernetes rights), but one bad
  load can exhaust the worker for everyone, and evaluation runs would need per-run pods anyway.
- **Jobs with the control plane's own identity.** Breaks ADR-0004's rule that data is only touched
  as the caller.
- **The caller sends a refresh token** (as Jupyter's broker sessions do). Works only for clients
  that hold refresh tokens; token exchange works for any caller with an access token.

## Notes

- **A job lives within the caller's Keycloak login session.** The realm uses Keycloak's
  defaults (30 minutes idle, 10 hours maximum), and both limits apply to the job:
  - The maximum counts from when the caller signed in, not from when the job started: a load
    started 9½ hours after signing in has about 30 minutes to finish.
  - A pod that waits longer than the idle timeout before its first token call (e.g. for a node
    to come up) fails with `session_ended`.
  - A running job's token refreshes keep the caller's browser session from idling out.
  - Signing out ends the job.

  Fine for loads that take minutes. Jobs that must outlive a session (long evaluation runs)
  need another answer when they appear, such as offline tokens, which outlive sign-out and
  need their own revocation story.
- **The control-plane client's secret can act as users.** With token exchange on, whoever holds
  it can turn a captured user token (from any client that adds the `teehr-control-plane`
  audience) into a refresh token for that user's session. It lives only in the control-plane
  pod. When enabling on AWS (#171), consider a separate client used only for exchange, so the
  admin-API identity and the "act as users" identity aren't lost through one secret.
- Upload staging, inspection and the load contract follow VeriRace's upload API
  (`system-layout.md`).
- **Amended 2026-11-18 (ADR-0017):** a run that needs more than one pod runs as one Indexed Job
  of N pods sharing its grant; the control plane keeps the run's shard queue, which pods claim
  through the grant. Pod 0 plans, merges and commits. Everything else here is unchanged.
