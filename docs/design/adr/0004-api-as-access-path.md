# ADR-0004: API as the access path — writes now, reads later

- **Status:** Accepted (ratified by the team, 2026-11-13)
- **Date:** 2026-11-10
- **Deciders:** TEEHR team
- **Related:** #133, [phased plan](https://claude.ai/artifact/NDYWNmX5biKrBvPuZAdsNE), ADR-0001 (layout), spike-1 (identity propagation)

## Context

Today the API reads Trino as one static admin-level service user, and Jupyter has direct catalog
and storage access. Project workspaces need consistent schema and reliable record-keeping around
who wrote what. An open #133 question: must **all** access — including Jupyter, including reads —
go through the API, or do notebook users keep direct access?

Direct Jupyter writes make project-file record-keeping and schema consistency hard. Forcing
everything through the API immediately is disruptive to notebook users and its read performance is
unproven.

## Decision

- **The API/SDK is the only write path.** All writes go through one gateway so the internal schema
  stays consistent, whatever the surface (web, SDK, CLI, MCP, Jupyter).
- **Jupyter keeps full read access** during the transition, plus an **explicitly temporary**
  power-user sandbox.
- **Ultimate goal:** all access, reads included, goes through the API. Revisit making reads
  API-only once the transition is done and in-cluster read performance through the API is proven.

## Consequences

- **Positive:** one schema authority; every write is attributable and schema-checked; a single
  control point for isolation and quota.
- **Negative / cost:** existing Jupyter **write** workflows must move to the SDK/API; a temporary
  sandbox to operate and later retire; read-through-API latency must be measured in-cluster before
  reads can be forced through it.
- **Tradeoff accepted:** a transitional window of mixed access (API writes + direct Jupyter reads)
  rather than a hard cutover.

## Alternatives considered

- **Force all access (reads included) through the API now.** Too disruptive to notebook users, and
  read performance is unproven. Deferred to the "ultimate goal" revisit, not this ADR.
- **Keep direct Jupyter writes.** Breaks schema consistency and project record-keeping. Rejected.

## Notes

- Depends on identity propagation to the data plane (spike-1) and the write gateway (Phase 2).
- The sandbox is governed by the "Migrate forward" principle: temporary, not a second system to
  maintain forever.
