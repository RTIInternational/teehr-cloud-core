# ADR-0001: Warehouse layout — project = namespace, table-per-dataset

- **Status:** Accepted (team call, 2026-11)
- **Date:** 2026-11-10
- **Deciders:** TEEHR team
- **Related:** #133, [phased plan](https://claude.ai/artifact/NDYWNmX5biKrBvPuZAdsNE), ADR-0006 (migrations), ADR-0008 (repo topology), spike-2 (storage isolation)

## Context

Today the warehouse is single-tenant: one Polaris `teehr` namespace, one S3 bucket with a
whole-bucket `allowed_locations` grant, and one shared table per data type. Project workspaces
(#133) require isolation at both the catalog and storage layers, and the data must scale as it
spreads across many tenants.

Two forks, framed in the #133 discussion as jbkolze's "one database with boundaries" vs
"database per tenant":

1. **Tenancy boundary** — one shared namespace with row-level boundaries, or one namespace per
   project.
2. **Table shape** — one shared table per type (all timeseries in one table), or one table per
   dataset.

## Decision

**A project is a Polaris namespace** (tenant = namespace). **Within a namespace, each dataset is
its own typed Iceberg table** — type ∈ {timeseries, locations, crosswalks, attributes} — rather
than a shared table per type. Two **system projects** are provisioned at bootstrap (not created
through the project API):

- **`public`** — the open input data (observations, model outputs) and the shared domain tables.
  Readable by every authenticated user; written only by the platform (ingest).
- **`platform`** — platform-owned evaluations and the derived tables that power the official
  dashboards. Readable by every authenticated user; written only by the platform.

Every other project is a **tenant** namespace (members only) that can reference `public` and
`platform`, stand alone, or publish datasets back to everyone.

A `timeseries` dataset is **homogeneous** in `configuration`, `variable`, and `statistic` — any
variation is a separate table — so those (and the canonical unit) are dataset-level metadata, not
fact-row columns. `interval` is the exception: per-series (in the `series` catalog), nullable for
instantaneous data. See `../warehouse-data-model.md` and ADR-0010.

## Consequences

- **Positive:** isolation matches how Iceberg and Polaris already partition (namespace grants +
  prefix-scoped storage); each dataset gets its own snapshots, schema evolution, and lifecycle;
  geospatial datasets can carry arbitrary attribute columns inline instead of a separate
  attributes table; per-project storage prefixes drop cleanly out of per-project namespaces.
- **Negative / cost:** many more tables; the query/analytics layer must **compose datasets by
  union/view** rather than reading one table; cross-namespace reads (tenant ⊕ `public` /
  `platform`) need explicit grant handling.
- **Tradeoff accepted:** more tables and a composing planner, in exchange for real isolation and
  per-dataset lifecycle — over the simpler single-table-per-type design.

## Alternatives considered

- **One shared namespace, row-level boundaries.** Simpler table set, but tenancy enforced only in
  query predicates, not at the catalog/storage layer — a weaker isolation guarantee and harder to
  back with prefix-scoped credentials.
- **Table-per-type within per-project namespaces.** Keeps namespace isolation but loses
  per-dataset snapshots/lifecycle and the arbitrary-attribute benefit for geospatial data.

## Notes

- The migration story must now span many namespaces once project = namespace — see ADR-0006.
- S3-level isolation needs prefix-scoped `allowed_locations` + STS vending; a nominal catalog grant
  alone is not sufficient. Spike-2 closed as a documented assumption, not a live test — the real
  proof (STS credential sub-scoping on remote AWS) is part of Phase 1.
- **Amended 2026-11-13:** the system project formerly called `core` is renamed **`public`** (it is
  defined by what it holds — open, world-readable data — and matches VeriRace's name, so portable
  specs mean the same thing on both systems, ADR-0011). A second system project, **`platform`**,
  is added for platform-owned evaluations and dashboard data. No data moves; this is naming and
  structure only.
