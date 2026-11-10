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
than a shared table per type. The public TEEHR data becomes the `core` project (namespace); every
other project is a tenant namespace that can reference `core`, stand alone, or publish datasets
back to everyone.

## Consequences

- **Positive:** isolation matches how Iceberg and Polaris already partition (namespace grants +
  prefix-scoped storage); each dataset gets its own snapshots, schema evolution, and lifecycle;
  geospatial datasets can carry arbitrary attribute columns inline instead of a separate
  attributes table; per-project storage prefixes drop cleanly out of per-project namespaces.
- **Negative / cost:** many more tables; the query/analytics layer must **compose datasets by
  union/view** rather than reading one table; cross-namespace reads (tenant ⊕ `core`) need
  explicit grant handling.
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
- S3-level isolation (prefix-scoped `allowed_locations` + STS vending) is proven by spike-2; a
  nominal catalog grant alone is not sufficient.
