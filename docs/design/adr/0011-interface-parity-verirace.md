# ADR-0011: Interface parity with VeriRace; deviate on substrate

- **Status:** Proposed (adopted as the design direction; ratify at the Phase 0 gate)
- **Date:** 2026-11-10
- **Deciders:** TEEHR team
- **Related:** #133, [phased plan](https://claude.ai/artifact/NDYWNmX5biKrBvPuZAdsNE), ADR-0001 (layout), ADR-0002 (engine), ADR-0010 (series identity), `../spec-schema-v0.md`

## Context

[VeriRace](https://github.com/mgdenno/verirace) is a working greenfield implementation whose
**interface layer** — the evaluation spec/schema, the REST API, the CLI, and the MCP server — is
well-developed and already matches the target for this refactor. Re-designing those from scratch
would duplicate solved work and invite bikeshedding. VeriRace's **substrate**, however, differs
from TEEHR's: it runs on DuckLake + DuckDB, whereas TEEHR runs on Iceberg + Polaris + Trino/Spark
at larger scale.

## Decision

**Follow VeriRace's interface contracts; deviate deliberately on the substrate.**

- **Adopt as the template** (adapting names to TEEHR conventions, per ADR-0010 etc.):
  - the **evaluation spec / schema** (inputs → pair → fields → filter → aggregate → output),
  - the **REST API** shape,
  - the **CLI**,
  - the **MCP server**.
- **Deviate on the substrate / engine**, where TEEHR's platform and scale needs differ:
  - table format: **Iceberg** (not DuckLake),
  - catalog + access control: **Polaris** REST catalog + token broker (not DuckLake/Postgres only),
  - compute: **vectorized Python + Spark** via the dual-path engine (ADR-0002), with Trino for
    interactive reads (not DuckDB fan-out),
  - identity to the data plane: Keycloak → Polaris-scoped tokens.
- **Adopt the patterns, not the code.** VeriRace is a separate, personal, early-development
  codebase on a different substrate; we re-implement its contracts, we do not fork or depend on it.

## Consequences

- **Positive:** a large head start on the interface (a proven, coherent design); consistency for
  users across spec/API/CLI/MCP; VeriRace serves as an executable reference to test our behavior
  against.
- **Negative / cost:** VeriRace is early-development and may evolve — we track it rather than pin to
  it; naming/adaptation work; some interface assumptions may not fit the Iceberg/Polaris/Spark
  substrate (scale, multi-tenant auth, job vs inline) and must be validated per contract before
  adoption.
- **Tradeoff accepted:** tracking an external design and re-homing it on our substrate, in exchange
  for not reinventing a well-solved interface.

## Alternatives considered

- **Design our own interface from scratch.** Wasteful — VeriRace already solved the spec/API/CLI/MCP
  design for this exact problem shape. Rejected.
- **Fork or depend on VeriRace directly.** It is personal, early-stage, and built on a different
  substrate; coupling to it is a liability. Rejected — adopt patterns, not the dependency.

## Notes

- Where a VeriRace contract collides with the substrate (e.g. size-based inline-vs-job routing, auth
  token flow), the TEEHR ADRs win (ADR-0002, ADR-0003, ADR-0004) and the interface is adapted.
- The spec adaptation is tracked in `../spec-schema-v0.md`.
