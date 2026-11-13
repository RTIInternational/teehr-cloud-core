# ADR-0011: Interface parity with VeriRace; deviate on substrate

- **Status:** Accepted (ratified by the team, 2026-11-13)
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
- **TEEHR owns and publishes the engine-independent core as a shareable package.** The re-implemented
  engine-free pieces — the metric kernels (ADR-0002), the canonical data model, the evaluation
  spec / schema, and the mergeable metric-state format — are packaged as a clean, engine-neutral
  library that **TEEHR owns and publishes** (`teehr-core`, ADR-0007). It is built to the parity
  contract — **arrays in / arrays out, no Spark/Iceberg coupling, a numpy-only reference path** — so
  that **VeriRace, or any other engine, can adopt TEEHR's package** in place of its own. The
  dependency arrow points *at* TEEHR: we publish the shared core, others consume it; we still do not
  depend on VeriRace. The clean boundary is what makes this possible.
- **Validate against shared golden values.** We re-implement the metric code, but reuse VeriRace's
  independently-computed golden reference *values* as one of our test oracles (**data, not code**).
  This both validates the re-implementation and proves the two packages are interchangeable.

## Consequences

- **Positive:** a large head start on the interface (a proven, coherent design); consistency for
  users across spec/API/CLI/MCP; VeriRace serves as an executable reference to test our behavior
  against.
- **Portable specs (the real prize):** if TEEHR, VeriRace, and other systems share one spec schema,
  an evaluation spec becomes a portable, engine-independent artifact — the same definition runs on
  DuckDB (VeriRace), Iceberg/Spark (TEEHR), or another backend, and can be exchanged, published, and
  reproduced across systems. The spec becomes the interoperability layer, not just our internal
  format. This is a strong reason to keep the schema common rather than fork it.
- **A TEEHR-owned shareable core (clean boundaries are the enabler):** because the engine-free pieces
  sit behind an arrays-in / arrays-out boundary, they package as a standalone library TEEHR publishes
  and others adopt — convergence with VeriRace and future engines **without** TEEHR taking a
  dependency or standing up joint governance. The boundary discipline is what makes the core
  shareable in the first place.
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
- **Amended 2026-11-13** (prompted by the VeriRace team's "shared pieces" proposal): the shared-code
  direction is *TEEHR owns and publishes* the engine-free core (`teehr-core`, ADR-0007); we
  re-implement (we do **not** adopt VeriRace's package) and build it clean enough that VeriRace can
  switch to ours. This supersedes any reading of this ADR in which TEEHR would depend on VeriRace's
  package — the dependency points at TEEHR, not away from it.
