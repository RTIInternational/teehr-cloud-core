# ADR-0009: Branching & deployment strategy during the refactor

- **Status:** Accepted
- **Date:** 2026-11-10
- **Deciders:** TEEHR team
- **Related:** #133, [phased plan](https://claude.ai/artifact/NDYWNmX5biKrBvPuZAdsNE), ADR-0008 (repo topology)

## Context

The current system stays in production while the new project-based platform is built. The risk is
that refactor work destabilizes the production trunk, since `main` of the existing repos is what
prod is built from. Two facts shape the safe path:

- **Merging to `teehr-cloud-core` `main` is not deploying to prod.** Prod deploys from the
  deployment repo (`teehr-hub`) via GitHub Actions, and `teehr-hub` pins `teehr-cloud-core` by a
  **submodule commit**. The pin bump + deployment-repo CI is the real gate to prod.
- Per ADR-0008, the bulk of the new system lives in **new repos**, not the existing ones.

ADR-0008 decides *where code lives* (repos/packages); this ADR decides *how we branch, merge, and
deploy* during the transition.

## Decision

- **Docs and inert changes merge to `main` freely** — markdown, ADRs, design artifacts change no
  manifest or image and carry no deploy impact.
- **The new system is built in new repos** (ADR-0008) and deployed to a **separate
  environment / namespace / hostname**, so legacy and its prod deploy are untouched.
- **Unavoidable changes to existing repos** (`teehr`, `teehr-cloud-core`) land on `main`
  **additive and dormant** — new tables, endpoints, and code paths gated behind flags/config and
  not wired into prod behavior until cutover. **No long-lived divergent refactor branch.**
- **Gates to prod stay explicit:** the submodule pin bump in the deployment repo plus deployment
  CI; remote environments deploy only through GitHub Actions.
- **Cutover is a routing flip** (Phase 7), not a big merge.
- **Branch/PR convention:** short-lived feature branches `<user>/<issue#>-<slug>` → PR → `main`;
  conventional commits; brief PRs. One tracking issue per phase as a sub-issue of #133; feature
  branches link to the relevant sub-issue (umbrella #133 stays open).

## Consequences

- **Positive:** prod trunk stays always-releasable; no big-merge risk; the new stack is testable
  in isolation; the single source of truth stays on `main`.
- **Negative / cost:** requires flag discipline; some additive code sits unused on `main` until
  cutover; two environments run in parallel during the transition window (cost).
- **Tradeoff accepted:** flag discipline and dormant-on-`main` code, in exchange for never
  carrying a divergent branch.

## Alternatives considered

- **Long-lived `v2` / integration branch** for existing-repo changes. Rejected — drift,
  double-maintenance, and a high-risk final merge; the opposite of insulation.
- **Build refactor directly into prod code paths.** Rejected — destabilizes production before
  cutover.

## Notes

- Reserve a short-lived integration branch only for a specific change that genuinely cannot be
  made additive or flag-gated; it is the exception, not the model.
- This ADR is why Phase 0 docs can merge to `main` now with no prod risk.
