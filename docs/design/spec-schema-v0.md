# Analytics spec schema (moved)

The evaluation spec is shared with VeriRace and now lives in
[RTIInternational/teehr-shared-eval-spec](https://github.com/RTIInternational/teehr-shared-eval-spec)
(private): `spec/README.md`, with units in `standards/units.md` and periods in
`standards/periods.md`. Changes are agreed there by PR.

This file keeps only what is TEEHR's own.

## TEEHR specifics

- **Dataset references across projects:** a reference may carry a system-project prefix,
  `public.` (open input data) or `platform.` (platform-owned evaluations). No prefix means the
  project's own namespace. One spec may mix them (your forecast against
  `public.usgs_streamflow`). Applies to `dataset`, `relation` and `reference`.
- **Validation** runs against the control-plane registries (datasets, units, periods, variables;
  `warehouse-data-model.md`) and the metric kernel registry (ADR-0002).
- **Evaluation outputs** are written to `{pairs|results|groups|state}_{slug}_v{version}` tables
  (`warehouse-data-model.md`); `bootstrap` resamples the `state_*` chunks.

The draft v0 text (2026-11-10) is in git history.
