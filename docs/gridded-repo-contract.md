# Gridded Repo Contract

Last updated: 2026-11-02

What a gridded Icechunk repo stores, so the Prefect flows that write it (teehr-hub, teehr-fved) and the
readers (xpublish-api, the flows' overview and mean areal steps, notebooks and outside users) agree.

The names below are defined in two places that must stay in step:

- `prefect-workflows/workflows/utils/repo_const.py` in each deployment repo (kept identical in hub and fved)
- `xpublish-api/src/provider.py` here

## Two shapes

| Shape | Data dims | Example |
|---|---|---|
| Analysis | `(time, y, x)` | NWM forcing analysis, UA SWANN snow |
| Forecast | `(reference_time, [member], lead_time, y, x)` | NWM forcing short range |

A forecast repo has one **step** per (cycle, [member], lead): one full map. An analysis repo has one step per
`time`. `member` is present only for ensembles.

## Groups

| Group | Contents |
|---|---|
| `/references` | Virtual references to the source files. Always written. |
| `/data` | Materialized copy of `/references`. Only if the ingest materializes. |
| `/overviews/<factor>` | Coarser levels: the mean of each `factor`×`factor` block of pixels, on the native grid. |

Readers use the group named by the `data_group` root attribute, and the overviews for zoomed-out tiles.

## Root attributes

| Attribute | Meaning | If missing |
|---|---|---|
| `append_dim` | Axis the repo grows along: `time`, or `reference_time` for forecasts | `time` |
| `time_origin`, `time_step` | First slot and step of that axis (a forecast's step is its cycle interval) | - |
| `fixed_dims` | Axes after the append axis, in order, e.g. `["lead_time"]` or `["member", "lead_time"]` | `[]` (analysis) |
| `layout_version` | On-disk layout: `1` analysis, `2` forecast axes | `1` |
| `data_group` | `/references` or `/data` | `/data` |
| `grid_name` | Grid the data is on; repos with the same name are pixel-aligned and share mean areal weights | - |
| `configuration_name` | teehr configuration the repo holds | - |
| `timeseries_type` | In teehr's terms, `primary` (analysis) or `secondary` (forecast) data | - |

Readers refuse a `layout_version` other than 1 or 2 rather than guess at the layout.

## Coordinates

| Name | Dims | Values | CF `standard_name` |
|---|---|---|---|
| `time` | `time` | Valid time | `time` (with `axis: T`) |
| `reference_time` | `reference_time` | Forecast cycle | `forecast_reference_time` |
| `member` | `member` | Ensemble member, integer | `realization` |
| `lead_time` | `lead_time` | Lead, integer hours (`units: hours`) | `forecast_period` |
| `value_time` | `(reference_time, lead_time)` | `reference_time + lead_time` | `time` |

`time` and `value_time` label the **end** of an averaging or accumulation interval, so an analysis and a
forecast on the same grid line up with `analysis.sel(time=forecast.value_time)`.

## Time grid and per-step status

The append axis has a slot for every step from `time_origin`, whether written or not. Slots not yet
written store no chunks and read as missing. Every step also has:

| Coordinate | Meaning |
|---|---|
| `status` (int8) | `-2` not expected (the source never publishes it), `-1` not yet written, `0` early, `1` provisional, `2` stable |
| `created_at`, `updated_at` | When the step was first and last written |
| `source_last_modified` | Last modified time of the source file it came from |

These have the dims of the steps: `(time)`, or `(reference_time, [member], lead_time)`. Readers skip
steps with `status < 0`. A repo without `status` reads as fully written.

## How xpublish-api serves it

- `GET /api/datasets/{id}/metadata` returns `append_dim`, `fixed_dims` and `layout_version`, so the frontend
  knows whether to show forecast controls.
- `GET /api/datasets/{id}/coords/{name}` returns only values with a written step, e.g. cycles with any
  written lead.
- Tiles and EDR select a forecast step with query parameters named after its dims:
  `reference_time=…&lead_time=3[&member=…]`. EDR's `datetime` selects cycles.

## Changing the contract

- Adding a name is safe: give it a default when missing, and update both places listed above.
- Renaming one, or changing a value's meaning, breaks existing repos and readers.
- Bump `layout_version` only for an on-disk change existing readers can't handle. Deploy readers that
  know the new version before writing any repo that uses it.
