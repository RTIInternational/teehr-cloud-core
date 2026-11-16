# Upload samples

Files for trying uploads by hand on local KinD: `POST /uploads` -> PUT ->
`POST /uploads/{id}/inspect` -> `POST /uploads/{id}/ingest` (the load).

The small files are committed. `08_big_obs.csv` isn't (12 MB); make it with:

    ../../.venv/bin/python generate.py

`generate.py` rewrites every file the same way each run; change it, not the files.

| File | What inspect should say |
| --- | --- |
| 01_usgs_obs_local_times.csv | 3 sites (leading zeros kept), 144 rows; maps site_no/datetime/discharge/qualifiers; **needs mapping.timezone** (times have no offset) |
| 02_usgs_obs_utc.csv | same data with `Z` times; nothing needed beyond dataset metadata |
| 03_single_gauge.parquet | one gauge, no key column, 72 rows; **needs mapping.series_key_value** |
| 04_nwm_medium_range_ensemble.parquet | forecast: feature_id, reference_time, value_time, member, streamflow; 288 rows |
| 05_rfc_quantile_forecast.csv | quantile forecast; issue_time -> reference_time, valid_time -> value_time |
| 06_unrecognized_columns.csv | names it can't map (gauge, when, q_obs); **needs series key, value_time and value** (two numeric-looking columns, so no guess) |
| 07_not_really.parquet | **422**: not a Parquet file |
| 08_big_obs.csv | ~12 MB, 350k rows; inspect reads only the first MiB, so `rows` is null with a note |
| 09_quantile_too_precise.csv | inspects fine; the load fails: quantile 0.12345 has more than 4 decimal places |
| 10_duplicate_keys.csv | inspects fine; the load fails on the repeated key and writes nothing |

Every upload also lists `dataset.role`, `dataset.variable`, `dataset.unit` and
`dataset.statistic` as needed (the uploader says those).

Layers (`kind: layer`) and crosswalks (`kind: crosswalk`), with the three sites above:

| File | Kind | What inspect should say |
| --- | --- | --- |
| 11_gauges.geojson | layer | maps site_no / station_nm / geometry, EPSG:4326; drain_mi2 and huc_cd as attributes; nothing needed |
| 12_gauges_albers.zip | layer | a zipped Shapefile in EPSG:5070; notes it's reprojected to EPSG:4326 on load |
| 13_gauges_no_prj.zip | layer | the same with no `.prj`: **needs mapping.crs** (`EPSG:5070`) |
| 14_gauges_and_basins.gpkg | layer | two layers, `gauges` and `basins`; inspects `gauges`; `?source_layer=basins` inspects the polygons |
| 15_gauge_points.csv | layer | points from dec_long_va / dec_lat_va; huc_cd proposed as `string` (leading zeros) |
| 16_usgs_nwm_crosswalk.csv | crosswalk | primary_location_id -> left_key, secondary_location_id -> right_key, weight extra; **needs dataset.left and dataset.right** |
| 17_projected_points.csv | layer | inspects fine; the load fails: 01010500's coordinates aren't longitude/latitude |
| 18_wide_obs.csv | timeseries | wide: one column per gauge, no key column; proposes `layout: wide` with the three gauges as `series_columns` |
| 19_non_iso_times.csv | timeseries | times like `01/01/2026 00:00`: **needs mapping.time_format** (`%m/%d/%Y %H:%M`) and **mapping.timezone** |

## Before you start

The control plane and S3 ingresses need hosts entries (add to /etc/hosts):

    127.0.0.1 control-plane.teehr.local.app.garden s3.teehr.local.app.garden

Then accept both certificates once in the browser:
https://control-plane.teehr.local.app.garden/docs and https://s3.teehr.local.app.garden

## With the script

    ./upload.sh <project> 01_usgs_obs_local_times.csv
    ./upload.sh <project> 11_gauges.geojson user layer
    ./upload.sh <project> 16_usgs_nwm_crosswalk.csv user crosswalk

Create the project first (in /docs, or `POST /v1/projects {"name": "..."}`).
Optional third argument: the Keycloak user (default `user`, password = username).

## In the browser (/docs)

1. Authorize, then `POST /v1/projects` and `POST /v1/projects/{name}/uploads`
   with `{"filename": "02_usgs_obs_utc.csv", "kind": "timeseries"}`.
2. PUT the file to the returned `upload_url` (the URL works for an hour, one file):

       curl -k -X PUT --upload-file 02_usgs_obs_utc.csv "<upload_url>"

3. `POST /v1/projects/{name}/uploads/{id}/inspect`.
4. `DELETE` the upload or the project to clean up (staged files go with them).

## Loading (ingest)

After inspecting, `POST /v1/projects/{name}/uploads/{id}/ingest` (in /docs). Inspect's
`preview.ingest` is a body to start from: replace its `<...>` placeholders (ingest
refuses any left) and send it. The `dataset.name` it proposes comes from the file
name; change it to load into a dataset you already have. The
`dataset.unit` is the unit of the file's values; they're stored in the variable's
unit. For a new dataset give `role`, `variable` and `statistic`; for an existing one,
just its `name` and `unit`. `mapping` changes what inspect proposed.

02_usgs_obs_utc.csv into a new dataset:

    {"dataset": {"name": "obs", "unit": "[cft_i]/s", "role": "observed",
                 "variable": "streamflow", "statistic": "inst"}}

01_usgs_obs_local_times.csv into the same dataset (times are New York local):

    {"dataset": {"name": "obs", "unit": "[cft_i]/s"},
     "mapping": {"timezone": "America/New_York"}}

03_single_gauge.parquet (no key column):

    {"dataset": {"name": "gauge", "unit": "m3/s", "role": "observed",
                 "variable": "streamflow", "statistic": "inst"},
     "mapping": {"series_key_value": "my-gauge"}}

05_rfc_quantile_forecast.csv:

    {"dataset": {"name": "rfc", "unit": "[cft_i]/s", "role": "forecast",
                 "variable": "streamflow", "statistic": "inst"}}

A layer, then timeseries that point at it (11, then 02 with location keys):

    {"dataset": {"name": "gauges"}}

    {"dataset": {"name": "obs", "unit": "[cft_i]/s", "role": "observed",
                 "variable": "streamflow", "statistic": "inst",
                 "location_layer": "gauges"},
     "mapping": {"location_key": "site_no"}}

13_gauges_no_prj.zip (the file doesn't say its coordinate system):

    {"dataset": {"name": "gauges_albers"}, "mapping": {"crs": "EPSG:5070"}}

14_gauges_and_basins.gpkg after `inspect?source_layer=basins`:

    {"dataset": {"name": "basins"}, "mapping": {"source_layer": "basins"}}

15_gauge_points.csv, keeping only the HUC code as an attribute, renamed:

    {"dataset": {"name": "gauge_points"},
     "mapping": {"attributes": [{"column": "huc_cd", "name": "huc8"}]}}

16_usgs_nwm_crosswalk.csv between `obs` and an NWM dataset (`nwm`, say):

    {"dataset": {"name": "usgs_nwm", "left": "obs", "right": "nwm"}}

Keys not (yet) in a side's series table are written and listed in `result.notes`.

Then `GET .../uploads/{id}` until `status` is `succeeded` or `failed` (`rows_done`
shows progress; `problems` says what was wrong), and read the data with
`GET .../datasets/{name}/data`.

The load runs as a Kubernetes Job (`kubectl -n teehr-hub get jobs -l app=teehr-job`;
its pod's log has what it did). If reads say `token_refresh_required`, your token
predates your role in the project: get a new one.
