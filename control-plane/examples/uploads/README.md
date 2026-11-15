# Upload samples

Files for trying uploads by hand on local KinD: `POST /uploads` -> PUT ->
`POST /uploads/{id}/inspect` (the load, `ingest`, comes next; 09 and 10 are for it).

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
| 09_quantile_too_precise.csv | inspects fine; the load must refuse quantile 0.12345 |
| 10_duplicate_keys.csv | inspects fine; the load must refuse the repeated key |

Every upload also lists `dataset.role`, `dataset.variable`, `dataset.unit` and
`dataset.statistic` as needed (the uploader says those).

## Before you start

The control plane and S3 ingresses need hosts entries (add to /etc/hosts):

    127.0.0.1 control-plane.teehr.local.app.garden s3.teehr.local.app.garden

Then accept both certificates once in the browser:
https://control-plane.teehr.local.app.garden/docs and https://s3.teehr.local.app.garden

## With the script

    ./upload.sh <project> 01_usgs_obs_local_times.csv

Create the project first (in /docs, or `POST /v1/projects {"name": "..."}`).
Optional third argument: the Keycloak user (default `user`, password = username).

## In the browser (/docs)

1. Authorize, then `POST /v1/projects` and `POST /v1/projects/{name}/uploads`
   with `{"filename": "02_usgs_obs_utc.csv", "kind": "timeseries"}`.
2. PUT the file to the returned `upload_url` (the URL works for an hour, one file):

       curl -k -X PUT --upload-file 02_usgs_obs_utc.csv "<upload_url>"

3. `POST /v1/projects/{name}/uploads/{id}/inspect`.
4. `DELETE` the upload or the project to clean up (staged files go with them).
