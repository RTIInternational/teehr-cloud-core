# Checking incremental runs by hand (ADR-0019)

Against KinD's `user_test.cli_nwm31_vs_usgs` (it reads `public.usgs_inst_streamflow`):

1. `teehr runs start cli_nwm31_vs_usgs -p user_test --wait`: full the first time after deploy
   ("didn't record its inputs' states").
2. Again: incremental, `changed: []`.
3. `teehr datasets write usgs_inst_streamflow examples/incremental/one_october_value.csv -p public`
   (one value of 2026-10-06 changed), then run again: incremental, `changed` holds `2026-10`
   (and any month the rewritten file also spans).
