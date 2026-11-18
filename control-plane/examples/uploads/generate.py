"""Write the upload sample files into this folder.

    python generate.py          # the small files (committed) and 08_big_obs.csv
    python generate.py --small  # only the small files

Needs pyarrow, pyogrio and shapely (the control-plane venv has them). Output is the
same every run, except the GeoPackage, which records when it was written.
"""

import math
import random
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
import shapely

HERE = Path(__file__).resolve().parent
T0 = datetime(2026, 1, 1)
SITES = ["01010000", "01010500", "02087500"]
CFS_TO_CMS = 0.028316846592

random.seed(7)


def flow(i: int, s: int) -> float:
    return round(50 + 30 * math.sin(i / 8 + s) + random.random() * 5, 2)


def write(name: str, header: str, rows: list[str]) -> None:
    (HERE / name).write_text(header + "\n" + "\n".join(rows) + "\n")


def small() -> None:
    # 1. Naive local times, cfs, qualifiers: needs mapping.timezone.
    write(
        "01_usgs_obs_local_times.csv",
        "site_no,datetime,discharge,qualifiers",
        [
            f"{s},{T0 + timedelta(hours=i):%Y-%m-%d %H:%M},{flow(i, k)},"
            f"{'P' if i > 36 else 'A'}"
            for k, s in enumerate(SITES)
            for i in range(48)
        ],
    )
    # 2. The same with UTC times.
    write(
        "02_usgs_obs_utc.csv",
        "site_no,datetime,discharge,qualifiers",
        [
            f"{s},{T0 + timedelta(hours=i):%Y-%m-%dT%H:%M:%SZ},{flow(i, k)},"
            f"{'P' if i > 36 else 'A'}"
            for k, s in enumerate(SITES)
            for i in range(48)
        ],
    )
    # 3. One gauge, no key column, zoned times: needs mapping.series_key_value.
    times = [T0.replace(tzinfo=UTC) + timedelta(hours=i) for i in range(72)]
    pq.write_table(
        pa.table(
            {
                "value_time": pa.array(times, pa.timestamp("us", tz="UTC")),
                "flow_cms": [round(flow(i, 0) * CFS_TO_CMS, 4) for i in range(72)],
            }
        ),
        HERE / "03_single_gauge.parquet",
    )
    # 4. Ensemble forecast.
    recs = []
    for feature in ("6077927", "6078177"):
        for r in range(2):
            ref = T0.replace(tzinfo=UTC) + timedelta(hours=6 * r)
            for m in range(1, 4):
                for h in range(1, 25):
                    value = round(flow(h, m) * CFS_TO_CMS, 4)
                    recs.append((feature, ref, ref + timedelta(hours=h), str(m), value))
    cols = list(zip(*recs, strict=True))
    pq.write_table(
        pa.table(
            {
                "feature_id": cols[0],
                "reference_time": pa.array(cols[1], pa.timestamp("us", tz="UTC")),
                "value_time": pa.array(cols[2], pa.timestamp("us", tz="UTC")),
                "member": cols[3],
                "streamflow": cols[4],
            }
        ),
        HERE / "04_nwm_medium_range_ensemble.parquet",
    )
    # 5. Quantile forecast with other column names.
    write(
        "05_rfc_quantile_forecast.csv",
        "location_id,issue_time,valid_time,quantile,value",
        [
            f"{loc},2026-01-01T12:00:00Z,"
            f"{T0 + timedelta(hours=12 + h):%Y-%m-%dT%H:%M:%SZ},{q},"
            f"{round(flow(h, 1) * (0.7 + q / 2), 2)}"
            for loc in ("BRKM2", "SHDM2")
            for h in range(1, 13)
            for q in (0.05, 0.5, 0.95)
        ],
    )
    # 6. Names inspect can't map.
    write(
        "06_unrecognized_columns.csv",
        "gauge,when,q_obs",
        [
            f"{SITES[0]},{T0 + timedelta(hours=i):%Y-%m-%dT%H:%M:%SZ},{flow(i, 0)}"
            for i in range(24)
        ],
    )
    # 7. Not a Parquet file.
    (HERE / "07_not_really.parquet").write_bytes(b"this is not parquet\n")
    # 9, 10. Bad values only the load job catches.
    write(
        "09_quantile_too_precise.csv",
        "location_id,issue_time,valid_time,quantile,value",
        ["BRKM2,2026-01-01T12:00:00Z,2026-01-01T13:00:00Z,0.12345,10.0"],
    )
    write(
        "10_duplicate_keys.csv",
        "site_no,datetime,discharge",
        [
            "01010000,2026-01-01T00:00:00Z,1.0",
            "01010000,2026-01-01T00:00:00Z,2.0",
        ],
    )


# The three sites of the timeseries samples, where they are.
GAUGES = [
    ("01010000", "St. John River at Ninemile Bridge, ME", -69.7156, 46.7006, 1341.0),
    ("01010500", "St. John River at Dickey, ME", -69.0884, 47.1131, 2700.0),
    ("02087500", "Neuse River near Clayton, NC", -78.4058, 35.6473, 1150.0),
]
HUCS = ["01010001", "01010002", "03020201"]


def gauges(crs: str = "EPSG:4326") -> pa.Table:
    import pyproj

    xy = [(lon, lat) for _, _, lon, lat, _ in GAUGES]
    if crs != "EPSG:4326":
        move = pyproj.Transformer.from_crs("EPSG:4326", crs, always_xy=True)
        xy = [move.transform(x, y) for x, y in xy]
    return pa.table(
        {
            "site_no": [g[0] for g in GAUGES],
            "station_nm": [g[1] for g in GAUGES],
            "drain_mi2": [g[4] for g in GAUGES],
            "huc_cd": HUCS,
            "geometry": pa.array(
                [shapely.Point(round(x, 4), round(y, 4)).wkb for x, y in xy]
            ),
        }
    )


def shapefile_zip(name: str, table: pa.Table, crs: str | None) -> None:
    """A zipped Shapefile (fixed timestamps, so the zip is the same every run)."""
    import tempfile
    import zipfile

    with tempfile.TemporaryDirectory() as tmp:
        pyogrio.write_arrow(table, f"{tmp}/gauges.shp", geometry_name="geometry",
                            geometry_type="Point", crs=crs)  # fmt: skip
        with zipfile.ZipFile(HERE / name, "w") as z:
            for ext in ("shp", "shx", "dbf", "prj", "cpg"):
                part = Path(tmp) / f"gauges.{ext}"
                if part.exists() and not (ext == "prj" and crs is None):
                    info = zipfile.ZipInfo(f"gauges.{ext}", (2026, 1, 1, 0, 0, 0))
                    z.writestr(info, part.read_bytes())


def locations() -> None:
    """Layer and crosswalk samples (11-17)."""
    for old in ("11_gauges.geojson", "14_gauges_and_basins.gpkg"):
        (HERE / old).unlink(missing_ok=True)  # GDAL won't overwrite
    # 11. GeoJSON: always EPSG:4326.
    pyogrio.write_arrow(gauges(), HERE / "11_gauges.geojson", driver="GeoJSON",
                        geometry_name="geometry", geometry_type="Point",
                        crs="EPSG:4326")  # fmt: skip
    # 12. Zipped Shapefile in CONUS Albers: reprojected on load.
    shapefile_zip("12_gauges_albers.zip", gauges("EPSG:5070"), "EPSG:5070")
    # 13. Zipped Shapefile with no .prj: needs mapping.crs (EPSG:5070).
    shapefile_zip("13_gauges_no_prj.zip", gauges("EPSG:5070"), None)
    # 14. GeoPackage with two layers: gauges (points) and basins (polygons).
    gpkg = HERE / "14_gauges_and_basins.gpkg"
    pyogrio.write_arrow(gauges(), gpkg, layer="gauges", geometry_name="geometry",
                        geometry_type="Point", crs="EPSG:4326")  # fmt: skip
    basins = pa.table(
        {
            "huc8": HUCS,
            "name": ["St. John", "Allagash", "Upper Neuse"],
            "geometry": pa.array(
                [
                    shapely.box(lon - 0.5, lat - 0.3, lon + 0.5, lat + 0.3).wkb
                    for _, _, lon, lat, _ in GAUGES
                ]
            ),
        }
    )
    pyogrio.write_arrow(basins, gpkg, layer="basins", geometry_name="geometry",
                        geometry_type="Polygon", crs="EPSG:4326")  # fmt: skip
    # 15. CSV of points (NWIS site-file columns); HUC codes keep their zeros.
    write(
        "15_gauge_points.csv",
        "site_no,station_nm,dec_lat_va,dec_long_va,huc_cd",
        [f'{g[0]},"{g[1]}",{g[3]},{g[2]},{h}' for g, h in zip(GAUGES, HUCS)],
    )
    # 16. Crosswalk USGS site -> NWM feature id (teehr's column names).
    write(
        "16_usgs_nwm_crosswalk.csv",
        "primary_location_id,secondary_location_id,weight",
        ["01010000,724696,1.0", "01010500,724838,1.0", "02087500,8731016,1.0"],
    )
    # 17. Points whose coordinates are projected, not longitude/latitude.
    write(
        "17_projected_points.csv",
        "site_no,lat,lon",
        ["01010000,46.7006,-69.7156", "01010500,2860000.0,2040000.0"],
    )


def contract() -> None:
    """Shared upload contract samples (18-19)."""
    # 18. Wide: one value column per gauge, no key column (layout wide).
    write(
        "18_wide_obs.csv",
        "datetime," + ",".join(SITES),
        [
            f"{T0 + timedelta(hours=i):%Y-%m-%dT%H:%M:%SZ},"
            + ",".join(str(flow(i, k)) for k in range(len(SITES)))
            for i in range(24)
        ],
    )
    # 19. Times that aren't ISO 8601: needs mapping.time_format (and timezone).
    write(
        "19_non_iso_times.csv",
        "site_no,datetime,discharge",
        [f"{SITES[0]},{T0 + timedelta(hours=i):%m/%d/%Y %H:%M},{flow(i, 0)}" for i in range(24)],
    )


def big() -> None:
    """8. ~12 MB, past inspect's 1 MiB CSV window (not committed)."""
    with (HERE / "08_big_obs.csv").open("w") as f:
        f.write("site_no,datetime,discharge\n")
        for k in range(40):
            site = f"0{1000000 + k * 17:07d}"
            for i in range(8760):
                stamp = T0 + timedelta(hours=i)
                f.write(f"{site},{stamp:%Y-%m-%dT%H:%M:%SZ},{flow(i, k)}\n")


if __name__ == "__main__":
    small()
    locations()
    contract()
    if "--small" not in sys.argv:
        big()
