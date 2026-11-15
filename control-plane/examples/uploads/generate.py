"""Write the upload sample files into this folder.

    python generate.py          # the small files (committed) and 08_big_obs.csv
    python generate.py --small  # only the small files

Needs pyarrow (the control-plane venv has it). Output is the same every run.
"""

import math
import random
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

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
    if "--small" not in sys.argv:
        big()
