"""Spike 3 (#145): one metric definition, two backends, assert parity.

Proves the ADR-0002 claim: a single set of pure-Python metric kernels can be
driven by BOTH the vectorized-Python backend (in-process pandas) and the Spark
backend (applyInPandas) and produce identical results.

The point: the SAME `compute_metrics` function runs in both paths. Spark is only
a distribution mechanism around it, not a reimplementation of the science.

Run with the teehr venv so pyspark + the worker Python match:
  PYSPARK_PYTHON=/Users/mdenno/repos/teehr/.venv/bin/python \
  PYSPARK_DRIVER_PYTHON=/Users/mdenno/repos/teehr/.venv/bin/python \
  /Users/mdenno/repos/teehr/.venv/bin/python spike3_metric_parity.py
"""

from __future__ import annotations

import numpy as np
import pandas as pd

# --------------------------------------------------------------------------
# 1. THE SINGLE SOURCE OF TRUTH — pure kernels on numpy arrays.
#    No Spark, no pandas groupby; just the science. Both backends call this.
# --------------------------------------------------------------------------


def compute_metrics(obs: np.ndarray, pred: np.ndarray) -> dict[str, float]:
    """All metrics for one group of paired (obs, pred) values."""
    obs = np.asarray(obs, dtype="float64")
    pred = np.asarray(pred, dtype="float64")
    n = obs.size
    err = pred - obs

    mean_obs = obs.mean()
    # population std (ddof=0) — consistent by construction since it is one fn
    std_obs = obs.std()
    std_pred = pred.std()
    r = np.corrcoef(obs, pred)[0, 1]
    alpha = std_pred / std_obs
    beta = pred.mean() / mean_obs

    return {
        "count": float(n),
        "mean_error": float(err.mean()),
        "mae": float(np.abs(err).mean()),
        "rmse": float(np.sqrt((err**2).mean())),
        "nse": float(1.0 - (err**2).sum() / ((obs - mean_obs) ** 2).sum()),
        "kge": float(1.0 - np.sqrt((r - 1) ** 2 + (alpha - 1) ** 2 + (beta - 1) ** 2)),
    }


METRIC_COLS = ["count", "mean_error", "mae", "rmse", "nse", "kge"]
GROUP_COLS = ["series_key", "lead_bin"]


# --------------------------------------------------------------------------
# 2. Sample paired dataset (deterministic).
# --------------------------------------------------------------------------


def make_sample(seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    frames = []
    for s_i, series in enumerate(["g1", "g2", "g3"]):
        for lead in (0, 6):
            n = 200
            # a per-group signal so correlation/variance are non-trivial
            base = 100.0 + 20.0 * s_i + rng.normal(0, 15, n).cumsum() / 10.0
            obs = np.clip(base + rng.normal(0, 5, n), 1.0, None)
            # predicted = scaled + biased + noise (lead-dependent degradation)
            pred = np.clip(
                1.02 * obs + 2.0 + lead * 0.5 + rng.normal(0, 3 + lead, n), 0.1, None
            )
            frames.append(
                pd.DataFrame(
                    {
                        "series_key": series,
                        "lead_bin": lead,
                        "obs_value": obs,
                        "pred_value": pred,
                    }
                )
            )
    return pd.concat(frames, ignore_index=True)


# --------------------------------------------------------------------------
# 3. Backend A — vectorized Python, in-process.
# --------------------------------------------------------------------------


def run_python(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (series, lead), g in df.groupby(GROUP_COLS, sort=True):
        m = compute_metrics(g["obs_value"].to_numpy(), g["pred_value"].to_numpy())
        rows.append({"series_key": series, "lead_bin": lead, **m})
    return (
        pd.DataFrame(rows).sort_values(GROUP_COLS).reset_index(drop=True)
    )


# --------------------------------------------------------------------------
# 4. Backend B — Spark, calling the SAME compute_metrics via applyInPandas.
# --------------------------------------------------------------------------


def run_spark(df: pd.DataFrame) -> pd.DataFrame:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("spike3-metric-parity")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    out_schema = StructType(
        [
            StructField("series_key", StringType()),
            StructField("lead_bin", LongType()),
            *[StructField(c, DoubleType()) for c in METRIC_COLS],
        ]
    )

    def group_udf(pdf: pd.DataFrame) -> pd.DataFrame:
        # runs inside a Spark worker, calling the identical kernel
        m = compute_metrics(pdf["obs_value"].to_numpy(), pdf["pred_value"].to_numpy())
        return pd.DataFrame(
            [{"series_key": pdf["series_key"].iloc[0],
              "lead_bin": int(pdf["lead_bin"].iloc[0]), **m}]
        )

    sdf = spark.createDataFrame(df)
    result = (
        sdf.groupBy(*GROUP_COLS).applyInPandas(group_udf, schema=out_schema)
    )
    pdf = result.toPandas().sort_values(GROUP_COLS).reset_index(drop=True)
    spark.stop()
    return pdf


# --------------------------------------------------------------------------
# 5. Parity check.
# --------------------------------------------------------------------------


def main() -> int:
    df = make_sample()
    print(f"sample: {len(df)} rows, {df.groupby(GROUP_COLS).ngroups} groups\n")

    a = run_python(df)
    b = run_spark(df)

    # align column order/types
    a = a[GROUP_COLS + METRIC_COLS]
    b = b[GROUP_COLS + METRIC_COLS]

    merged = a.merge(b, on=GROUP_COLS, suffixes=("_py", "_spark"))
    atol = 1e-9
    print(f"max |Python - Spark| per metric (tolerance {atol}):")
    ok = True
    for c in METRIC_COLS:
        diff = (merged[f"{c}_py"] - merged[f"{c}_spark"]).abs().max()
        flag = "ok" if diff <= atol else "FAIL"
        if diff > atol:
            ok = False
        print(f"  {c:12s} {diff:.3e}  {flag}")

    print("\nPython backend results:")
    print(a.to_string(index=False))

    print("\n" + ("PASS — backends agree within tolerance." if ok else "FAIL — divergence above tolerance."))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
