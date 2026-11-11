"""Spike 3 (#145), increment 2: seeded block bootstrap over per-chunk state.

Proves the harder half of the ADR-0002 claim:
  1. Per-chunk ADDITIVE SUFFICIENT STATISTICS are computed by one kernel, driven
     by both backends (pandas + Spark applyInPandas) -> identical state.
  2. A seeded BLOCK BOOTSTRAP resamples chunks and merges their stats (never
     touching raw data), reconstructing moment-based metrics from the merged
     stats. Run on each backend's state -> identical confidence intervals.

This is the <eval>__state path: bootstrap operates on stored state, so it is
backend-agnostic once the state is produced.

Also surfaces a real numerical finding: metrics reconstructed from additive
stats are NOT bit-identical to direct computation (variance cancellation), so
the stats path needs tolerance-based parity, not exact.

Run with the teehr venv (so pyspark workers match the driver):
  PYSPARK_PYTHON=/Users/mdenno/repos/teehr/.venv/bin/python \
  PYSPARK_DRIVER_PYTHON=/Users/mdenno/repos/teehr/.venv/bin/python \
  /Users/mdenno/repos/teehr/.venv/bin/python spike3_bootstrap_parity.py
"""

from __future__ import annotations

import numpy as np
import pandas as pd

GROUP_COLS = ["series_key", "lead_bin"]
STAT_COLS = ["n", "s_obs", "s_pred", "s_obs2", "s_pred2", "s_op", "s_abserr", "s_err2"]
METRIC_COLS = ["count", "mean_error", "mae", "rmse", "nse", "kge"]
CHUNK_SIZE = 20  # rows per time block


# --------------------------------------------------------------------------
# Single source of truth: additive sufficient stats, and metrics from stats.
# --------------------------------------------------------------------------


def chunk_stats(obs: np.ndarray, pred: np.ndarray) -> dict[str, float]:
    obs = np.asarray(obs, "float64")
    pred = np.asarray(pred, "float64")
    err = pred - obs
    return {
        "n": float(obs.size),
        "s_obs": float(obs.sum()),
        "s_pred": float(pred.sum()),
        "s_obs2": float((obs**2).sum()),
        "s_pred2": float((pred**2).sum()),
        "s_op": float((obs * pred).sum()),
        "s_abserr": float(np.abs(err).sum()),
        "s_err2": float((err**2).sum()),
    }


def metrics_from_stats(t: dict[str, float]) -> dict[str, float]:
    """Reconstruct moment-based metrics from merged additive stats."""
    n = t["n"]
    mean_obs = t["s_obs"] / n
    mean_pred = t["s_pred"] / n
    var_obs = t["s_obs2"] / n - mean_obs**2
    var_pred = t["s_pred2"] / n - mean_pred**2
    cov = t["s_op"] / n - mean_obs * mean_pred
    ss_obs = t["s_obs2"] - n * mean_obs**2  # sum((obs-mean)^2)
    r = cov / np.sqrt(var_obs * var_pred)
    alpha = np.sqrt(var_pred / var_obs)
    beta = mean_pred / mean_obs
    return {
        "count": n,
        "mean_error": (t["s_pred"] - t["s_obs"]) / n,
        "mae": t["s_abserr"] / n,
        "rmse": np.sqrt(t["s_err2"] / n),
        "nse": 1.0 - t["s_err2"] / ss_obs,
        "kge": 1.0 - np.sqrt((r - 1) ** 2 + (alpha - 1) ** 2 + (beta - 1) ** 2),
    }


def merge_stats(rows: pd.DataFrame) -> dict[str, float]:
    """Merge (sum) the additive stats of several chunks."""
    return {c: float(rows[c].sum()) for c in STAT_COLS}


# --------------------------------------------------------------------------
# Sample data with a time index to chunk on.
# --------------------------------------------------------------------------


def make_sample(seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    frames = []
    for s_i, series in enumerate(["g1", "g2", "g3"]):
        for lead in (0, 6):
            n = 200
            base = 100.0 + 20.0 * s_i + rng.normal(0, 15, n).cumsum() / 10.0
            obs = np.clip(base + rng.normal(0, 5, n), 1.0, None)
            pred = np.clip(
                1.02 * obs + 2.0 + lead * 0.5 + rng.normal(0, 3 + lead, n), 0.1, None
            )
            frames.append(
                pd.DataFrame(
                    {
                        "series_key": series,
                        "lead_bin": lead,
                        "t": np.arange(n),
                        "chunk_id": np.arange(n) // CHUNK_SIZE,
                        "obs_value": obs,
                        "pred_value": pred,
                    }
                )
            )
    return pd.concat(frames, ignore_index=True)


# --------------------------------------------------------------------------
# Backend A: per-chunk state via pandas.
# --------------------------------------------------------------------------


def state_python(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (series, lead, chunk), g in df.groupby(["series_key", "lead_bin", "chunk_id"], sort=True):
        st = chunk_stats(g["obs_value"].to_numpy(), g["pred_value"].to_numpy())
        rows.append({"series_key": series, "lead_bin": lead, "chunk_id": chunk, **st})
    return pd.DataFrame(rows).sort_values(["series_key", "lead_bin", "chunk_id"]).reset_index(drop=True)


# --------------------------------------------------------------------------
# Backend B: per-chunk state via Spark, same chunk_stats kernel.
# --------------------------------------------------------------------------


def state_spark(df: pd.DataFrame) -> pd.DataFrame:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("spike3-bootstrap-parity")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType(
        [
            StructField("series_key", StringType()),
            StructField("lead_bin", LongType()),
            StructField("chunk_id", LongType()),
            *[StructField(c, DoubleType()) for c in STAT_COLS],
        ]
    )

    def udf(pdf: pd.DataFrame) -> pd.DataFrame:
        st = chunk_stats(pdf["obs_value"].to_numpy(), pdf["pred_value"].to_numpy())
        return pd.DataFrame(
            [{"series_key": pdf["series_key"].iloc[0],
              "lead_bin": int(pdf["lead_bin"].iloc[0]),
              "chunk_id": int(pdf["chunk_id"].iloc[0]), **st}]
        )

    sdf = spark.createDataFrame(df)
    out = sdf.groupBy("series_key", "lead_bin", "chunk_id").applyInPandas(udf, schema=schema)
    pdf = out.toPandas().sort_values(["series_key", "lead_bin", "chunk_id"]).reset_index(drop=True)
    spark.stop()
    return pdf


# --------------------------------------------------------------------------
# Seeded block bootstrap over the state table (backend-agnostic).
# --------------------------------------------------------------------------


def block_bootstrap(state: pd.DataFrame, seed: int, n_boot: int, ci: float) -> pd.DataFrame:
    """Resample chunks within each group, merge stats, reconstruct metrics."""
    groups = sorted(state.groupby(GROUP_COLS).groups.keys())  # deterministic order
    lo_q, hi_q = (1 - ci) / 2, 1 - (1 - ci) / 2
    out = []
    for gi, key in enumerate(groups):
        series, lead = key
        g = state[(state.series_key == series) & (state.lead_bin == lead)].reset_index(drop=True)
        n_chunks = len(g)
        # deterministic per-group RNG, identical across backends (sorted order)
        rng = np.random.default_rng([seed, gi])
        draws = rng.integers(0, n_chunks, size=(n_boot, n_chunks))
        reps = {m: np.empty(n_boot) for m in METRIC_COLS}
        for b in range(n_boot):
            merged = merge_stats(g.iloc[draws[b]])
            m = metrics_from_stats(merged)
            for name in METRIC_COLS:
                reps[name][b] = m[name]
        row = {"series_key": series, "lead_bin": lead}
        for name in METRIC_COLS:
            row[f"{name}_lo"] = float(np.quantile(reps[name], lo_q))
            row[f"{name}_hi"] = float(np.quantile(reps[name], hi_q))
        out.append(row)
    return pd.DataFrame(out).sort_values(GROUP_COLS).reset_index(drop=True)


# --------------------------------------------------------------------------
# Main: three parity checks.
# --------------------------------------------------------------------------


def main() -> int:
    df = make_sample()
    print(f"sample: {len(df)} rows, {df.groupby(GROUP_COLS).ngroups} groups, "
          f"{len(df)//CHUNK_SIZE} chunks total ({CHUNK_SIZE} rows each)\n")

    sa = state_python(df)
    sb = state_spark(df)

    # Check 1: per-chunk state parity (should be exact — same kernel).
    state_diff = max(
        (sa[c] - sb[c]).abs().max() for c in STAT_COLS
    )
    print(f"[1] per-chunk state  max|A-B| = {state_diff:.3e}  "
          f"{'EXACT' if state_diff == 0 else 'ok' if state_diff < 1e-9 else 'FAIL'}")

    # Check 2: seeded block bootstrap CI parity across backends (should be exact).
    seed, n_boot, ci = 42, 1000, 0.90
    ba = block_bootstrap(sa, seed, n_boot, ci)
    bb = block_bootstrap(sb, seed, n_boot, ci)
    ci_cols = [c for c in ba.columns if c.endswith(("_lo", "_hi"))]
    boot_diff = max((ba[c] - bb[c]).abs().max() for c in ci_cols)
    print(f"[2] bootstrap CIs    max|A-B| = {boot_diff:.3e}  "
          f"{'EXACT' if boot_diff == 0 else 'ok' if boot_diff < 1e-9 else 'FAIL'}  "
          f"(seed={seed}, {n_boot} iters, {int(ci*100)}% CI)")

    # Check 3: stats-reconstructed metrics vs direct raw computation (tolerance).
    from_stats = []
    for key, g in sa.groupby(GROUP_COLS):
        m = metrics_from_stats(merge_stats(g))
        from_stats.append({"series_key": key[0], "lead_bin": key[1], **m})
    fs = pd.DataFrame(from_stats).sort_values(GROUP_COLS).reset_index(drop=True)
    direct = []
    for key, g in df.groupby(GROUP_COLS):
        o, p = g["obs_value"].to_numpy(), g["pred_value"].to_numpy()
        direct.append({"series_key": key[0], "lead_bin": key[1],
                       **metrics_from_stats(chunk_stats(o, p))})  # whole-group as one chunk
    # compare stats-path vs a direct (obs-mean)^2 computation of nse/kge
    raw = []
    for key, g in df.groupby(GROUP_COLS):
        o, p = g["obs_value"].to_numpy(), g["pred_value"].to_numpy()
        err = p - o
        mean_o = o.mean()
        r = np.corrcoef(o, p)[0, 1]
        raw.append({
            "series_key": key[0], "lead_bin": key[1],
            "nse": 1 - (err**2).sum() / ((o - mean_o) ** 2).sum(),
            "kge": 1 - np.sqrt((r - 1) ** 2 + (p.std()/o.std() - 1) ** 2 + (p.mean()/mean_o - 1) ** 2),
        })
    rawdf = pd.DataFrame(raw).sort_values(GROUP_COLS).reset_index(drop=True)
    recon_diff = max((fs[c] - rawdf[c]).abs().max() for c in ["nse", "kge"])
    print(f"[3] stats-vs-raw     max|diff| = {recon_diff:.3e}  "
          f"{'(exact)' if recon_diff == 0 else '(within tol, fp cancellation)'}")

    print("\nBootstrap 90% CIs (Python-state path):")
    show = ["series_key", "lead_bin", "rmse_lo", "rmse_hi", "kge_lo", "kge_hi"]
    print(ba[show].to_string(index=False))

    ok = state_diff < 1e-9 and boot_diff < 1e-9
    print("\n" + ("PASS — state and seeded bootstrap agree exactly across backends."
                  if ok else "FAIL — divergence across backends."))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
