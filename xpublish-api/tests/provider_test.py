"""Unit tests for provider helpers; run from xpublish-api/ with `python -m pytest tests`."""
import numpy as np
import pandas as pd
import xarray as xr

from src.provider import written_coord_values

TIMES = pd.date_range("2000-01-01", periods=5, freq="D")


def test_drops_unwritten_time_slots():
    ds = xr.Dataset(coords={"time": TIMES, "status": ("time", np.array([2, -1, 1, -1, 0], "int8"))})
    assert list(written_coord_values(ds, "time")) == list(TIMES[[0, 2, 4]].values)


def test_unchanged_without_status():
    ds = xr.Dataset(coords={"time": TIMES})
    assert list(written_coord_values(ds, "time")) == list(TIMES.values)


def test_other_coords_unchanged():
    ds = xr.Dataset(coords={"time": TIMES, "x": [1.0, 2.0], "status": ("time", np.full(5, -1, "int8"))})
    assert list(written_coord_values(ds, "x")) == [1.0, 2.0]
    assert len(written_coord_values(ds, "time")) == 0
