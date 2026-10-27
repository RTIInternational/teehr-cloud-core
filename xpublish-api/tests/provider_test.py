"""Unit tests for provider helpers; run from xpublish-api/ with `python -m pytest tests`."""
import numpy as np
import pandas as pd
import xarray as xr

from src.provider import IcechunkDatasetProvider, RepoConfig, written_coord_values

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


def test_on_repos_changed_fires_only_when_the_repo_set_changes(monkeypatch):
    listings = iter([["a"], ["a"], ["a", "b"], ["b"], ["b"]])
    monkeypatch.setattr(
        IcechunkDatasetProvider,
        "_discover_repo_configs",
        lambda self: [RepoConfig(name=n, bucket="bkt", prefix=f"p/{n}") for n in next(listings)],
    )
    seen = []
    provider = IcechunkDatasetProvider(
        bucket="bkt", prefix="p", storage_kwargs={}, discovery_ttl_seconds=0, on_repos_changed=seen.append
    )
    for _ in range(5):
        provider.repo_names()
    assert seen == [["a"], ["a", "b"], ["b"]]
