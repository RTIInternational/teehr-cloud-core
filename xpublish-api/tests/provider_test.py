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


def test_tiles_tree_is_data_group_plus_overviews(tmp_path, monkeypatch):
    import icechunk as ic

    repo = ic.Repository.create(ic.local_filesystem_storage(str(tmp_path)))
    session = repo.writable_session("main")
    grid = {"x": np.arange(8.0), "y": np.arange(4.0)}
    data = xr.Dataset({"v": (("time", "y", "x"), np.ones((5, 4, 8)))}, coords={"time": TIMES, **grid})
    data.to_zarr(session.store, group="data", zarr_format=3, consolidated=False)
    session.commit("data only")
    monkeypatch.setattr(
        IcechunkDatasetProvider, "_discover_repo_configs", lambda self: [RepoConfig(name="r", bucket="b", prefix="p")]
    )
    monkeypatch.setattr(IcechunkDatasetProvider, "_open_repo", lambda self, cfg: repo)
    provider = IcechunkDatasetProvider(bucket="b", prefix="p", storage_kwargs={}, cache_ttl_seconds=0)

    # Before any overviews, tiles come from the data group alone
    assert list(provider.get_datatree_for_dataset("r").dataset.data_vars) == ["v"]

    session = repo.writable_session("main")
    for factor in (4, 2):
        level = data.coarsen(x=factor, y=factor).mean()
        level.to_zarr(session.store, group=f"overviews/{factor}", zarr_format=3, consolidated=False)
    session.commit("overviews")
    tree = provider.get_datatree_for_dataset("r")
    assert list(tree.children) == ["1", "2", "4"]
    assert tree["4"].sizes["x"] == 2
    # EDR reads the data group, never the overviews
    assert provider.dataset_ids() == ["r", "r_data"]
    assert not provider.get_datatree_for_dataset("r_data").children
