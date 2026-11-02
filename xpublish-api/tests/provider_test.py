"""Unit tests for provider helpers; run from xpublish-api/ with `python -m pytest tests`."""
import numpy as np
import pytest
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
    data = xr.Dataset({"v": (("time", "y", "x"), np.ones((5, 4, 8))), "crs": ((), 0)}, coords={"time": TIMES, **grid})
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
        level = data[["v"]].coarsen(x=factor, y=factor).mean()
        level.to_zarr(session.store, group=f"overviews/{factor}", zarr_format=3, consolidated=False)
    session.commit("overviews")
    tree = provider.get_datatree_for_dataset("r")
    assert list(tree.children) == ["1", "2", "4"]
    assert tree["4"].sizes["x"] == 2
    # EDR reads the data group, never the overviews
    assert provider.dataset_ids() == ["r", "r_data"]
    assert not provider.get_datatree_for_dataset("r_data").children
    # The grid mapping isn't offered as a variable
    assert list(provider.get_datatree_for_dataset("r_data").dataset.data_vars) == ["v"]
    assert "crs" in provider.get_datatree_for_dataset("r_data").dataset.coords


# --- Forecast repos: (reference_time, [member], lead_time) with per-step status ---

CYCLES = pd.date_range("2026-10-01", periods=3, freq="6h")


def _forecast_status():
    status = np.full((3, 4), 2, "int8")
    status[1] = -1  # cycle not yet written
    status[2, :2] = -2  # leads never published
    return xr.Dataset(coords={
        "reference_time": CYCLES, "lead_time": [1, 2, 3, 6],
        "status": (("reference_time", "lead_time"), status),
    })


def test_forecast_coords_keep_written_cycles_and_leads():
    ds = _forecast_status()
    assert list(written_coord_values(ds, "reference_time")) == list(CYCLES[[0, 2]].values)
    assert list(written_coord_values(ds, "lead_time")) == [1, 2, 3, 6]


def _repo_with(tmp_path, ds, root_attrs):
    import icechunk as ic
    import zarr

    repo = ic.Repository.create(ic.local_filesystem_storage(str(tmp_path)))
    session = repo.writable_session("main")
    zarr.open_group(session.store, mode="a", zarr_format=3).attrs.update(root_attrs)
    ds.to_zarr(session.store, group="data", zarr_format=3, consolidated=False)
    session.commit("repo")
    return repo


def _provider_for(monkeypatch, repo):
    monkeypatch.setattr(
        IcechunkDatasetProvider, "_discover_repo_configs", lambda self: [RepoConfig(name="r", bucket="b", prefix="p")]
    )
    monkeypatch.setattr(IcechunkDatasetProvider, "_open_repo", lambda self, cfg: repo)
    return IcechunkDatasetProvider(bucket="b", prefix="p", storage_kwargs={}, cache_ttl_seconds=0)


def _forecast_dataset():
    value_time = CYCLES.values[:, None] + np.array([1, 2, 3, 6], "timedelta64[h]")[None, :]
    return xr.Dataset(
        {"v": (("reference_time", "lead_time", "y", "x"), np.ones((3, 4, 2, 2)))},
        coords={
            "reference_time": ("reference_time", CYCLES, {"standard_name": "forecast_reference_time"}),
            "lead_time": ("lead_time", np.array([1, 2, 3, 6], "int32"), {"standard_name": "forecast_period", "units": "hours"}),
            "value_time": (("reference_time", "lead_time"), value_time, {"standard_name": "time"}),
            "x": np.arange(2.0), "y": np.arange(2.0),
        },
    )


def test_forecast_grid_spec_and_edr_time_axis(tmp_path, monkeypatch):
    pytest.importorskip("cf_xarray")
    repo = _repo_with(tmp_path, _forecast_dataset(),
                      {"append_dim": "reference_time", "fixed_dims": ["lead_time"], "layout_version": 2})
    provider = _provider_for(monkeypatch, repo)
    assert provider.dataset_metadata("r") == {
        "append_dim": "reference_time", "fixed_dims": ["lead_time"], "layout_version": 2,
    }
    assert provider.dataset_metadata("missing") is None
    ds = provider.get_datatree_for_dataset("r_data").dataset
    # EDR's datetime selects cycles: the reference time is the only CF T axis when serving
    assert ds.cf.axes["T"] == ["reference_time"]
    assert ds.lead_time.dtype == np.int32


def test_newer_layout_is_refused(tmp_path, monkeypatch):
    repo = _repo_with(tmp_path, _forecast_dataset(), {"append_dim": "reference_time", "layout_version": 3})
    with pytest.raises(ValueError, match="layout_version"):
        _provider_for(monkeypatch, repo).get_datatree_for_dataset("r_data")


def test_analysis_repo_served_as_before(tmp_path, monkeypatch):
    """No forecast attrs: the default grid spec, and the time axis and its attrs untouched."""
    pytest.importorskip("cf_xarray")
    data = xr.Dataset(
        {"v": (("time", "y", "x"), np.ones((5, 2, 2)))},
        coords={"time": ("time", TIMES, {"standard_name": "time", "axis": "T"}), "x": np.arange(2.0), "y": np.arange(2.0),
                "status": ("time", np.array([2, -1, 1, -1, 0], "int8"))},
    )
    provider = _provider_for(monkeypatch, _repo_with(tmp_path, data, {"append_dim": "time"}))
    assert provider.dataset_metadata("r") == {"append_dim": "time", "fixed_dims": [], "layout_version": 1}
    ds = provider.get_datatree_for_dataset("r_data").dataset
    assert ds.cf.axes["T"] == ["time"] and ds.time.attrs == {"standard_name": "time", "axis": "T"}
    assert list(written_coord_values(ds, "time")) == list(TIMES[[0, 2, 4]].values)
