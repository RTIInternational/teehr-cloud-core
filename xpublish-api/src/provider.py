"""
Xpublish data provider plugin for icechunk repos.

Each discovered repo is exposed as two dataset IDs:
  - ``<name>``           -> /pyramids group (DataTree for TilesPlugin)
  - ``<name>_raw_data``  -> the repo's data group (Dataset for CfEdrPlugin): the group named by
    the root ``data_group`` attribute the ingest flow records, ``/raw_data`` or ``/references``.
    Repos without it use ``/raw_data``. ``/references`` chunks are read from the source bucket,
    so repos are opened with anonymous read access to their virtual chunk containers.

Nothing touches S3 at import time.  Repos are discovered by listing the
top-level prefixes under ``bucket/prefix`` on the first request and re-listed
whenever ``discovery_ttl_seconds`` has elapsed, so a repo created by a Prefect
ingest workflow after the pod started is picked up without a redeploy or
restart.  Datasets are loaded lazily and cached; every ``cache_ttl_seconds``
the branch tip is checked and a dataset is re-opened only if it moved.
"""

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any

import icechunk as ic
import numpy as np
import xarray as xr
import zarr
from pydantic import PrivateAttr
from xpublish import Plugin, hookimpl
from xpublish_tiles.multiscale import assign_leaf_xpublish_ids

from .storage import build_s3_client, list_storage_prefixes

logger = logging.getLogger(__name__)

# Root-group attribute the ingest flow writes; must match the flow's DATA_GROUP_ATTR
DATA_GROUP_ATTR = "data_group"
DEFAULT_DATA_GROUP = "/raw_data"
# Per-step status the ingest flow writes along its time grid; -1 marks a slot not yet written
STATUS_COORD = "status"
UNWRITTEN = -1


def written_coord_values(ds: xr.Dataset, coord_name: str) -> np.ndarray:
    """A coordinate's values, without time-grid slots not yet written; unchanged for repos without status."""
    values = ds.coords[coord_name].values
    status = ds.coords.get(STATUS_COORD)
    if status is not None and status.dims == (coord_name,):
        values = values[status.values != UNWRITTEN]
    return values


def _anonymous_credentials(url_prefix: str):
    scheme = url_prefix.split("://", 1)[0]
    if scheme in ("http", "https"):
        return ic.credentials.HttpAccess
    if scheme in ("gs", "gcs"):
        return ic.Credentials.Gcs(ic.credentials.gcs_credentials(anonymous=True))
    if scheme == "s3":
        return ic.Credentials.S3(ic.credentials.s3_credentials(anonymous=True))
    return None


def _open_with_virtual_access(storage) -> ic.Repository:
    """Open a repo with anonymous read access to its virtual chunk containers (e.g. NWM on GCS)."""
    config = ic.Repository.fetch_config(storage)
    containers = (config.virtual_chunk_containers if config else None) or {}
    return ic.Repository.open(
        storage,
        authorize_virtual_chunk_access={prefix: _anonymous_credentials(prefix) for prefix in containers},
    )


def _data_group(store) -> str:
    try:
        attrs = zarr.open_group(store, mode="r", zarr_format=3).attrs
    except (zarr.errors.GroupNotFoundError, FileNotFoundError):
        return DEFAULT_DATA_GROUP
    return attrs.get(DATA_GROUP_ATTR, DEFAULT_DATA_GROUP)


@dataclass
class RepoConfig:
    name: str
    bucket: str
    prefix: str


@dataclass
class _CacheEntry:
    datatree: xr.DataTree
    # Keep the icechunk session alive for the lifetime of this cache entry.
    # The DataTree's zarr backend holds a reference to session.store; if the
    # Python session object is GC'd before the entry expires, the underlying
    # Rust session could be dropped, causing a dangling pointer on next access.
    session: Any
    snapshot_id: str
    checked_at: float = field(default_factory=time.monotonic)

    def is_checked(self, ttl: float) -> bool:
        return (time.monotonic() - self.checked_at) < ttl


def _ensure_repo_initialized(storage, branch: str) -> None:
    """Create an empty icechunk repo if one does not already exist.

    On first deployment the repo may not yet exist, which would cause
    ``ic.Repository.open()`` to raise ``icechunk.IcechunkError``.  This
    function guards against that by creating the repo and writing empty
    placeholder zarr groups for ``/pyramids`` and ``/raw_data`` so that
    ``xr.open_datatree`` and ``xr.open_zarr`` succeed at startup.  The
    Prefect ingest workflow will subsequently overwrite both groups with
    real data using ``mode="w"``.
    """
    if ic.Repository.exists(storage):
        logger.info("Icechunk repo already exists, skipping initialization")
        return

    logger.info("Icechunk repo not found — creating empty repo with placeholder groups")
    repo = ic.Repository.create(storage)
    session = repo.writable_session(branch)
    empty_ds = xr.Dataset()
    empty_ds.to_zarr(session.store, group="/pyramids", mode="w", zarr_format=3, consolidated=False)
    empty_ds.to_zarr(session.store, group="/raw_data", mode="w", zarr_format=3, consolidated=False)
    session.commit("Initialize empty repo placeholder")
    logger.info("Empty icechunk repo initialized on branch '%s'", branch)


class IcechunkDatasetProvider(Plugin):
    """Xpublish data provider plugin for icechunk repos.

    Implements ``get_datasets`` and ``get_datatree`` hookimpls so that
    xpublish resolves dataset IDs dynamically on each request.  Repos are
    discovered from ``bucket/prefix`` and re-listed on the
    ``discovery_ttl_seconds`` interval; repository objects are then cached for
    the lifetime of the plugin, and DataTree/Dataset objects until their
    branch tip moves (checked every ``cache_ttl_seconds``).  New repos and new
    snapshots appear without a pod restart.
    """

    name: str = "icechunk-dataset-provider"
    branch: str = "main"
    cache_ttl_seconds: float = 60.0
    discovery_ttl_seconds: float = 60.0

    _bucket: str = PrivateAttr()
    _prefix: str = PrivateAttr()
    _storage_kwargs: dict = PrivateAttr()
    _repo_configs: list = PrivateAttr(default_factory=list)
    _discovered_at: float | None = PrivateAttr(default=None)
    _repos: dict = PrivateAttr(default_factory=dict)
    _cache: dict = PrivateAttr(default_factory=dict)
    _dataset_locks: dict = PrivateAttr(default_factory=dict)
    _lock: threading.Lock = PrivateAttr(default_factory=threading.Lock)
    _cache_lock: threading.Lock = PrivateAttr(default_factory=threading.Lock)
    _discovery_lock: threading.Lock = PrivateAttr(default_factory=threading.Lock)

    def __init__(
        self,
        bucket: str,
        prefix: str,
        storage_kwargs: dict,
        branch: str = "main",
        cache_ttl_seconds: float = 60.0,
        discovery_ttl_seconds: float = 60.0,
    ):
        super().__init__(
            branch=branch,
            cache_ttl_seconds=cache_ttl_seconds,
            discovery_ttl_seconds=discovery_ttl_seconds,
        )
        self._bucket = bucket
        self._prefix = prefix
        self._storage_kwargs = storage_kwargs
        self._repo_configs = []
        self._discovered_at = None
        self._repos = {}
        self._cache = {}
        self._dataset_locks = {}
        self._lock = threading.Lock()
        self._cache_lock = threading.Lock()
        self._discovery_lock = threading.Lock()

    # --- Repo discovery ---

    def _discover_repo_configs(self) -> list[RepoConfig]:
        """List the top-level prefixes under ``bucket/prefix``; one repo each."""
        return [
            RepoConfig(name=item["id"], bucket=self._bucket, prefix=item["path"].rstrip("/"))
            for item in list_storage_prefixes(build_s3_client(), self._bucket, self._prefix)
        ]

    def _evict(self, names: set[str]) -> None:
        """Drop cached repos and datasets for repos that are no longer present.

        The two locks are taken one after another, never nested, so this can't
        deadlock with a load (which holds a dataset lock, then ``_lock``).
        """
        with self._lock:
            for name in names:
                self._repos.pop(name, None)
        with self._cache_lock:
            for name in names:
                for dataset_id in (name, f"{name}_raw_data"):
                    self._cache.pop(dataset_id, None)
                    self._dataset_locks.pop(dataset_id, None)

    def _repo_configs_fresh(self) -> bool:
        if self._discovered_at is None:
            return False
        return (time.monotonic() - self._discovered_at) < self.discovery_ttl_seconds

    def _refresh_repo_configs(self) -> list[RepoConfig]:
        """Return the current repo configs, re-listing S3 if the TTL expired.

        A listing failure keeps the previously discovered configs so that one
        transient S3 error does not de-register every working dataset.  The
        timestamp is stamped either way, so a hard outage is retried once per
        TTL instead of on every request.
        """
        if self._repo_configs_fresh():
            return self._repo_configs

        with self._discovery_lock:
            # Double-checked locking: another thread may have refreshed while
            # this one waited on the lock.
            if self._repo_configs_fresh():
                return self._repo_configs
            try:
                configs = self._discover_repo_configs()
            except Exception:
                logger.exception(
                    "Icechunk repo discovery failed for s3://%s/%s/ — keeping %d previously "
                    "discovered repo(s); retrying in %ss",
                    self._bucket,
                    self._prefix,
                    len(self._repo_configs),
                    self.discovery_ttl_seconds,
                )
                self._discovered_at = time.monotonic()
                return self._repo_configs

            first_discovery = self._discovered_at is None
            self._discovered_at = time.monotonic()
            previous = {cfg.name for cfg in self._repo_configs}
            current = {cfg.name for cfg in configs}
            self._repo_configs = configs

            # Only on a transition — otherwise an empty deployment would warn
            # once per TTL for as long as it runs.
            if not configs and (first_discovery or previous):
                logger.warning(
                    "No icechunk repos found under s3://%s/%s/ — no datasets registered",
                    self._bucket,
                    self._prefix,
                )
            added = current - previous
            if added:
                logger.info("Discovered icechunk repo(s): %s", sorted(added))
            removed = previous - current
            if removed:
                logger.info("Icechunk repo(s) no longer present, evicting: %s", sorted(removed))
                self._evict(removed)

        return self._repo_configs

    def repo_names(self) -> list[str]:
        """Return the tiles-capable dataset names (no ``_raw_data`` variants)."""
        return [cfg.name for cfg in self._refresh_repo_configs()]

    def dataset_ids(self) -> list[str]:
        """Return all dataset IDs served by this provider (pyramid + raw_data pairs)."""
        ids = []
        for cfg in self._refresh_repo_configs():
            ids.append(cfg.name)
            ids.append(f"{cfg.name}_raw_data")
        return ids

    def _cfg_for_dataset_id(self, dataset_id: str) -> tuple[RepoConfig | None, str]:
        for cfg in self._refresh_repo_configs():
            if dataset_id == cfg.name:
                return cfg, "/pyramids"
            if dataset_id == f"{cfg.name}_raw_data":
                return cfg, "data"  # resolved to the repo's data group on load
        return None, ""

    def _open_repo(self, cfg: RepoConfig) -> ic.Repository:
        if cfg.name not in self._repos:
            with self._lock:
                # Double-checked locking: re-test after acquiring to avoid
                # redundant opens when multiple threads race on a cold cache.
                if cfg.name not in self._repos:
                    storage = ic.s3_storage(bucket=cfg.bucket, prefix=cfg.prefix, **self._storage_kwargs)
                    _ensure_repo_initialized(storage, self.branch)
                    self._repos[cfg.name] = _open_with_virtual_access(storage)
        return self._repos[cfg.name]

    def _load_datatree(self, dataset_id: str, repo: ic.Repository, zarr_group: str, snapshot_id: str) -> _CacheEntry:
        session = repo.readonly_session(snapshot_id=snapshot_id)
        if zarr_group == "/pyramids":
            dt = xr.open_datatree(
                session.store,
                group="/pyramids",
                engine="zarr",
                decode_coords="all",
                consolidated=False,
            )
            dt.attrs["_xpublish_id"] = dataset_id
            assign_leaf_xpublish_ids(dt)
        else:
            # Lazy without dask (one task per chunk made point queries slow);
            # cache=False so a full read is never pinned in the cached dataset
            ds = xr.open_dataset(
                session.store,
                engine="zarr",
                group=_data_group(session.store),
                consolidated=False,
                chunks=None,
                cache=False,
            )
            if "time" in ds.dims and not ds.indexes["time"].is_monotonic_increasing:
                ds = ds.sortby("time")
            dt = xr.DataTree(dataset=ds)
            dt.attrs["_xpublish_id"] = dataset_id
        dt._icechunk_session = session  # Anchors session to dt so it outlives the _CacheEntry on cache refresh
        return _CacheEntry(datatree=dt, session=session, snapshot_id=snapshot_id)

    def _dataset_lock(self, dataset_id: str) -> threading.Lock:
        with self._cache_lock:
            return self._dataset_locks.setdefault(dataset_id, threading.Lock())

    def get_datatree_for_dataset(self, dataset_id: str) -> xr.DataTree | None:
        """Return a (possibly cached) DataTree for the given dataset_id.

        Called by the custom discovery endpoints in addition to the
        ``get_datatree`` hookimpl so that endpoints can reuse the same
        cached object without going through the pluggy hook machinery.
        """
        cfg, zarr_group = self._cfg_for_dataset_id(dataset_id)
        if cfg is None:
            return None
        entry = self._cache.get(dataset_id)
        if entry is not None and entry.is_checked(self.cache_ttl_seconds):
            return entry.datatree
        # Per-dataset lock: different datasets load in parallel, the same one loads once
        with self._dataset_lock(dataset_id):
            entry = self._cache.get(dataset_id)
            if entry is not None and entry.is_checked(self.cache_ttl_seconds):
                return entry.datatree
            repo = self._open_repo(cfg)
            try:
                tip = repo.lookup_branch(self.branch)
            except Exception:
                if entry is None:
                    raise
                logger.warning("Branch lookup failed for '%s'; serving cached snapshot", dataset_id, exc_info=True)
                entry.checked_at = time.monotonic()
                return entry.datatree
            if entry is not None and entry.snapshot_id == tip:
                entry.checked_at = time.monotonic()
                return entry.datatree
            logger.info("Loading dataset '%s' at snapshot %s", dataset_id, tip)
            entry = self._load_datatree(dataset_id, repo, zarr_group, tip)
            self._cache[dataset_id] = entry
            return entry.datatree

    @hookimpl
    def get_datasets(self) -> list[str]:
        return self.dataset_ids()

    @hookimpl
    def get_datatree(self, dataset_id: str, group: str) -> xr.DataTree | None:
        dt = self.get_datatree_for_dataset(dataset_id)
        if dt is None:
            return None
        if not group:
            return dt
        try:
            return dt[group]
        except KeyError:
            return None
