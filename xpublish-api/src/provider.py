"""
Xpublish data provider plugin for icechunk repos.

Each discovered repo is exposed as two dataset IDs:
  - ``<name>``           -> /pyramids group (DataTree for TilesPlugin)
  - ``<name>_raw_data``  -> /raw_data group (Dataset for CfEdrPlugin)

Nothing touches S3 at import time.  Repos are discovered by listing the
top-level prefixes under ``bucket/prefix`` on the first request and re-listed
whenever ``discovery_ttl_seconds`` has elapsed, so a repo created by a Prefect
ingest workflow after the pod started is picked up without a redeploy or
restart.  Datasets themselves are loaded lazily and cached for
``cache_ttl_seconds``; after that TTL the next request re-opens a fresh
icechunk readonly session so newly written snapshots become visible.
"""

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any

import icechunk as ic
import xarray as xr
from pydantic import PrivateAttr
from xpublish import Plugin, hookimpl
from xpublish_tiles.multiscale import assign_leaf_xpublish_ids

from .storage import build_s3_client, list_storage_prefixes

logger = logging.getLogger(__name__)


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
    loaded_at: float = field(default_factory=time.monotonic)

    def is_fresh(self, ttl: float) -> bool:
        return (time.monotonic() - self.loaded_at) < ttl


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
    the lifetime of the plugin, and DataTree/Dataset objects for
    ``cache_ttl_seconds``.  Both layers refresh on their own so that new repos
    and new snapshots appear without a pod restart.
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

        The two locks are taken one after another rather than nested: the read
        path holds ``_cache_lock`` while acquiring ``_lock`` (via
        ``_open_repo``), so nesting them the other way round here could
        deadlock.
        """
        with self._lock:
            for name in names:
                self._repos.pop(name, None)
        with self._cache_lock:
            for name in names:
                self._cache.pop(name, None)
                self._cache.pop(f"{name}_raw_data", None)

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
                return cfg, "/raw_data"
        return None, ""

    def _open_repo(self, cfg: RepoConfig) -> ic.Repository:
        if cfg.name not in self._repos:
            with self._lock:
                # Double-checked locking: re-test after acquiring to avoid
                # redundant opens when multiple threads race on a cold cache.
                if cfg.name not in self._repos:
                    storage = ic.s3_storage(bucket=cfg.bucket, prefix=cfg.prefix, **self._storage_kwargs)
                    _ensure_repo_initialized(storage, self.branch)
                    self._repos[cfg.name] = ic.Repository.open(storage)
        return self._repos[cfg.name]

    def _load_datatree(self, dataset_id: str, cfg: RepoConfig, zarr_group: str) -> _CacheEntry:
        repo = self._open_repo(cfg)
        session = repo.readonly_session(self.branch)
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
            ds = xr.open_zarr(session.store, group="/raw_data", consolidated=False)
            if "time" in ds.dims:
                ds = ds.sortby("time")
            dt = xr.DataTree(dataset=ds)
            dt.attrs["_xpublish_id"] = dataset_id
        dt._icechunk_session = session  # Anchors session to dt so it outlives the _CacheEntry on cache refresh
        return _CacheEntry(datatree=dt, session=session)

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
        if entry is None or not entry.is_fresh(self.cache_ttl_seconds):
            with self._cache_lock:
                entry = self._cache.get(dataset_id)
                if entry is None or not entry.is_fresh(self.cache_ttl_seconds):
                    logger.info("Loading dataset '%s' from icechunk (cache miss or TTL expired)", dataset_id)
                    self._cache[dataset_id] = self._load_datatree(dataset_id, cfg, zarr_group)
        return self._cache[dataset_id].datatree

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
