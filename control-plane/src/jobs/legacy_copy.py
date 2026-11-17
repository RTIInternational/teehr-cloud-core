"""Copying legacy warehouse data into ``public`` (Phase 7 groundwork), in the job's
pod, as the platform admin who started it. For now: counting what a copy would move.

The legacy tables are the running system's (namespace ``teehr``, the pre-v1 model:
``primary_timeseries``, ``secondary_timeseries``, ``locations``,
``location_crosswalks``). A copy of one forecast configuration takes its rows, the
observations over the same months, the gauges and the crosswalk; ``count`` says how
much that is, per month, before anything is written.

Counts come from Iceberg metadata when a table is partitioned by
``configuration_name`` and a time transform of ``value_time`` (exact, and quick);
otherwise from scanning ``value_time`` alone.
"""

from __future__ import annotations

import os
from collections import Counter
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThan, StartsWith

from src.jobs.load import GrantClient
from src.tables import Partitioning, PolarisTables

LEGACY = "teehr"  # the running system's namespace
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _month(t: datetime) -> str:
    return f"{t.year:04d}-{t.month:02d}"


def partition_month(value: int | None, transform: str) -> str | None:
    """The month (YYYY-MM) a time partition value falls in; None for a year or
    unknown transform (coarser than a month)."""
    if value is None:
        return None
    if transform == "month":
        return f"{1970 + value // 12:04d}-{value % 12 + 1:02d}"
    if transform == "day":
        return _month(_EPOCH + timedelta(days=value))
    if transform == "hour":
        return _month(_EPOCH + timedelta(hours=value))
    return None


def months_from_metadata(p: Partitioning, configuration: str) -> dict[str, Any] | None:
    """Rows, files and bytes per month of one configuration, from partition metadata
    (None when the table isn't partitioned so that can be told)."""
    by_source = {source: (name, transform) for name, source, transform in p.fields}
    config, time = by_source.get("configuration_name"), by_source.get("value_time")
    if p.counts is None or config is None or time is None or config[1] != "identity":
        return None
    rows: Counter[str] = Counter()
    files: Counter[str] = Counter()
    size: Counter[str] = Counter()
    for r in p.counts.to_pylist():
        part = r["partition"] or {}
        if part.get(config[0]) != configuration:
            continue
        month = partition_month(part.get(time[0]), time[1])
        if month is None:
            return None  # a partition coarser than a month: scan instead
        rows[month] += r["record_count"] - (r.get("position_delete_record_count") or 0)
        files[month] += r["file_count"]
        size[month] += r["total_data_file_size_in_bytes"]
    return _summary(
        rows, "metadata", files=sum(files.values()), size=sum(size.values())
    )


def months_from_scan(reader: pa.RecordBatchReader) -> dict[str, Any]:
    """Rows per month of value_time, counted from a scan of that column."""
    rows: Counter[str] = Counter()
    for batch in reader:
        if batch.num_rows == 0:
            continue
        months = pc.strftime(batch.column("value_time"), format="%Y-%m", locale="C")
        for item in pc.value_counts(months).to_pylist():
            if item["values"] is not None:
                rows[item["values"]] += item["counts"]
    return _summary(rows, "scan")


def _summary(rows: Counter[str], method: str, **extra: Any) -> dict[str, Any]:
    months = dict(sorted((m, n) for m, n in rows.items() if n))
    return {
        "method": method,
        "rows": sum(months.values()),
        "first": next(iter(months), None),
        "last": next(reversed(months), None) if months else None,
        "months": months,
        **{k: v for k, v in extra.items() if v is not None},
    }


def _period(months: dict[str, int]) -> tuple[datetime, datetime] | None:
    if not months:
        return None
    first = datetime.fromisoformat(f"{min(months)}-01T00:00:00+00:00")
    last = datetime.fromisoformat(f"{max(months)}-01T00:00:00+00:00")
    end = datetime(last.year + (last.month == 12), last.month % 12 + 1, 1, tzinfo=UTC)
    return first, end


def count_configuration(
    tables: PolarisTables, token: str, table: str, configuration: str,
    period: tuple[datetime, datetime] | None = None,
) -> dict[str, Any]:  # fmt: skip
    """One configuration's rows in a legacy timeseries table, per month (within
    ``period`` when given)."""
    where: Any = EqualTo("configuration_name", configuration)
    if period is not None:
        # Microseconds since the epoch bind to timestamp and timestamptz alike.
        lo, hi = (int(t.timestamp() * 1_000_000) for t in period)
        where = And(
            where,
            And(GreaterThanOrEqual("value_time", lo), LessThan("value_time", hi)),
        )
    summary = months_from_metadata(
        tables.partitioning(token, LEGACY, table, where), configuration
    )
    if summary is None:
        summary = months_from_scan(
            tables.scan_batches(token, LEGACY, table, ("value_time",), where)
        )
    if period is not None:  # metadata counts whole partitions: keep the period's months
        lo, hi = _month(period[0]), _month(period[1] - timedelta(seconds=1))
        kept = Counter({m: n for m, n in summary["months"].items() if lo <= m <= hi})
        summary = _summary(kept, summary["method"])
    return summary


def _count_rows(
    tables: PolarisTables, token: str, table: str, column: str, prefix: str
) -> int:
    reader = tables.scan_batches(
        token, LEGACY, table, (column,), StartsWith(column, prefix)
    )
    return sum(batch.num_rows for batch in reader)


def count(tables: PolarisTables, token: str, request: dict[str, Any]) -> dict[str, Any]:
    """What copying ``request["configuration"]`` would move: its forecasts, the
    observations over the same months, the crosswalk and the gauges."""
    configuration = request["configuration"]
    prefix = configuration.split("_", 1)[0] + "-"  # nwm30_medium_range -> "nwm30-"
    forecasts = count_configuration(
        tables, token, "secondary_timeseries", configuration
    )
    observations = count_configuration(
        tables, token, "primary_timeseries", request.get("observations", "usgs_observations"),
        _period(forecasts["months"]),
    )  # fmt: skip
    crosswalk = _count_rows(
        tables, token, "location_crosswalks", "secondary_location_id", prefix
    )
    gauges = _count_rows(tables, token, "locations", "id", "usgs-")
    return {
        "configuration": configuration,
        "forecasts": forecasts,
        "observations": observations,
        "crosswalk_rows": crosswalk,
        "gauges": gauges,
    }


def catalog_tables() -> PolarisTables:
    """Table I/O against Polaris from the control plane's ConfigMap settings (the
    job gets those, not the control plane's secrets, so not ``get_settings``)."""
    realm = os.getenv("POLARIS_REALM", "teehr")
    settings = SimpleNamespace(
        polaris_url=os.environ["POLARIS_URL"],
        polaris_realm=realm,
        polaris_catalog=os.getenv("POLARIS_CATALOG") or realm,
        storage_endpoint=os.getenv("STORAGE_S3_ENDPOINT") or None,
        storage_region=os.getenv("STORAGE_S3_REGION") or "us-east-1",
        storage_path_style=os.getenv("STORAGE_S3_PATH_STYLE", "").lower() == "true",
    )
    return PolarisTables(settings)  # type: ignore[arg-type]


def run_legacy_copy(client: GrantClient, task: dict[str, Any]) -> dict[str, Any]:
    """Do a legacy-copy job's task (``mode: count`` for now)."""
    request = task["copy"]
    if request.get("mode", "count") != "count":
        raise ValueError(f"mode {request['mode']!r} isn't supported yet")
    return count(catalog_tables(), client.token(), request)
