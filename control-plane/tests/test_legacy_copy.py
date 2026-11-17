"""Copying legacy warehouse data into public: for now, counting what a copy moves."""

from datetime import datetime

import pyarrow as pa
import pytest

from src.jobs import legacy_copy
from src.jobs.legacy_copy import (
    count,
    months_from_metadata,
    months_from_scan,
    partition_month,
)
from src.jobs.load import GrantClient
from src.tables import Partitioning
from tests.test_datasets import ALICE, ROOT
from tests.test_datasets import tables as tables  # noqa: F401  (fixture)
from tests.test_ingest import grant_of
from tests.test_ingest import jobs as jobs  # noqa: F401  (fixture)
from tests.test_layers import public as public  # noqa: F401  (fixture)
from tests.test_uploads import staging as staging  # noqa: F401  (fixture)

URL = "/v1/admin/legacy-copies"
FIELDS = (
    ("configuration_name", "configuration_name", "identity"),
    ("value_time_month", "value_time", "month"),
)


def months_since_1970(year, month):
    return (year - 1970) * 12 + month - 1


def partitions(rows):
    """A partitions table from (configuration, month, records[, deleted]) rows."""
    rows = [(*r, 0) if len(r) == 3 else r for r in rows]
    return pa.table({
        "partition": [{"configuration_name": c, "value_time_month": m} for c, m, _, _ in rows],
        "record_count": [n for _, _, n, _ in rows],
        "file_count": [1] * len(rows),
        "total_data_file_size_in_bytes": [n * 10 for _, _, n, _ in rows],
        "position_delete_record_count": [d for _, _, _, d in rows],
    })  # fmt: skip


def test_partition_months():
    assert partition_month(months_since_1970(2024, 1), "month") == "2024-01"
    assert partition_month(months_since_1970(2024, 12), "month") == "2024-12"
    assert partition_month(19_723, "day") == "2024-01"  # 2024-01-01
    assert partition_month(19_723 * 24 + 5, "hour") == "2024-01"
    assert partition_month(54, "year") is None and partition_month(None, "day") is None


def test_counts_from_partition_metadata():
    jan, feb = months_since_1970(2024, 1), months_since_1970(2024, 2)
    p = Partitioning(FIELDS, partitions([
        ("nwm30_medium_range", jan, 100), ("nwm30_medium_range", feb, 50, 5),
        ("nwm30_short_range", jan, 999),
    ]))  # fmt: skip
    got = months_from_metadata(p, "nwm30_medium_range")
    assert got["method"] == "metadata"
    assert got["months"] == {"2024-01": 100, "2024-02": 45}  # deletes subtracted
    assert (got["rows"], got["first"], got["last"]) == (145, "2024-01", "2024-02")
    assert got["files"] == 2 and got["size"] == 1500
    # Not partitioned by configuration (or by year only): scan instead.
    assert months_from_metadata(Partitioning(FIELDS[1:], None), "x") is None
    yearly = (FIELDS[0], ("value_time_year", "value_time", "year"))
    assert months_from_metadata(Partitioning(yearly, partitions([])), "x") is not None
    years = pa.table({
        "partition": [{"configuration_name": "x", "value_time_year": 54}],
        "record_count": [1], "file_count": [1], "total_data_file_size_in_bytes": [1],
        "position_delete_record_count": [0],
    })  # fmt: skip
    assert months_from_metadata(Partitioning(yearly, years), "x") is None


def batches(times):
    table = pa.table({"value_time": pa.array(times, pa.timestamp("us"))})
    return pa.RecordBatchReader.from_batches(
        table.schema, table.to_batches(max_chunksize=2)
    )


def test_counts_from_a_scan():
    got = months_from_scan(batches([datetime(2024, 1, d) for d in (1, 2, 3)]
                                   + [datetime(2024, 3, 1)]))  # fmt: skip
    assert got["months"] == {"2024-01": 3, "2024-03": 1} and got["method"] == "scan"


class FakeLegacy:
    """The legacy tables: forecasts partitioned (metadata), observations not (scan)."""

    def __init__(self):
        self.calls = []

    def partitioning(self, token, namespace, name, row_filter=None):
        self.calls.append(("partitioning", namespace, name))
        if name == "secondary_timeseries":
            return Partitioning(FIELDS, partitions([
                ("nwm30_medium_range", months_since_1970(2024, 1), 300),
                ("nwm30_medium_range", months_since_1970(2024, 2), 200),
            ]))  # fmt: skip
        return Partitioning((), None)

    def scan_batches(self, token, namespace, name, columns, row_filter=None):
        self.calls.append(("scan", namespace, name))
        if name == "primary_timeseries":  # the period's months (the filter's job)
            return batches(
                [datetime(2024, 1, 5), datetime(2024, 2, 5), datetime(2024, 2, 6)]
            )
        n = {"location_crosswalks": 4, "locations": 7}[name]
        table = pa.table({columns[0]: [f"k{i}" for i in range(n)]})
        return pa.RecordBatchReader.from_batches(table.schema, table.to_batches())


def test_count_a_configuration():
    fake = FakeLegacy()
    got = count(fake, "token", {"configuration": "nwm30_medium_range"})
    assert got["forecasts"]["rows"] == 500 and got["forecasts"]["method"] == "metadata"
    assert got["observations"]["months"] == {"2024-01": 1, "2024-02": 2}
    assert (got["crosswalk_rows"], got["gauges"]) == (4, 7)
    assert {ns for _, ns, _ in fake.calls} == {
        "teehr"
    }  # reads the legacy namespace only


def test_only_platform_admins_start_one(client, jobs, public):  # noqa: F811
    resp = client.post(URL, json={"configuration": "nwm30_medium_range"}, headers=ALICE)
    assert resp.status_code == 403
    assert client.get(URL, headers=ALICE).status_code == 403


def test_a_count_job_reads_the_catalog_as_the_admin(client, jobs, public, monkeypatch):  # noqa: F811
    kube, _ = jobs
    resp = client.post(URL, json={"configuration": "nwm30_medium_range"}, headers=ROOT)
    assert resp.status_code == 202, resp.text
    copy = resp.json()
    assert copy["status"] == "queued" and copy["request"]["mode"] == "count"
    assert kube.created[-1]["metadata"]["generateName"].startswith("legacy-copy-")
    container = kube.created[-1]["spec"]["template"]["spec"]["containers"][0]
    assert container["envFrom"] == [
        {"configMapRef": {"name": "teehr-control-plane-config"}}
    ]

    monkeypatch.setattr(legacy_copy, "catalog_tables", FakeLegacy)
    job = GrantClient("http://testserver", grant_of(kube.created[-1]), http=client)
    task = job.task()
    assert (
        task["kind"] == "legacy_copy"
        and task["copy"]["configuration"] == "nwm30_medium_range"
    )
    job.finish("succeeded", result=legacy_copy.run_legacy_copy(job, task))

    got = client.get(f"{URL}/{copy['id']}", headers=ROOT).json()
    assert got["status"] == "succeeded" and got["result"]["forecasts"]["rows"] == 500
    assert [c["id"] for c in client.get(URL, headers=ROOT).json()] == [copy["id"]]


@pytest.mark.parametrize(
    "bad", [{"configuration": "Bad-Name"}, {"configuration": "x", "mode": "copy"}]
)
def test_requests_are_checked(client, jobs, public, bad):  # noqa: F811
    assert client.post(URL, json=bad, headers=ROOT).status_code == 422
