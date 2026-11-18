"""Reads through the API (Phase 2.3): pages pinned to a snapshot, JSON by default,
Arrow / Parquet on request - the same contract as VeriRace's reads."""

import dataclasses
import io
import math
from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.config import get_settings
from src.main import app
from src.tables import ReadResult, SnapshotGone, TableForbidden
from tests.conftest import auth
from tests.test_datasets import ALICE, BOB, add_member, register  # noqa: F401
from tests.test_datasets import tables as tables  # noqa: F401  (fixture)

ARROW = "application/vnd.apache.arrow.stream"
PARQUET = "application/vnd.apache.parquet"
COLUMNS = [
    {"name": "location_id", "type": "string", "required": True},
    {"name": "value_time", "type": "timestamptz", "required": True},
    {"name": "value", "type": "float64"},
    {"name": "blob", "type": "binary"},
]
URL = "/v1/projects/flood/datasets/obs/data"


def make_rows(n, start=0):
    base = datetime(2026, 1, 1, tzinfo=UTC)
    return pa.table(
        {
            "location_id": [f"g{i}" for i in range(start, start + n)],
            "value_time": pa.array(
                [base + timedelta(hours=i) for i in range(start, start + n)],
                pa.timestamp("us", tz="UTC"),
            ),
            "value": [float(i) for i in range(start, start + n)],
            "blob": [b"\x00\x01"] * n,
        }
    )


class Snapshots:
    """Fake table storage with one table per snapshot, like Iceberg."""

    def __init__(self, fake):
        self.fake = fake
        self.by_snapshot: dict[int, pa.Table] = {}
        self.current: int | None = None
        self.reads: list[dict] = []
        self.fail_with: Exception | None = None
        fake.read = self.read
        fake.count = self.count

    def commit(self, table):
        snapshot = (self.current or 100) + 1
        self.by_snapshot[snapshot] = table
        self.current = snapshot
        return snapshot

    def read(self, token, ns, name, *, columns, row_filter, snapshot_id, limit):
        self.reads.append(
            {
                "columns": columns,
                "filter": row_filter,
                "snapshot": snapshot_id,
                "limit": limit,
            }
        )
        if self.fail_with:
            raise self.fail_with
        snap = snapshot_id if snapshot_id is not None else self.current
        if snap is None:
            return ReadResult(
                pa.schema([("location_id", pa.string())]).empty_table(), None
            )
        if snap not in self.by_snapshot:
            raise SnapshotGone("expired")
        data = self.by_snapshot[snap]
        if columns:
            data = data.select(list(columns))
        return ReadResult(data.slice(0, limit), snap)

    def count(self, token, ns, name, row_filter, snapshot_id):
        return self.by_snapshot[snapshot_id].num_rows


@pytest.fixture
def lake(client, tables):  # noqa: F811
    assert register(client, columns=COLUMNS).status_code == 201
    return Snapshots(tables)


def read(client, headers=ALICE, accept=None, **params):
    h = {**headers, **({"Accept": accept} if accept else {})}
    return client.get(URL, headers=h, params=params)


def test_json_pages_follow_the_cursor_to_the_end(client, lake):
    lake.commit(make_rows(25))
    seen, cursor, pages = [], None, 0
    while True:
        resp = read(client, limit=10, **({"cursor": cursor} if cursor else {}))
        assert resp.status_code == 200
        body = resp.json()
        seen += [r["location_id"] for r in body["rows"]]
        pages += 1
        cursor = body["next_cursor"]
        if cursor is None:
            break
    assert pages == 3
    assert seen == [f"g{i}" for i in range(25)]
    # Each page asked for one row more than it returns, to know if there's more.
    assert [r["limit"] for r in lake.reads] == [11, 21, 31]


def test_cursor_pins_the_snapshot_while_writes_land(client, lake):
    first = lake.commit(make_rows(4))
    resp = read(client, limit=2)
    assert resp.headers["x-snapshot-id"] == str(first)
    lake.commit(make_rows(100, start=1000))  # a write lands mid-read
    page2 = read(client, limit=2, cursor=resp.json()["next_cursor"])
    assert [r["location_id"] for r in page2.json()["rows"]] == ["g2", "g3"]
    assert page2.headers["x-snapshot-id"] == str(first)
    assert page2.json()["next_cursor"] is None


def test_json_values(client, lake):
    lake.commit(make_rows(1).set_column(2, "value", pa.array([math.nan])))
    row = read(client).json()["rows"][0]
    assert row["value_time"] == "2026-01-01T00:00:00+00:00"
    assert row["value"] is None  # NaN isn't JSON
    assert row["blob"] == "AAE="  # base64


@pytest.mark.parametrize("accept", [ARROW, PARQUET])
def test_binary_formats_put_paging_in_headers(client, lake, accept):
    snap = lake.commit(make_rows(5))
    resp = read(client, accept=accept, limit=2, count="true")
    assert resp.status_code == 200
    assert resp.headers["content-type"] == accept
    assert resp.headers["x-snapshot-id"] == str(snap)
    assert resp.headers["x-total-rows"] == "5"
    assert "x-next-cursor" in resp.headers
    if accept == ARROW:
        table = pa.ipc.open_stream(pa.BufferReader(resp.content)).read_all()
    else:
        table = pq.read_table(io.BytesIO(resp.content))
    assert table.column("location_id").to_pylist() == ["g0", "g1"]
    assert str(table.schema.field("value_time").type) == "timestamp[us, tz=UTC]"


def test_columns_and_filter_reach_the_scan(client, lake):
    lake.commit(make_rows(3))
    resp = read(client, columns="value, location_id", filter="location_id = 'g1'")
    assert resp.status_code == 200
    assert list(resp.json()["rows"][0]) == ["value", "location_id"]
    assert lake.reads[-1]["columns"] == ("value", "location_id")
    assert "g1" in repr(lake.reads[-1]["filter"])


def test_count_in_json(client, lake):
    lake.commit(make_rows(7))
    assert read(client, count="true", limit=2).json()["total"] == 7
    assert "total" not in read(client, limit=2).json()


def test_empty_dataset(client, lake):
    resp = read(client)
    assert resp.status_code == 200
    assert resp.json() == {"rows": [], "next_cursor": None}
    assert "x-snapshot-id" not in resp.headers


def test_page_caps_differ_by_format(client, lake):
    small = dataclasses.replace(
        app.dependency_overrides[get_settings](),
        max_page_rows_json=10,
        max_page_rows_binary=100,
    )
    app.dependency_overrides[get_settings] = lambda: small
    lake.commit(make_rows(1))
    assert read(client, limit=11).status_code == 400
    assert read(client, limit=11, accept=ARROW).status_code == 200


@pytest.mark.parametrize("cursor", ["garbage", "eyJzIjoxfQ"])
def test_bad_cursor_is_400(client, lake, cursor):
    lake.commit(make_rows(3))
    assert read(client, cursor=cursor).status_code == 400


def test_cursor_from_another_query_is_400(client, lake):
    lake.commit(make_rows(5))
    cursor = read(client, limit=2).json()["next_cursor"]
    resp = read(client, limit=2, cursor=cursor, filter="location_id = 'g1'")
    assert resp.status_code == 400
    assert "different filter or columns" in resp.json()["detail"]


def test_expired_snapshot_is_410(client, lake):
    snap = lake.commit(make_rows(5))
    cursor = read(client, limit=2).json()["next_cursor"]
    del lake.by_snapshot[snap]  # expired by maintenance
    assert read(client, limit=2, cursor=cursor).status_code == 410


@pytest.mark.parametrize(
    "params, status",
    [({"columns": "nope"}, 422), ({"filter": "nope = 1"}, 422), ({"limit": 0}, 422)],
)
def test_bad_parameters(client, lake, params, status):
    assert read(client, **params).status_code == status


def test_readers_read_non_members_dont(client, lake):
    lake.commit(make_rows(2))
    add_member(client, "bob", "read")
    assert read(client, headers=BOB).status_code == 200
    assert read(client, headers=auth("carol")).status_code == 404


def test_stale_token_gets_the_refresh_code(client, lake):
    lake.fail_with = TableForbidden("no activated role")
    resp = read(client)
    assert resp.status_code == 403
    assert resp.json()["code"] == "token_refresh_required"


def test_read_stats_count_files_once_and_every_read_of_them():
    from types import SimpleNamespace

    from src.jobs.reads import CatalogAccess, Warehouse

    def task(path, size):
        return SimpleNamespace(
            file=SimpleNamespace(file_path=path, file_size_in_bytes=size)
        )

    wh = Warehouse(
        CatalogAccess("http://polaris/api/catalog", "w", "teehr"), lambda: "t"
    )
    try:
        wh._tally("p.fcst", [task("s3://a", 10), task("s3://b", 20)])  # shard 1
        wh._tally("p.fcst", [task("s3://b", 20), task("s3://c", 5)])  # shard 2
        wh._tally("p.obs", [])
        assert wh.read_stats() == {
            "p.fcst": {"reads": 2, "files": 3, "file_reads": 4, "bytes_read": 55},
            "p.obs": {"reads": 1, "files": 0, "file_reads": 0, "bytes_read": 0},
        }
    finally:
        wh.close()
