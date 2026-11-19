"""Upserts commit once, matching on the key, without a clause per row."""

from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pytest

from src import tables as t

T0 = datetime(2024, 1, 1, tzinfo=UTC)
COLUMNS = [
    {"name": "series_key", "type": "string", "required": True},
    {"name": "value_time", "type": "timestamptz", "required": True},
    {"name": "value", "type": "float64"},
]
SCHEMA = pa.schema([pa.field("series_key", pa.string(), nullable=False),
                    pa.field("value_time", pa.timestamp("us", tz="UTC"), nullable=False),
                    pa.field("value", pa.float64())])  # fmt: skip


@pytest.fixture
def tables(tmp_path):
    tables = t.PolarisTables.at({
        "type": "sql", "uri": f"sqlite:///{tmp_path}/c.db",
        "warehouse": f"file://{tmp_path}/w",
    })  # fmt: skip
    tables._catalog("token").create_namespace("p")
    tables.create_table(
        "token", "p", "ts", COLUMNS, ["series_key", "value_time"],
        t.Layout(partition=("value_time", "month")),
    )  # fmt: skip
    return tables


def rows(*items):
    return pa.table(
        {"series_key": [k for k, _, _ in items],
         "value_time": [T0 + timedelta(days=d) for _, d, _ in items],
         "value": [v for _, _, v in items]},
        schema=SCHEMA,
    )  # fmt: skip


def stored(tables):
    table = tables._catalog("token").load_table(("p", "ts"))
    return sorted(
        (r["series_key"], (r["value_time"] - T0).days, r["value"])
        for r in table.scan().to_arrow().to_pylist()
    )


def test_upsert_replaces_matching_keys_keeps_the_rest_in_one_commit(tables):
    tables.write(
        "token",
        "p",
        "ts",
        rows(("a", 0, 1.0), ("a", 1, 2.0), ("a", 40, 3.0), ("b", 1, 4.0)),
        "upsert",
    )
    table = tables._catalog("token").load_table(("p", "ts"))
    before = len(table.metadata.snapshots)

    result = tables.write(
        "token", "p", "ts", rows(("a", 1, 20.0), ("a", 2, 5.0), ("c", 1, 6.0)), "upsert"
    )

    assert stored(tables) == [("a", 0, 1.0), ("a", 1, 20.0), ("a", 2, 5.0),
                              ("a", 40, 3.0), ("b", 1, 4.0), ("c", 1, 6.0)]  # fmt: skip
    table = table.refresh()
    assert table.current_snapshot().snapshot_id == result.snapshot_id
    # One commit (an overwrite is a delete and an append snapshot, at most).
    assert len(table.metadata.snapshots) - before <= 2


def test_key_range_is_small_for_many_rows():
    data = rows(*[(f"s{i % 50}", i % 30, 0.0) for i in range(5000)])
    where = repr(t.key_range(data, ["series_key", "value_time"]))
    assert (
        where.count("GreaterThanOrEqual") == 1 and where.count("LessThanOrEqual") == 1
    )
    assert len(where) < 5000


def test_upsert_on_a_number_key_reads_only_its_range(tables):
    columns = [{"name": "id", "type": "int64", "required": True},
               {"name": "v", "type": "string"}]  # fmt: skip
    tables.create_table("token", "p", "ids", columns, ["id"], t.Layout())
    schema = pa.schema([pa.field("id", pa.int64(), nullable=False), ("v", pa.string())])

    def batch(*pairs):
        return pa.table({"id": [i for i, _ in pairs], "v": [v for _, v in pairs]},
                        schema=schema)  # fmt: skip

    tables.write("token", "p", "ids", batch((1, "a"), (5, "b"), (9, "c")), "upsert")
    assert "GreaterThanOrEqual" in repr(t.key_range(batch((4, "x"), (6, "y")), ["id"]))
    tables.write("token", "p", "ids", batch((5, "B"), (6, "d")), "upsert")
    table = tables._catalog("token").load_table(("p", "ids"))
    got = sorted((r["id"], r["v"]) for r in table.scan().to_arrow().to_pylist())
    assert got == [(1, "a"), (5, "B"), (6, "d"), (9, "c")]
