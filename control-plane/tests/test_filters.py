"""/data filters in the shared expression language (#196, spec/expressions.md):
allow-list checks (teehr-core's), the split into scan pushdown and the rest, and
reads with it."""

from datetime import UTC, datetime

import pyarrow as pa
import pytest
from pyiceberg.expressions.visitors import bind
from pyiceberg.io.pyarrow import expression_to_pyarrow

from src.expressions import compile_filter
from src.tables import DataMismatch, ReadResult, iceberg_schema
from tests.test_datasets import ALICE
from tests.test_datasets import tables as tables  # noqa: F401  (fixture)
from tests.test_reads import COLUMNS, URL, make_rows, read
from tests.test_reads import lake as lake  # noqa: F401  (fixture)

SERIES = [
    {"name": "series_key", "type": "string", "required": True},
    {"name": "value_time", "type": "timestamptz", "required": True},
    {"name": "value", "type": "float64"},
    {"name": "quality", "type": "string"},
]


@pytest.mark.parametrize(
    ("text", "pushed", "rest"),
    [
        ("series_key = 'a'", True, False),
        ("value_time >= '2026-01-01T00:00:00Z'", True, False),  # Z binds as +00:00
        ("series_key IN ('a', 'b') AND value BETWEEN 1 AND 2", True, False),
        ("quality IS NOT NULL OR value > 3", True, False),
        ("hour(value_time) = 0", False, True),
        ("series_key = 'a' AND value * 2 > 3", True, True),
        ("series_key = 'a' OR hour(value_time) = 0", False, True),
        ("value > -1", True, False),
    ],
)
def test_filters_split_into_pushdown_and_the_rest(text, pushed, rest):
    compiled = compile_filter(text, SERIES)
    assert (compiled.pushdown is not None) == pushed
    assert (compiled.residual is not None) == rest


@pytest.mark.parametrize(
    ("text", "why"),
    [
        ("log(value) > 1", "use ln, log10"),
        ("nope > 1", "unknown column 'nope'"),
        ("t.value > 1", "unknown column"),
        ("value > (SELECT 1)", "'subquery' is not allowed"),
        ("dayofweek(value_time) = 1", "engines number weekdays differently"),
        ("series_key = 'a'; DROP TABLE x", "is not allowed"),
        ("cast(value AS json) = 1", "'CAST to JSON' is not allowed"),
        ("value >", "invalid filter"),
    ],
)
def test_off_list_filters_are_refused(text, why):
    with pytest.raises(DataMismatch, match=why):
        compile_filter(text, SERIES)


def table():
    times = [datetime(2026, 1, 1, h, tzinfo=UTC) for h in (0, 1, 2)]
    return pa.table(
        {
            "series_key": ["a", "b", "a"],
            "value_time": pa.array(times, pa.timestamp("us", tz="UTC")),
            "value": [1.0, 2.0, None],
            "quality": ["P", None, "A"],
        }
    )


@pytest.mark.parametrize(
    ("text", "keys"),
    [
        ("hour(value_time) = 1", ["b"]),
        ("value * 2 > 3", ["b"]),
        ("coalesce(value, 0) = 0", ["a"]),
        ("CASE WHEN value > 1 THEN 1 ELSE 0 END = 1", ["b"]),
        ("ln(value) >= 0 AND log10(value) < 1", ["a", "b"]),
        ("lower(quality) LIKE 'p%'", ["a"]),
        ("series_key || coalesce(quality, '') = 'aA'", ["a"]),
        ("extract(hour FROM value_time) = 2", ["a"]),
        ("date_trunc('hour', value_time) = '2026-01-01T01:00:00Z'", ["b"]),
        (
            "value_time BETWEEN '2026-01-01T00:30:00Z' AND '2026-01-01T03:00:00Z' "
            "AND minute(value_time) = 0",
            ["b", "a"],
        ),  # fmt: skip
        ("abs(value - 2) < 0.5 OR quality = 'A'", ["b", "a"]),
        ("CAST(value > 1 AS BOOLEAN)", ["b"]),
        ("value_time + INTERVAL '1' HOUR = '2026-01-01T02:00:00Z'", ["b"]),
        ("value_time - INTERVAL '90' MINUTE >= '2026-01-01T00:00:00Z'", ["a"]),
    ],
)
def test_the_rest_is_evaluated_on_the_rows(text, keys):
    compiled = compile_filter(text, SERIES)
    rows = table()
    if compiled.pushdown is not None:  # what the scan would do
        bound = bind(iceberg_schema(SERIES), compiled.pushdown, case_sensitive=True)
        rows = rows.filter(expression_to_pyarrow(bound))
    assert compiled.apply(rows)["series_key"].to_pylist() == keys


# --- through the API ------------------------------------------------------------


@pytest.fixture
def scans(lake):  # noqa: F811
    """The fake scan applies the pushed-down filter, as Iceberg would."""
    original = lake.read

    def read_(token, ns, name, *, columns, row_filter, snapshot_id, limit):
        result = original(token, ns, name, columns=None, row_filter=None,
                          snapshot_id=snapshot_id, limit=None)  # fmt: skip
        rows = result.rows
        if row_filter is not None:
            bound = bind(iceberg_schema(COLUMNS), row_filter, case_sensitive=True)
            rows = rows.filter(expression_to_pyarrow(bound))
        if columns:
            rows = rows.select(list(columns))
        lake.last = {"columns": columns, "limit": limit, "filter": row_filter}
        return ReadResult(rows.slice(0, limit), result.snapshot_id)

    lake.fake.read = read_
    return lake


def test_reads_take_functions_and_page_through_them(client, scans):
    scans.commit(make_rows(48))  # g0..g47, hourly from 2026-01-01T00
    keys, cursor = [], None
    while True:
        params = {"filter": "hour(value_time) % 6 = 0", "limit": 3, "count": "true"}
        resp = read(client, **params, **({"cursor": cursor} if cursor else {}))
        assert resp.status_code == 200, resp.text
        body = resp.json()
        keys += [r["location_id"] for r in body["rows"]]
        assert body["total"] == 8
        cursor = body["next_cursor"]
        if cursor is None:
            break
    assert keys == [f"g{i}" for i in range(0, 48, 6)]
    # The rest needed every row the scan matched.
    assert scans.last["limit"] is None


def test_pushdown_alone_keeps_the_page_limit(client, scans):
    scans.commit(make_rows(10))
    resp = read(client, filter="location_id IN ('g1', 'g2', 'g3')", limit=2)
    assert [r["location_id"] for r in resp.json()["rows"]] == ["g1", "g2"]
    assert scans.last["limit"] == 3 and scans.last["filter"] is not None


def test_the_rest_can_use_columns_not_returned(client, scans):
    scans.commit(make_rows(6))
    resp = read(client, filter="value * 10 > 35", columns="location_id")
    assert resp.status_code == 200, resp.text
    assert resp.json()["rows"] == [{"location_id": "g4"}, {"location_id": "g5"}]
    assert set(scans.last["columns"]) == {"location_id", "value"}


def test_bad_filters_are_422(client, scans):
    scans.commit(make_rows(2))
    for text in ("log(value) > 1", "nope = 1", "value + location_id > 1"):
        resp = read(client, filter=text)
        assert resp.status_code == 422, (text, resp.text)


def test_overwrite_filters_must_be_pushdown(client, scans):
    import io

    import pyarrow.parquet as pq

    sink = io.BytesIO()
    pq.write_table(make_rows(1), sink)
    headers = {**ALICE, "Content-Type": "application/vnd.apache.parquet"}
    url = URL
    resp = client.post(url, content=sink.getvalue(), headers=headers,
                       params={"mode": "overwrite",
                               "filter": "hour(value_time) = 0"})  # fmt: skip
    assert resp.status_code == 422 and "overwrite filter" in resp.text
    resp = client.post(url, content=sink.getvalue(), headers=headers,
                       params={"mode": "overwrite",
                               "filter": "location_id = 'g0'"})  # fmt: skip
    assert resp.status_code == 201, resp.text
