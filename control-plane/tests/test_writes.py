"""Writes through the API (Phase 2.2): Arrow/Parquet in, checked against the
declared columns, committed as the caller, with lineage."""

import dataclasses
import io
from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.config import get_settings
from src.main import app
from src.tables import TableForbidden, TableMissing, WriteConflict
from tests.conftest import auth
from tests.test_datasets import ALICE, BOB, add_member, register, tables  # noqa: F401

ARROW = "application/vnd.apache.arrow.stream"
PARQUET = "application/vnd.apache.parquet"
KEYED = [
    {"name": "location_id", "type": "string", "required": True},
    {"name": "value_time", "type": "timestamptz", "required": True},
    {"name": "value", "type": "float64"},
]
URL = "/v1/projects/flood/datasets/obs/data"


def rows(n=3):
    return pa.table(
        {
            "location_id": [f"g{i}" for i in range(n)],
            "value_time": pa.array(
                [
                    datetime(2026, 1, 1, tzinfo=UTC) + timedelta(hours=i)
                    for i in range(n)
                ],
                pa.timestamp("us", tz="UTC"),
            ),
            "value": [float(i) for i in range(n)],
        }
    )


def arrow_bytes(table):
    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue()


def parquet_bytes(table):
    sink = io.BytesIO()
    pq.write_table(table, sink)
    return sink.getvalue()


@pytest.fixture
def obs(client, tables):  # noqa: F811
    assert register(client, columns=KEYED).status_code == 201
    return tables


def write(client, body, media=ARROW, headers=ALICE, **params):
    return client.post(
        URL, content=body, headers={**headers, "Content-Type": media}, params=params
    )


def test_append_arrow_records_lineage(client, obs):
    resp = write(client, arrow_bytes(rows(3)))
    assert resp.status_code == 201
    body = resp.json()
    assert body["mode"] == "append" and body["rows"] == 3
    assert body["snapshot_id"] == "9000000000000000001"  # string: 64-bit
    assert body["written_by"] == "alice"
    mode, flt, data = obs.writes[-1]
    assert (mode, flt) == ("append", None)
    assert data.schema.field("location_id").nullable is False
    assert obs.calls[-1][1] == ALICE["Authorization"].removeprefix("Bearer ")


def test_parquet_is_accepted(client, obs):
    assert write(client, parquet_bytes(rows(2)), PARQUET).status_code == 201


def test_overwrite_whole_and_filtered(client, obs):
    assert write(client, arrow_bytes(rows()), mode="overwrite").status_code == 201
    assert obs.writes[-1][1] is None
    resp = write(
        client, arrow_bytes(rows(1)), mode="overwrite", filter="location_id = 'g0'"
    )
    assert resp.status_code == 201
    assert resp.json()["filter"] == "location_id = 'g0'"
    assert "location_id" in repr(obs.writes[-1][1])


def test_writes_are_listed_newest_first_and_readers_can_see_them(client, obs):
    write(client, arrow_bytes(rows(1)))
    write(client, arrow_bytes(rows(2)), mode="overwrite")
    add_member(client, "bob", "read")
    listed = client.get("/v1/projects/flood/datasets/obs/writes", headers=BOB).json()
    assert [w["rows"] for w in listed] == [2, 1]


@pytest.mark.parametrize(
    "params, why",
    [
        ({"filter": "location_id = 'g0'"}, "only applies to mode=overwrite"),
        ({"mode": "overwrite", "filter": "nope = 1"}, "invalid filter"),
        ({"mode": "upsert"}, ""),
    ],
)
def test_bad_parameters_are_422(client, obs, params, why):
    resp = write(client, arrow_bytes(rows()), **params)
    assert resp.status_code == 422 and why in resp.text
    assert not getattr(obs, "writes", [])


def test_data_that_doesnt_fit_is_422(client, obs):
    bad = rows().drop_columns(["location_id"])
    resp = write(client, arrow_bytes(bad))
    assert resp.status_code == 422
    assert "missing required column 'location_id'" in resp.json()["detail"]


def test_empty_and_unreadable_bodies(client, obs):
    assert write(client, arrow_bytes(rows(0))).status_code == 422
    assert write(client, b"not arrow").status_code == 422
    assert write(client, b"x", "text/plain").status_code == 415


def test_oversized_body_is_413(client, obs):
    small = dataclasses.replace(
        app.dependency_overrides[get_settings](), max_write_bytes=100
    )
    app.dependency_overrides[get_settings] = lambda: small
    resp = write(client, arrow_bytes(rows(50)))
    assert resp.status_code == 413
    assert "staged upload" in resp.json()["detail"]


def test_reader_and_non_member(client, obs):
    add_member(client, "bob", "read")
    assert write(client, arrow_bytes(rows()), headers=BOB).status_code == 403
    assert write(client, arrow_bytes(rows()), headers=auth("carol")).status_code == 404


def test_unknown_dataset_is_404(client, obs):
    resp = client.post(
        "/v1/projects/flood/datasets/nope/data",
        content=arrow_bytes(rows()),
        headers={**ALICE, "Content-Type": ARROW},
    )
    assert resp.status_code == 404


@pytest.mark.parametrize(
    "error, status_code, code",
    [
        (TableForbidden("x"), 403, "token_refresh_required"),
        (TableMissing("x"), 409, None),
        (WriteConflict("x"), 409, None),
    ],
)
def test_catalog_failures_record_nothing(client, obs, error, status_code, code):
    obs.fail_with = error
    resp = write(client, arrow_bytes(rows()))
    assert resp.status_code == status_code
    assert resp.json().get("code") == code
    obs.fail_with = None
    assert (
        client.get("/v1/projects/flood/datasets/obs/writes", headers=ALICE).json() == []
    )


def written(obs):
    return obs.writes[-1][2]


def test_ndjson_and_json_array(client, obs):
    lines = (
        '{"location_id": "a", "value_time": "2026-01-01T00:00:00Z", "value": 1}\n'
        '{"location_id": "b", "value_time": "2026-01-01T01:00:00Z"}\n'
    )
    assert write(client, lines.encode(), "application/x-ndjson").status_code == 201
    assert written(obs).column("value").to_pylist() == [1.0, None]
    array = (
        '[{"location_id": "c", "value_time": "2026-01-01T00:00:00+01:00", '
        '"value": 2.5}]'
    )
    resp = write(client, array.encode(), "application/json")
    assert resp.status_code == 201
    assert [str(v) for v in written(obs).column("value_time").to_pylist()] == [
        "2025-12-31 23:00:00+00:00"
    ]


@pytest.mark.parametrize(
    "media, body, why",
    [
        # A timestamptz value without an offset is ambiguous: refused.
        (
            "application/x-ndjson",
            '{"location_id": "a", "value_time": "2026-01-01T00:00:00"}\n',
            "offset",
        ),
        (
            "application/json",
            '[{"location_id": "a", "value_time": "2026-01-01T00:00:00Z", "x": 1}]',
            "unexpected field",
        ),
        ("application/json", '{"location_id": "a"}', "array of objects"),
        ("application/json", "[]", "No rows"),
    ],
)
def test_text_formats_are_strict(client, obs, media, body, why):
    resp = write(client, body.encode(), media)
    assert resp.status_code == 422, resp.text
    assert why in resp.text
    assert not getattr(obs, "writes", [])


def test_json_refused_for_binary_columns(client, tables):  # noqa: F811
    cols = [
        {"name": "k", "type": "string", "required": True},
        {"name": "blob", "type": "binary"},
    ]
    register(client, "blobs", columns=cols)
    resp = client.post(
        "/v1/projects/flood/datasets/blobs/data",
        content=b'[{"k": "a"}]',
        headers={**ALICE, "Content-Type": "application/json"},
    )
    assert resp.status_code == 422 and "binary" in resp.text


def test_csv_is_refused(client, obs):
    body = b"location_id,value_time\na,2026-01-01T00:00:00Z\n"
    resp = write(client, body, "text/csv")
    assert resp.status_code == 415


def naive_rows(times, unit="us"):
    return pa.table(
        {
            "location_id": ["a"] * len(times),
            "value_time": pa.array(times, pa.timestamp(unit)),
        }
    )


def test_timezone_reads_zone_less_arrow_times_and_stores_utc(client, obs):
    body = arrow_bytes(naive_rows([datetime(2026, 1, 1, 0, 0)]))
    assert write(client, body).status_code == 422  # still refused without it
    resp = write(client, body, timezone="America/New_York")
    assert resp.status_code == 201
    assert [str(v) for v in written(obs).column("value_time").to_pylist()] == [
        "2026-01-01 05:00:00+00:00"
    ]


def test_timezone_with_json_keeps_explicit_offsets(client, obs):
    lines = (
        '{"location_id": "a", "value_time": "2026-07-01T00:00:00"}\n'
        '{"location_id": "b", "value_time": "2026-07-01T00:00:00Z"}\n'
    )
    resp = write(
        client, lines.encode(), "application/x-ndjson", timezone="America/Denver"
    )
    assert resp.status_code == 201
    assert [str(v) for v in written(obs).column("value_time").to_pylist()] == [
        "2026-07-01 06:00:00+00:00",  # Denver summer time, UTC-6
        "2026-07-01 00:00:00+00:00",  # had its own offset
    ]


@pytest.mark.parametrize(
    "local",
    ["2026-03-08T02:30:00", "2026-11-01T01:30:00"],  # doesn't exist; happens twice
)
def test_timezone_refuses_daylight_saving_gaps_and_repeats(client, obs, local):
    line = f'{{"location_id": "a", "value_time": "{local}"}}\n'
    resp = write(
        client, line.encode(), "application/x-ndjson", timezone="America/New_York"
    )
    assert resp.status_code == 422
    assert "daylight-saving" in resp.json()["detail"]


def test_unknown_timezone_is_422(client, obs):
    resp = write(client, arrow_bytes(rows()), timezone="Mars/Olympus")
    assert resp.status_code == 422 and "unknown time zone" in resp.json()["detail"]


def test_a_token_function_is_asked_on_every_catalog_request(monkeypatch):
    """A job's write gives the catalog a function, not a token: a commit after a long
    write asks it again rather than reusing one that has since expired (#228)."""
    import requests
    from pyiceberg.catalog.rest import RestCatalog

    from src.tables import PolarisTables, catalog_properties

    monkeypatch.setattr(RestCatalog, "_fetch_config", lambda self: None)
    issued = iter(["first", "second"])
    polaris = PolarisTables.at(
        catalog_properties("http://polaris.test/api/catalog", "wh", "realm")
    )
    catalog = polaris._catalog(lambda: next(issued))
    request = requests.Request("GET", "http://polaris.test/v1/config").prepare()
    for expected in ("Bearer first", "Bearer second"):
        assert catalog._session.auth(request).headers["Authorization"] == expected
