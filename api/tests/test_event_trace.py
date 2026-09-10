from datetime import datetime

import pandas as pd
import pytest

from src import database


class _FakeCursor:
    def __init__(self):
        self.description = [("a",), ("b",)]
        self.query = None
        self.params = None

    def execute(self, query, params):
        self.query = query
        self.params = params

    def fetchall(self):
        return [(1, "x"), (2, "y")]


class _FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_validate_event_id_valid():
    start, end = database.validate_event_id("2018-07-06 00:00:00-2018-07-09 05:00:00")

    assert start == datetime(2018, 7, 6, 0, 0, 0)
    assert end == datetime(2018, 7, 9, 5, 0, 0)


def test_validate_event_id_invalid_format():
    with pytest.raises(ValueError, match="Invalid event ID format"):
        database.validate_event_id("2018-07-06T00:00:00/2018-07-09T05:00:00")


def test_validate_event_id_end_before_start():
    with pytest.raises(ValueError, match="end time must be after start time"):
        database.validate_event_id("2018-07-09 05:00:00-2018-07-06 00:00:00")


@pytest.mark.parametrize(
    "token, expected",
    [
        ("10th", "10th"),
        ("q_10th", "10th"),
        ("p10", "10th"),
        ("0.1", "10th"),
        ("25th", "25th"),
        ("q_50th", "50th"),
        ("p75", "75th"),
        ("0.90", "90th"),
    ],
)
def test_normalize_threshold_token(token, expected):
    assert database.normalize_threshold_token(token) == expected


def test_normalize_threshold_token_invalid():
    with pytest.raises(ValueError, match="Unsupported threshold token"):
        database.normalize_threshold_token("q_33rd")


def test_execute_query_params_passes_parameters(monkeypatch):
    fake_cursor = _FakeCursor()
    fake_conn = _FakeConn(fake_cursor)

    monkeypatch.setattr(database, "get_trino_connection", lambda: fake_conn)

    result = database.execute_query_params(
        "SELECT a, b FROM table WHERE id = ?",
        params=["abc"],
    )

    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["a", "b"]
    assert result.to_dict(orient="records") == [
        {"a": 1, "b": "x"},
        {"a": 2, "b": "y"},
    ]
    assert fake_cursor.query == "SELECT a, b FROM table WHERE id = ?"
    assert fake_cursor.params == ["abc"]
