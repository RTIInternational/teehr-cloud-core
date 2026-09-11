import sys
import types
from datetime import datetime

import pandas as pd
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src import database

# The test environment may not have geospatial dependencies installed.
sys.modules.setdefault("geopandas", types.ModuleType("geopandas"))

from src.routes import events


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


def _build_test_client() -> TestClient:
    app = FastAPI()
    app.include_router(events.router)
    return TestClient(app)


def test_event_trace_initializations_default_is_last_prior_to_event_start(monkeypatch):
    rows = [
        {
            "reference_time": datetime(2018, 7, 6, 0, 0, 0),
            "event_start": datetime(2018, 7, 6, 0, 0, 0),
            "event_end": datetime(2018, 7, 9, 0, 0, 0),
        },
        {
            "reference_time": datetime(2018, 7, 7, 0, 0, 0),
            "event_start": datetime(2018, 7, 6, 0, 0, 0),
            "event_end": datetime(2018, 7, 9, 0, 0, 0),
        },
        {
            "reference_time": datetime(2018, 7, 8, 0, 0, 0),
            "event_start": datetime(2018, 7, 6, 0, 0, 0),
            "event_end": datetime(2018, 7, 9, 0, 0, 0),
        },
    ]

    captured = {}

    def fake_execute_query_params(query, params=None, max_rows=None, retry_count=0):
        captured["query"] = query
        captured["params"] = params
        return pd.DataFrame(rows)

    monkeypatch.setattr(events, "execute_query_params", fake_execute_query_params)

    client = _build_test_client()
    response = client.get(
        "/collections/joined_timeseries/event_trace/initializations",
        params={
            "primary_location_id": "usgs-12345",
            "configuration_name": "hefs_streamflow_forecast",
            "variable_name": "streamflow_hourly_inst",
            "threshold": "q_10th",
            "event_id": "2018-07-06 00:00:00-2018-07-09 00:00:00",
            "lead_time_hours": 24,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["threshold"] == "10th"
    assert payload["lead_time_hours"] == 24
    assert payload["expanded_event_start"] == "2018-07-05T00:00:00"
    assert payload["expanded_event_end"] == "2018-07-10T00:00:00"
    assert payload["available_initialization_datetimes"] == [
        "2018-07-06T00:00:00",
        "2018-07-07T00:00:00",
        "2018-07-08T00:00:00",
    ]
    # Default is the last forecast at or before event start.
    assert payload["default_initialization_datetime"] == "2018-07-06T00:00:00"

    assert "joined_timeseries" in captured["query"]
    assert captured["params"] == [
        "usgs-12345",
        "hefs_streamflow_forecast",
        "streamflow_hourly_inst",
        "2018-07-06 00:00:00-2018-07-09 00:00:00",
    ]


def test_event_trace_initializations_not_found(monkeypatch):
    def fake_execute_query_params(query, params=None, max_rows=None, retry_count=0):
        return pd.DataFrame(columns=["reference_time", "event_start", "event_end"])

    monkeypatch.setattr(events, "execute_query_params", fake_execute_query_params)

    client = _build_test_client()
    response = client.get(
        "/collections/joined_timeseries/event_trace/initializations",
        params={
            "primary_location_id": "usgs-12345",
            "configuration_name": "hefs_streamflow_forecast",
            "variable_name": "streamflow_hourly_inst",
            "threshold": "10th",
            "event_id": "2018-07-06 00:00:00-2018-07-09 00:00:00",
            "lead_time_hours": 48,
        },
    )

    assert response.status_code == 404


def test_event_trace_initializations_bad_threshold():
    client = _build_test_client()
    response = client.get(
        "/collections/joined_timeseries/event_trace/initializations",
        params={
            "primary_location_id": "usgs-12345",
            "configuration_name": "hefs_streamflow_forecast",
            "variable_name": "streamflow_hourly_inst",
            "threshold": "q_33rd",
            "event_id": "2018-07-06 00:00:00-2018-07-09 00:00:00",
            "lead_time_hours": 24,
        },
    )

    assert response.status_code == 400
    assert "Unsupported threshold token" in response.json()["detail"]


def test_event_trace_initializations_filters_reference_time_to_expanded_window(
    monkeypatch,
):
    rows = [
        {
            "reference_time": datetime(2018, 7, 3, 0, 0, 0),
            "event_start": datetime(2018, 7, 6, 0, 0, 0),
            "event_end": datetime(2018, 7, 9, 0, 0, 0),
        },
        {
            "reference_time": datetime(2018, 7, 5, 0, 0, 0),
            "event_start": datetime(2018, 7, 6, 0, 0, 0),
            "event_end": datetime(2018, 7, 9, 0, 0, 0),
        },
        {
            "reference_time": datetime(2018, 7, 6, 0, 0, 0),
            "event_start": datetime(2018, 7, 6, 0, 0, 0),
            "event_end": datetime(2018, 7, 9, 0, 0, 0),
        },
        {
            "reference_time": datetime(2018, 7, 11, 0, 0, 0),
            "event_start": datetime(2018, 7, 6, 0, 0, 0),
            "event_end": datetime(2018, 7, 9, 0, 0, 0),
        },
    ]

    def fake_execute_query_params(query, params=None, max_rows=None, retry_count=0):
        return pd.DataFrame(rows)

    monkeypatch.setattr(events, "execute_query_params", fake_execute_query_params)

    client = _build_test_client()
    response = client.get(
        "/collections/joined_timeseries/event_trace/initializations",
        params={
            "primary_location_id": "usgs-12345",
            "configuration_name": "hefs_streamflow_forecast",
            "variable_name": "streamflow_hourly_inst",
            "threshold": "10th",
            "event_id": "2018-07-06 00:00:00-2018-07-09 00:00:00",
            "lead_time_hours": 24,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    # Expanded window: start - 24 hrs (lead_time) = 2018-07-05, end + 24 hrs (hard-coded tail) = 2018-07-10
    # So 07-03 and 07-11 are excluded.
    assert payload["available_initialization_datetimes"] == [
        "2018-07-05T00:00:00",
        "2018-07-06T00:00:00",
    ]
    # Event start is 2018-07-06, so choose the latest <= event start.
    assert payload["default_initialization_datetime"] == "2018-07-06T00:00:00"


def test_event_trace_initializations_tail_is_hard_coded_24_hours(monkeypatch):
    """Test that event end is extended by hard-coded 24 hours, not by lead_time_hours."""
    rows = [
        {
            "reference_time": datetime(2018, 7, 4, 0, 0, 0),
            "event_start": datetime(2018, 7, 6, 0, 0, 0),
            "event_end": datetime(2018, 7, 9, 0, 0, 0),
        },
        {
            "reference_time": datetime(2018, 7, 10, 0, 0, 0),
            "event_start": datetime(2018, 7, 6, 0, 0, 0),
            "event_end": datetime(2018, 7, 9, 0, 0, 0),
        },
        {
            "reference_time": datetime(2018, 7, 11, 0, 0, 0),
            "event_start": datetime(2018, 7, 6, 0, 0, 0),
            "event_end": datetime(2018, 7, 9, 0, 0, 0),
        },
    ]

    def fake_execute_query_params(query, params=None, max_rows=None, retry_count=0):
        return pd.DataFrame(rows)

    monkeypatch.setattr(events, "execute_query_params", fake_execute_query_params)

    client = _build_test_client()
    response = client.get(
        "/collections/joined_timeseries/event_trace/initializations",
        params={
            "primary_location_id": "usgs-12345",
            "configuration_name": "hefs_streamflow_forecast",
            "variable_name": "streamflow_hourly_inst",
            "threshold": "10th",
            "event_id": "2018-07-06 00:00:00-2018-07-09 00:00:00",
            "lead_time_hours": 72,  # 3 days, but tail is still 24 hours hard-coded
        },
    )

    assert response.status_code == 200
    payload = response.json()
    # Expanded window: start - 72 hrs = 2018-07-03, end + 24 hrs (not 72) = 2018-07-10
    assert payload["expanded_event_start"] == "2018-07-03T00:00:00"
    assert payload["expanded_event_end"] == "2018-07-10T00:00:00"
    # 2018-07-04 is within window, 07-11 is outside (beyond end + 24 hrs)
    assert payload["available_initialization_datetimes"] == [
        "2018-07-04T00:00:00",
        "2018-07-10T00:00:00",
    ]


def test_event_trace_initializations_default_falls_back_when_no_prior_exists(
    monkeypatch,
):
    rows = [
        {
            "reference_time": datetime(2018, 7, 7, 0, 0, 0),
            "event_start": datetime(2018, 7, 6, 0, 0, 0),
            "event_end": datetime(2018, 7, 9, 0, 0, 0),
        },
        {
            "reference_time": datetime(2018, 7, 8, 0, 0, 0),
            "event_start": datetime(2018, 7, 6, 0, 0, 0),
            "event_end": datetime(2018, 7, 9, 0, 0, 0),
        },
    ]

    def fake_execute_query_params(query, params=None, max_rows=None, retry_count=0):
        return pd.DataFrame(rows)

    monkeypatch.setattr(events, "execute_query_params", fake_execute_query_params)

    client = _build_test_client()
    response = client.get(
        "/collections/joined_timeseries/event_trace/initializations",
        params={
            "primary_location_id": "usgs-12345",
            "configuration_name": "hefs_streamflow_forecast",
            "variable_name": "streamflow_hourly_inst",
            "threshold": "10th",
            "event_id": "2018-07-06 00:00:00-2018-07-09 00:00:00",
            "lead_time_hours": 24,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["available_initialization_datetimes"] == [
        "2018-07-07T00:00:00",
        "2018-07-08T00:00:00",
    ]
    # No initialization is <= event start, so earliest available is used.
    assert payload["default_initialization_datetime"] == "2018-07-07T00:00:00"


def test_event_trace_initializations_invalid_lead_time():
    client = _build_test_client()
    response = client.get(
        "/collections/joined_timeseries/event_trace/initializations",
        params={
            "primary_location_id": "usgs-12345",
            "configuration_name": "hefs_streamflow_forecast",
            "variable_name": "streamflow_hourly_inst",
            "threshold": "10th",
            "event_id": "2018-07-06 00:00:00-2018-07-09 00:00:00",
            "lead_time_hours": 169,
        },
    )

    assert response.status_code == 422
