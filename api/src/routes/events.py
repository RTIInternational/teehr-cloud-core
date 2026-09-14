"""Event-oriented endpoints for FIRO workflows."""

import logging
from datetime import datetime, timedelta

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

from ..database import (
    execute_query_params,
    normalize_threshold_token,
    sanitize_string,
    trino_catalog,
    trino_schema,
    validate_event_id,
)
from ..models import (
    EventTraceDataResponse,
    EventTraceInitializationsResponse,
    ObservedTraces,
    TracePoint,
)

router = APIRouter()
logger = logging.getLogger("teehr-api.routes.events")


def _select_default_initialization(
    reference_times: list[datetime], event_start: datetime
) -> datetime:
    """Select latest initialization at or before event start.

    Falls back to the earliest available initialization when no prior forecast exists.
    """
    prior_or_equal = [ts for ts in reference_times if ts <= event_start]
    if prior_or_equal:
        return prior_or_equal[-1]
    return reference_times[0]


@router.get(
    "/collections/joined_timeseries/event_trace/initializations",
    response_model=EventTraceInitializationsResponse,
)
async def get_event_trace_initializations(
    primary_location_id: str = Query(...),
    configuration_name: str = Query(...),
    variable_name: str = Query(...),
    threshold: str = Query(...),
    event_id: str = Query(...),
    lead_time_hours: int = Query(..., ge=1, le=168),
):
    """Return event window and available initialization datetimes for slider setup."""
    try:
        safe_location = sanitize_string(primary_location_id)
        safe_configuration = sanitize_string(configuration_name)
        safe_variable = sanitize_string(variable_name)

        canonical_threshold = normalize_threshold_token(threshold)
        event_start_hint, event_end_hint = validate_event_id(event_id)

        event_flag_column = f"event_{canonical_threshold}"
        event_id_column = f"event_{canonical_threshold}_id"

        query = f"""
            SELECT
                reference_time,
                MIN(value_time) AS event_start,
                MAX(value_time) AS event_end
            FROM {trino_catalog}.{trino_schema}.joined_timeseries
            WHERE primary_location_id = ?
              AND configuration_name = ?
              AND variable_name = ?
              AND {event_flag_column} = true
              AND {event_id_column} = ?
            GROUP BY reference_time
            ORDER BY reference_time
        """

        df = execute_query_params(
            query,
            params=[safe_location, safe_configuration, safe_variable, event_id],
        )

        if df.empty:
            raise HTTPException(
                status_code=404,
                detail="No initialization data found for this event and filter combination.",
            )

        df = df.dropna(subset=["reference_time"])
        if df.empty:
            raise HTTPException(
                status_code=404,
                detail="No initialization datetimes found for this event.",
            )

        df["reference_time"] = pd.to_datetime(df["reference_time"])
        df["event_start"] = pd.to_datetime(df["event_start"])
        df["event_end"] = pd.to_datetime(df["event_end"])

        event_start = df["event_start"].min().to_pydatetime()
        event_end = df["event_end"].max().to_pydatetime()

        expanded_event_start = event_start - timedelta(hours=lead_time_hours)
        expanded_event_end = event_end + timedelta(hours=24)

        reference_times = sorted(
            {
                ts.to_pydatetime()
                for ts in df["reference_time"]
                if pd.notna(ts)
                and expanded_event_start <= ts.to_pydatetime() <= expanded_event_end
            }
        )
        if not reference_times:
            raise HTTPException(
                status_code=404,
                detail="No initialization datetimes found in expanded event window.",
            )

        default_initialization = _select_default_initialization(
            reference_times, event_start
        )

        return EventTraceInitializationsResponse(
            primary_location_id=safe_location,
            configuration_name=safe_configuration,
            variable_name=safe_variable,
            threshold=canonical_threshold,
            event_id=event_id,
            lead_time_hours=lead_time_hours,
            event_start=event_start,
            event_end=event_end,
            expanded_event_start=expanded_event_start,
            expanded_event_end=expanded_event_end,
            available_initialization_datetimes=reference_times,
            default_initialization_datetime=default_initialization,
        )

    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.exception("Failed to get event trace initialization metadata")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch event trace initialization metadata: {str(e)}",
        ) from e


def _parse_iso_datetime(dt_str: str) -> datetime:
    """Parse ISO 8601 datetime string to datetime object."""
    return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))


@router.get(
    "/collections/joined_timeseries/event_trace/data",
    response_model=EventTraceDataResponse,
)
async def get_event_trace_data(
    primary_location_id: str = Query(...),
    configuration_name: str = Query(...),
    variable_name: str = Query(...),
    threshold: str = Query(...),
    window_start: str = Query(...),  # ISO 8601 datetime
    window_end: str = Query(...),  # ISO 8601 datetime
    initialization_time: str = Query(...),  # ISO 8601 datetime
):
    """Return observed trace data split pre/post initialization."""
    try:
        safe_location = sanitize_string(primary_location_id)
        safe_configuration = sanitize_string(configuration_name)
        safe_variable = sanitize_string(variable_name)
        canonical_threshold = normalize_threshold_token(threshold)

        window_start_dt = _parse_iso_datetime(window_start)
        window_end_dt = _parse_iso_datetime(window_end)
        init_time_dt = _parse_iso_datetime(initialization_time)

        if init_time_dt < window_start_dt or init_time_dt > window_end_dt:
            raise ValueError(
                "initialization_time must be within [window_start, window_end]"
            )

        # Query hourly-averaged observed data within the extended window
        query = f"""
            SELECT
                date_trunc('hour', value_time) as value_time,
                avg(primary_value) as value
            FROM {trino_catalog}.{trino_schema}.joined_timeseries
            WHERE primary_location_id = ?
                AND configuration_name = ?
                AND variable_name = ?
                AND value_time >= from_iso8601_timestamp(?)
                AND value_time <= from_iso8601_timestamp(?)
            GROUP BY date_trunc('hour', value_time)
            ORDER BY value_time
            LIMIT 2001
        """

        df = execute_query_params(
            query,
            params=[
                safe_location,
                safe_configuration,
                safe_variable,
                window_start_dt.isoformat(),
                window_end_dt.isoformat(),
            ],
        )

        if df.empty:
            raise HTTPException(
                status_code=404,
                detail="No observed data found in the specified time window.",
            )

        df["value_time"] = pd.to_datetime(df["value_time"])
        df = df.dropna(subset=["value"])

        if df.empty:
            raise HTTPException(
                status_code=404,
                detail="No observed data with valid values found in the specified time window.",
            )

        # Check if result exceeds maximum allowed points
        max_points = 2000
        if len(df) > max_points:
            raise HTTPException(
                status_code=400,
                detail=f"Query returned {len(df)} data points, which exceeds the maximum limit of {max_points}. "
                f"Please narrow your time window or select a different time period.",
            )

        # Split into pre and post initialization
        pre_init_df = df[df["value_time"] <= init_time_dt]
        post_init_df = df[df["value_time"] >= init_time_dt]

        pre_traces = [
            TracePoint(
                value_time=row["value_time"].to_pydatetime(), value=float(row["value"])
            )
            for _, row in pre_init_df.iterrows()
        ]
        post_traces = [
            TracePoint(
                value_time=row["value_time"].to_pydatetime(), value=float(row["value"])
            )
            for _, row in post_init_df.iterrows()
        ]

        return EventTraceDataResponse(
            primary_location_id=safe_location,
            configuration_name=safe_configuration,
            variable_name=safe_variable,
            threshold=canonical_threshold,
            initialization_datetime=init_time_dt,
            window_start=window_start_dt,
            window_end=window_end_dt,
            observed=ObservedTraces(  # type: ignore
                pre_initialization=pre_traces,
                post_initialization=post_traces,
            ),
        )

    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.exception("Failed to get event trace data")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch event trace data: {str(e)}",
        ) from e
