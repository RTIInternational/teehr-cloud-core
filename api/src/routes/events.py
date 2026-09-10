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
from ..models import EventTraceInitializationsResponse

router = APIRouter()
logger = logging.getLogger("teehr-api.routes.events")


def _select_default_initialization(
    reference_times: list[datetime], event_start: datetime, event_end: datetime
) -> datetime:
    """Select nearest initialization to event midpoint; tie-break to earlier."""
    midpoint = event_start + ((event_end - event_start) / 2)
    return min(reference_times, key=lambda ts: (abs(ts - midpoint), ts))


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

        # Limit event lookup by parsed event_id bounds to reduce unnecessary scans.
        lower_bound = event_start_hint.strftime("%Y-%m-%d %H:%M:%S")
        upper_bound = event_end_hint.strftime("%Y-%m-%d %H:%M:%S")

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
              AND value_time >= TIMESTAMP '{lower_bound}'
              AND value_time <= TIMESTAMP '{upper_bound}'
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

        reference_times = sorted(
            {ts.to_pydatetime() for ts in df["reference_time"] if pd.notna(ts)}
        )
        if not reference_times:
            raise HTTPException(
                status_code=404,
                detail="No initialization datetimes found for this event.",
            )

        # Guard against degenerate windows and preserve deterministic midpoint logic.
        if event_end < event_start:
            event_end = event_start
        if event_end == event_start:
            event_end = event_end + timedelta(seconds=0)

        default_initialization = _select_default_initialization(
            reference_times, event_start, event_end
        )

        return EventTraceInitializationsResponse(
            primary_location_id=safe_location,
            configuration_name=safe_configuration,
            variable_name=safe_variable,
            threshold=canonical_threshold,
            event_id=event_id,
            event_start=event_start,
            event_end=event_end,
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
