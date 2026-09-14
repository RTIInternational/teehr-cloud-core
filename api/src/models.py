"""
Database models and enums for the TEEHR API.
"""

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel


class MetricsTable(str, Enum):
    """Available metrics table names."""

    SIM_METRICS_BY_LOCATION = "sim_metrics_by_location"
    FCST_METRICS_BY_LOCATION = "fcst_metrics_by_location"
    FCST_METRICS_BY_LEAD_TIME_BINS = "fcst_metrics_by_lead_time_bins"


# class LocationResponse(BaseModel):
#     """Response model for location data."""
#     location_id: str
#     geometry: Dict[str, Any]
#     properties: Optional[Dict[str, Any]] = None


# class MetricResponse(BaseModel):
#     """Response model for metric data."""
#     location_id: str
#     configuration: str
#     variable: str
#     metric_name: str
#     metric_value: float
#     reference_time: Optional[datetime] = None
#     value_time: Optional[datetime] = None


# class TimeseriesPoint(BaseModel):
#     """Single point in a timeseries."""
#     value_time: datetime
#     value: float


# class TimeseriesResponse(BaseModel):
#     """Response model for timeseries data."""
#     location_id: str
#     configuration: Optional[str] = None
#     variable: str
#     unit: Optional[str] = None
#     timeseries: List[TimeseriesPoint]


class HealthResponse(BaseModel):
    """Health check response."""

    status: str
    timestamp: datetime
    version: str


# OGC API Models


class Link(BaseModel):
    """OGC API Link object."""

    href: str
    rel: str
    type: str | None = None
    title: str | None = None
    hreflang: str | None = None


class ConformanceResponse(BaseModel):
    """OGC API Conformance declaration."""

    conformsTo: list[str]


class Extent(BaseModel):
    """OGC API Extent object."""

    spatial: dict[str, Any] | None = None
    temporal: dict[str, Any] | None = None


class Collection(BaseModel):
    """OGC API Collection metadata."""

    id: str
    title: str
    description: str
    links: list[Link]
    extent: Extent | None = None
    itemType: str | None = None
    crs: list[str] | None = None


class CollectionsResponse(BaseModel):
    """OGC API Collections list."""

    links: list[Link]
    collections: list[Collection]


class LandingPage(BaseModel):
    """OGC API Landing page."""

    title: str
    description: str
    links: list[Link]


class EventTraceInitializationsResponse(BaseModel):
    """Initialization metadata for the FIRO event trace slider."""

    primary_location_id: str
    configuration_name: str
    variable_name: str
    threshold: str
    event_id: str
    lead_time_hours: int
    event_start: datetime
    event_end: datetime
    expanded_event_start: datetime
    expanded_event_end: datetime
    available_initialization_datetimes: list[datetime]
    default_initialization_datetime: datetime


class TracePoint(BaseModel):
    """Single value in a trace (observed or forecast)."""

    value_time: datetime
    value: float


class ObservedTraces(BaseModel):
    """Pre and post-initialization observed data."""

    pre_initialization: list[TracePoint]
    post_initialization: list[TracePoint]


class EnsembleMemberTrace(BaseModel):
    """Forecast trace for one ensemble member."""

    member: str
    values: list[TracePoint]


class ForecastPercentileTraces(BaseModel):
    """Percentile traces derived from ensemble forecasts."""

    p10: list[TracePoint]
    p50: list[TracePoint]
    p90: list[TracePoint]


class EventTraceDataResponse(BaseModel):
    """Observed trace data for an event initialization."""

    primary_location_id: str
    configuration_name: str
    variable_name: str
    threshold: str
    initialization_datetime: datetime
    window_start: datetime
    window_end: datetime
    observed: ObservedTraces
    forecast_members: list[EnsembleMemberTrace]
    forecast_percentiles: ForecastPercentileTraces
