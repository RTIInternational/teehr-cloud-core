"""Request and response bodies - the API contract.

These models generate the published OpenAPI spec (``control-plane/openapi.json``,
pinned by a snapshot test), which is what a CLI or SDK is built from (ADR-0007,
ADR-0011). Inputs are strict: an unknown field is an error, never silently dropped.
"""

import uuid
from datetime import datetime
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Discriminator,
    Field,
    Tag,
    field_validator,
    model_validator,
)

from src.models import ProjectKind, ProjectStatus, Role

# Same rule as spec dataset references; also a valid Polaris namespace, S3 prefix
# and Keycloak role-name fragment.
PROJECT_NAME_PATTERN = r"^[a-z][a-z0-9_]{0,59}$"

# public / platform are the system projects (ADR-0001); teehr is today's live
# namespace, which a new project must not collide with.
RESERVED_PROJECT_NAMES = frozenset({"public", "platform", "teehr"})


class StrictInput(BaseModel):
    """Base for request bodies: reject fields the API doesn't know."""

    model_config = ConfigDict(extra="forbid")


class ProjectCreate(StrictInput):
    name: str = Field(pattern=PROJECT_NAME_PATTERN, description="Unique project name")
    display_name: str | None = Field(default=None, max_length=200)
    description: str | None = Field(default=None, max_length=2000)


class ProjectUpdate(StrictInput):
    display_name: str | None = Field(default=None, max_length=200)
    description: str | None = Field(default=None, max_length=2000)


class ProjectOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    name: str
    display_name: str | None
    description: str | None
    kind: ProjectKind
    status: ProjectStatus
    status_detail: str | None = Field(
        default=None, description="Why provisioning last failed"
    )
    storage_location: str | None = Field(
        default=None, description="The project's storage prefix"
    )
    created_by: str
    created_at: datetime
    my_role: Role | None = Field(
        default=None,
        description=(
            "The caller's role (null if not a member: a platform admin, or anyone "
            "reading a system project)"
        ),
    )


class MemberAdd(StrictInput):
    username: str = Field(min_length=1, max_length=255, description="Keycloak username")
    role: Role


class MemberPut(StrictInput):
    role: Role
    username: str | None = Field(
        default=None,
        max_length=255,
        description="Ignored when Keycloak is configured (taken from Keycloak)",
    )


class MemberOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    subject: str = Field(description="Keycloak user id")
    username: str | None
    role: Role
    added_by: str
    added_at: datetime


class ErrorOut(BaseModel):
    """Every error response: RFC 9457 problem details (``application/problem+json``)."""

    type: str = Field(default="about:blank", description="Problem type URI")
    title: str = Field(description="The HTTP status phrase, e.g. Not Found")
    status: int
    detail: str | None = Field(default=None, description="What went wrong")
    code: str | None = Field(
        default=None,
        description=(
            "Machine-readable reason where a client can act on it, e.g. "
            "`token_refresh_required`"
        ),
    )


class ValidationProblem(BaseModel):
    loc: list[str] = Field(description="Where: e.g. body, columns, 0, type")
    msg: str
    type: str


class ValidationErrorOut(ErrorOut):
    """A 422: the request didn't validate; ``errors`` lists every problem."""

    errors: list[ValidationProblem]


# --- datasets ---------------------------------------------------------------

COLUMN_NAME_PATTERN = r"^[A-Za-z_][A-Za-z0-9_]{0,127}$"
ColumnType = Literal[
    "string",
    "int32",
    "int64",
    "float32",
    "float64",
    "boolean",
    "date",
    "timestamp",
    "timestamptz",
    "binary",
    "decimal(5,4)",
]


class DatasetColumn(StrictInput):
    name: str = Field(pattern=COLUMN_NAME_PATTERN)
    type: ColumnType
    required: bool = Field(default=False, description="Values may not be null")
    doc: str | None = Field(default=None, max_length=1000)


def _unique_names(columns: list[DatasetColumn]) -> list[DatasetColumn]:
    seen: set[str] = set()
    for col in columns:
        key = col.name.lower()
        if key in seen:
            raise ValueError(f"duplicate column name '{col.name}'")
        seen.add(key)
    return columns


class DatasetCreate(StrictInput):
    """An untyped dataset (`table`): any columns. A test path, not part of the
    data model; platform admins only unless configured otherwise."""

    type: Literal["table"] = "table"
    name: str = Field(
        pattern=PROJECT_NAME_PATTERN, description="Unique within the project"
    )
    description: str | None = Field(default=None, max_length=2000)
    columns: list[DatasetColumn] = Field(min_length=1, max_length=500)
    key_columns: list[str] | None = Field(
        default=None,
        max_length=10,
        description=(
            "Columns that identify a row (e.g. location_id, value_time); used to "
            "match rows when upserting. Each must be a declared, required column "
            "and not a float."
        ),
    )

    @field_validator("columns")
    @classmethod
    def _unique_column_names(cls, columns: list[DatasetColumn]):
        return _unique_names(columns)

    @model_validator(mode="after")
    def _valid_key_columns(self):
        if self.key_columns is None:
            return self
        if len(set(self.key_columns)) != len(self.key_columns):
            raise ValueError("key_columns has duplicates")
        declared = {col.name: col for col in self.columns}
        for name in self.key_columns:
            col = declared.get(name)
            if col is None:
                raise ValueError(f"key column '{name}' is not a declared column")
            if not col.required:
                raise ValueError(f"key column '{name}' must be required")
            if col.type in ("float32", "float64"):
                raise ValueError(f"key column '{name}' can't be a float")
        return self


TimeseriesRole = Literal["observed", "simulated", "forecast"]
Statistic = Literal["inst", "mean", "sum", "min", "max"]
OptionalColumn = Literal["reference_time", "member", "quantile", "quality"]
TimePartition = Literal["month", "year", "none"]

# Columns every timeseries has or may have; extra columns can't reuse them.
TIMESERIES_COLUMNS = frozenset(
    {
        "series_key",
        "value_time",
        "value",
        "reference_time",
        "member",
        "quantile",
        "quality",
        "period_start",
        "period_end",
    }
)
SERIES_COLUMNS = frozenset(
    {"series_key", "location_key", "location_layer", "period", "resolution"}
)


class TimeseriesCreate(StrictInput):
    """A `timeseries` dataset (warehouse data model v1): its columns, key and
    layout follow from these fields."""

    type: Literal["timeseries"]
    name: str = Field(
        pattern=PROJECT_NAME_PATTERN, description="Unique within the project"
    )
    description: str | None = Field(default=None, max_length=2000)
    source: str | None = Field(
        default=None, max_length=2000, description="Where the data comes from"
    )
    role: TimeseriesRole
    variable: str = Field(description="A registered variable")
    unit: str | None = Field(
        default=None,
        description=(
            "The variable's canonical unit (the default); values are stored in it"
        ),
    )
    statistic: Statistic
    period: str | None = Field(
        default=None,
        description=(
            "A registered period. Default `instant` for `inst`; required otherwise"
        ),
    )
    optional_columns: list[OptionalColumn] = Field(
        default_factory=list,
        description="Which optional columns exist (reference_time is on for forecasts)",
    )
    extra_columns: list[DatasetColumn] = Field(default_factory=list, max_length=100)
    series_table: str | None = Field(
        default=None,
        pattern=PROJECT_NAME_PATTERN,
        description=(
            "An existing series table to share (e.g. across a forecast family); "
            "default: a new `<name>_series`"
        ),
    )
    series_attributes: list[DatasetColumn] = Field(
        default_factory=list,
        max_length=100,
        description="Extra columns for a new series table (basin, zone, ...)",
    )
    location_layer: str | None = Field(
        default=None,
        pattern=PROJECT_NAME_PATTERN,
        description="Default layer for this dataset's series",
    )
    time_partition: TimePartition = "month"

    @field_validator("optional_columns")
    @classmethod
    def _no_repeats(cls, value: list[str]):
        if len(set(value)) != len(value):
            raise ValueError("optional_columns has duplicates")
        return value

    @field_validator("extra_columns")
    @classmethod
    def _extra_columns(cls, columns: list[DatasetColumn]):
        for col in columns:
            if col.name.lower() in TIMESERIES_COLUMNS:
                raise ValueError(f"'{col.name}' is a timeseries column")
        return _unique_names(columns)

    @field_validator("series_attributes")
    @classmethod
    def _series_attributes(cls, columns: list[DatasetColumn]):
        for col in columns:
            if col.name.lower() in SERIES_COLUMNS:
                raise ValueError(f"'{col.name}' is a series table column")
        return _unique_names(columns)

    @model_validator(mode="after")
    def _consistent(self):
        if self.role == "forecast" and "reference_time" not in self.optional_columns:
            self.optional_columns = ["reference_time", *self.optional_columns]
        if self.series_table and self.series_attributes:
            raise ValueError(
                "series_attributes only apply to a new series table, not a shared one"
            )
        return self


def _dataset_type(value) -> str:
    if isinstance(value, dict):
        return value.get("type", "table")
    return getattr(value, "type", "table")


DatasetRegistration = Annotated[
    Annotated[DatasetCreate, Tag("table")]
    | Annotated[TimeseriesCreate, Tag("timeseries")],
    Discriminator(_dataset_type),
]


class TimeseriesOut(BaseModel):
    """A timeseries dataset's dataset-level metadata."""

    role: TimeseriesRole
    variable: str
    unit: str
    statistic: Statistic
    period: str
    optional_columns: list[OptionalColumn]
    series_table: str
    location_layer: str | None = None
    time_partition: TimePartition
    source: str | None = None


class DatasetOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    name: str
    kind: str = Field(description="table | timeseries | series")
    description: str | None
    columns: list[DatasetColumn]
    key_columns: list[str] | None
    timeseries: TimeseriesOut | None = Field(
        default=None,
        validation_alias="properties",
        description="For a timeseries dataset: its metadata",
    )
    table: str = Field(
        validation_alias="table_identifier",
        description="The Iceberg table: <project namespace>.<dataset name>",
    )
    created_by: str | None
    created_at: datetime


class WriteOut(BaseModel):
    """Lineage for one write through the API."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    mode: str
    filter: str | None
    rows: int
    unit: str | None = Field(
        default=None,
        description="Timeseries: the unit the values arrived in (stored canonical)",
    )
    series_added: int | None = Field(
        default=None, description="Timeseries: new series keys this write added"
    )
    snapshot_id: str | None = Field(
        description="The Iceberg snapshot this write produced (a string: 64-bit id)"
    )
    written_by: str
    written_at: datetime

    @field_validator("snapshot_id", mode="before")
    @classmethod
    def _snapshot_as_string(cls, value):
        return None if value is None else str(value)


class DataPageOut(BaseModel):
    """A page of rows (JSON). Arrow and Parquet pages carry ``next_cursor`` and
    ``total`` in the ``X-Next-Cursor`` and ``X-Total-Rows`` headers instead."""

    rows: list[dict]
    next_cursor: str | None = Field(
        description="Pass as `cursor` for the next page; null on the last page"
    )
    total: int | None = Field(
        default=None, description="Rows matching the filter (only with count=true)"
    )


# --- domains ----------------------------------------------------------------

# UCUM symbols: printable ASCII, no spaces (e.g. m3/s, [cft_i]/s, Cel).
UNIT_SYMBOL_PATTERN = r"^[!-~]{1,64}$"
SLUG_PATTERN = r"^[a-z][a-z0-9_]{0,59}$"
# ISO 8601 durations with fixed parts only (P1D, PT1H, PT15M, P1Y2M).
DURATION_PATTERN = r"^P(\d+Y)?(\d+M)?(\d+W)?(\d+D)?(T(\d+H)?(\d+M)?(\d+S)?)?$"
MONTH_DAY_PATTERN = r"^--(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$"
_DAYS_IN_MONTH = (31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)


class DomainScope(StrictInput):
    project: str | None = Field(
        default=None,
        pattern=PROJECT_NAME_PATTERN,
        description="The project the row belongs to; omit for a public row",
    )


class UnitCreate(DomainScope):
    symbol: str = Field(pattern=UNIT_SYMBOL_PATTERN, description="UCUM symbol")
    dimension: str = Field(pattern=SLUG_PATTERN, description="e.g. volume_flow")
    to_si: float = Field(gt=0, description="si = value * to_si + offset")
    offset: float = 0.0
    description: str | None = Field(default=None, max_length=1000)


class UnitOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    symbol: str
    dimension: str
    to_si: float
    offset: float
    description: str | None
    project: str | None = Field(description="null for a public row")


PeriodForm = Literal["instant", "duration", "calendar", "absolute"]


class PeriodCreate(DomainScope):
    """A period (ADR-0012). A row fills exactly the fields its form uses:
    `duration` takes `length` and `value_time_at`; `calendar` takes `start` and
    `end`; `instant` and `absolute` take neither."""

    name: str = Field(pattern=SLUG_PATTERN)
    form: PeriodForm
    length: str | None = Field(
        default=None, pattern=DURATION_PATTERN, description="ISO 8601, e.g. PT1H"
    )
    value_time_at: Literal["start", "end"] | None = None
    start: str | None = Field(
        default=None, pattern=MONTH_DAY_PATTERN, description="--MM-DD, inclusive"
    )
    end: str | None = Field(
        default=None, pattern=MONTH_DAY_PATTERN, description="--MM-DD, inclusive"
    )
    description: str | None = Field(default=None, max_length=1000)

    @field_validator("length")
    @classmethod
    def _has_a_part(cls, value: str | None):
        if value is not None and (value == "P" or value.endswith("T")):
            raise ValueError(f"{value} is not a duration")
        return value

    @field_validator("start", "end")
    @classmethod
    def _day_exists(cls, value: str | None):
        if value is not None:
            month, day = int(value[2:4]), int(value[5:7])
            if day > _DAYS_IN_MONTH[month - 1]:
                raise ValueError(f"{value} is not a day of the year")
        return value

    @model_validator(mode="after")
    def _fields_for_form(self):
        uses = {
            "instant": set(),
            "absolute": set(),
            "duration": {"length", "value_time_at"},
            "calendar": {"start", "end"},
        }[self.form]
        for field in ("length", "value_time_at", "start", "end"):
            given = getattr(self, field) is not None
            if field in uses and not given:
                raise ValueError(f"a {self.form} period needs {field}")
            if field not in uses and given:
                raise ValueError(f"a {self.form} period doesn't take {field}")
        return self


class PeriodOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    name: str
    form: PeriodForm
    length: str | None
    value_time_at: str | None
    start: str | None
    end: str | None
    description: str | None
    project: str | None = Field(description="null for a public row")


class VariableCreate(DomainScope):
    name: str = Field(pattern=SLUG_PATTERN)
    canonical_unit: str = Field(
        pattern=UNIT_SYMBOL_PATTERN,
        description="A registered unit; values of this variable are stored in it",
    )
    description: str | None = Field(default=None, max_length=1000)


class VariableOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    name: str
    canonical_unit: str
    description: str | None
    project: str | None = Field(description="null for a public row")


# --- uploads ------------------------------------------------------------------


class UploadCreate(StrictInput):
    filename: str = Field(
        min_length=1, max_length=255, pattern=r"^[^/\\]+$", description="No paths"
    )
    format: Literal["csv", "parquet"] | None = Field(
        default=None, description="Default: from the file name's extension"
    )
    kind: Literal["timeseries"] = Field(
        description="What the file holds (layers and crosswalks come later)"
    )


class UploadOut(BaseModel):
    """An upload: its file, what inspecting found, and its load's outcome."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    filename: str
    format: str
    kind: str
    status: str = Field(
        description=(
            "staged | inspected | queued | running | succeeded | failed | cancelled"
        )
    )
    size_bytes: int | None
    preview: dict | None = Field(
        description=(
            "From inspect: columns, sample rows, row count (when known), the proposed "
            "mapping and dataset, what's still needed, and notes"
        )
    )
    mapping: dict | None
    dataset: dict | None
    rows_done: int
    result: dict | None
    problems: list[str] | None
    error: str | None
    created_by: str
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    upload_url: str | None = Field(
        default=None, description="Only when created: PUT the file here"
    )
    upload_url_expires_at: datetime | None = None


class IngestMapping(StrictInput):
    """Changes to the mapping inspect proposed (a field left out keeps it)."""

    series_key: str | None = Field(default=None, description="Column with series keys")
    series_key_value: str | None = Field(
        default=None, max_length=255, description="One series key for every row"
    )
    value_time: str | None = None
    value: str | None = None
    reference_time: str | None = None
    member: str | None = None
    quantile: str | None = None
    quality: str | None = None
    timezone: str | None = Field(
        default=None,
        max_length=64,
        description="IANA zone of times without an offset, e.g. America/Denver",
    )


class IngestDataset(StrictInput):
    """The dataset to load into: an existing timeseries, or a new one."""

    name: str | None = Field(
        default=None, pattern=PROJECT_NAME_PATTERN, description="Default: proposed"
    )
    unit: str = Field(
        pattern=UNIT_SYMBOL_PATTERN,
        description="The unit of the file's values (UCUM); converted on load",
    )
    role: TimeseriesRole | None = Field(default=None, description="New datasets")
    variable: str | None = Field(default=None, description="New datasets")
    statistic: Statistic | None = Field(default=None, description="New datasets")
    period: str | None = None
    description: str | None = Field(default=None, max_length=2000)
    source: str | None = Field(default=None, max_length=2000)
    location_layer: str | None = Field(default=None, pattern=PROJECT_NAME_PATTERN)
    time_partition: TimePartition = "month"


class IngestRequest(StrictInput):
    dataset: IngestDataset
    mapping: IngestMapping = Field(default_factory=IngestMapping)
