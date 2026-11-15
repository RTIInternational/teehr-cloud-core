"""Inspecting an uploaded file: what's in it, and how it maps to a timeseries.

Reads only the start of the file (a Parquet file's footer and first rows; a CSV's
first megabyte), and proposes a mapping from its columns to the timeseries columns
(warehouse data model v1), plus what the uploader still has to say (role,
variable, unit, ...). Nothing here writes data; the load job does (Phase 3, jobs).
"""

import csv
import io
import re
from dataclasses import dataclass, field

import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

from src.formats import json_rows

FORMATS = ("csv", "parquet")
CSV_HEAD_BYTES = 1024 * 1024
SAMPLE_ROWS = 10

# Column names that usually mean each timeseries column (lower-case).
CANDIDATES: dict[str, tuple[str, ...]] = {
    "series_key": (
        "series_key", "location_id", "location", "site_no", "site", "station",
        "station_id", "gage_id", "gauge_id", "feature_id", "id",
    ),
    "value_time": (
        "value_time", "datetime", "date_time", "valid_time", "time", "timestamp",
        "date", "obs_time",
    ),
    "value": (
        "value", "streamflow", "discharge", "flow", "stage", "precipitation",
        "precip", "temperature", "obs", "observation",
    ),
    "reference_time": (
        "reference_time", "issue_time", "issued", "forecast_time", "init_time",
    ),
    "member": ("member", "ensemble_member", "ens_member", "trace"),
    "quantile": ("quantile", "probability", "percentile"),
    "quality": ("quality", "qualifier", "qualifiers", "qc", "flag", "flags"),
}  # fmt: skip
MAPPED = tuple(CANDIDATES)
_OFFSET = re.compile(r"(?:[Zz]|[+-]\d{2}:?\d{2})$")


class UploadError(ValueError):
    """The file can't be read as the format it claims."""


def detect_format(filename: str) -> str | None:
    suffix = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    return {"csv": "csv", "parquet": "parquet", "pq": "parquet"}.get(suffix)


def dataset_name(filename: str) -> str:
    """A dataset name proposed from a file name (``USGS Obs-2026.csv`` ->
    ``usgs_obs_2026``)."""
    stem = filename.rsplit("/", 1)[-1].rsplit(".", 1)[0].lower()
    slug = re.sub(r"[^a-z0-9]+", "_", stem).strip("_")
    if not slug or not slug[0].isalpha():
        slug = f"upload_{slug}".rstrip("_")
    return slug[:60]


def read_head(file, fmt: str) -> tuple[pa.Table, int | None]:
    """The first rows of a staged file, and its row count when the format says."""
    try:
        if fmt == "parquet":
            parquet = pq.ParquetFile(file)
            rows = parquet.metadata.num_rows
            if parquet.metadata.num_row_groups == 0:
                return parquet.schema_arrow.empty_table(), rows
            batch = next(parquet.iter_batches(batch_size=SAMPLE_ROWS), None)
            head = (
                pa.Table.from_batches([batch])
                if batch is not None
                else parquet.schema_arrow.empty_table()
            )
            return head, rows
        raw = file.read(CSV_HEAD_BYTES)
        complete = len(raw) < CSV_HEAD_BYTES
        if not complete:  # drop the partial last line
            raw = raw[: raw.rfind(b"\n") + 1]
        table = read_csv_text(raw)
        return table, (table.num_rows if complete else None)
    except (pa.ArrowInvalid, OSError) as exc:
        raise UploadError(f"can't read the file as {fmt}: {exc}") from exc


def read_csv_text(data: bytes) -> pa.Table:
    """A CSV with every column as text (empty = null), so nothing is guessed:
    ``01010000`` stays ``01010000``. Values are typed later, by the dataset's
    columns, the same way JSON writes are."""
    header = next(
        csv.reader(io.StringIO(data.split(b"\n", 1)[0].decode("utf-8-sig"))), []
    )
    if not header:
        raise UploadError("the CSV has no header row")
    options = pcsv.ConvertOptions(
        column_types={name: pa.string() for name in header},
        strings_can_be_null=True,
    )
    return pcsv.read_csv(
        io.BytesIO(data), read_options=pcsv.ReadOptions(encoding="utf-8-sig"),
        convert_options=options,
    )  # fmt: skip


def looks_like(column: pa.ChunkedArray) -> str:
    """What a column's values look like: integer, number, time, text or empty."""
    if column.null_count == len(column):
        return "empty"
    if not (pa.types.is_string(column.type) or pa.types.is_large_string(column.type)):
        t = column.type
        if pa.types.is_integer(t):
            return "integer"
        if pa.types.is_floating(t) or pa.types.is_decimal(t):
            return "number"
        if pa.types.is_timestamp(t) or pa.types.is_date(t):
            return "time"
        return "text"
    for kind, target in (("integer", pa.int64()), ("number", pa.float64())):
        try:
            column.cast(target)
            return kind
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
            pass
    try:
        column.cast(pa.timestamp("us"))
        return "time"
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
        pass
    if all(_OFFSET.search(v.strip()) for v in column.to_pylist() if v):
        try:
            column.cast(pa.timestamp("us", tz="UTC"))
            return "time"
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
            pass
    return "text"


def _guess(names: list[str], table: pa.Table) -> dict[str, str | None]:
    lower = {n.lower(): n for n in names}
    mapping: dict[str, str | None] = {}
    taken: set[str] = set()
    for target in MAPPED:
        match = next(
            (
                lower[c]
                for c in CANDIDATES[target]
                if c in lower and lower[c] not in taken
            ),
            None,
        )
        mapping[target] = match
        if match:
            taken.add(match)
    if mapping["value"] is None:  # the only numeric column left
        numeric = [
            n for n in names
            if n not in taken and looks_like(table[n]) in ("integer", "number")
        ]  # fmt: skip
        if len(numeric) == 1:
            mapping["value"] = numeric[0]
    return mapping


def _has_zone(column: pa.ChunkedArray) -> bool | None:
    """Whether a time column's values carry a zone (None: can't tell)."""
    if pa.types.is_timestamp(column.type):
        return column.type.tz is not None
    if pa.types.is_string(column.type) or pa.types.is_large_string(column.type):
        values = [v for v in column.to_pylist() if v]
        if values:
            return all(_OFFSET.search(v.strip()) for v in values)
    return None


@dataclass
class Preview:
    columns: list[dict]
    sample: list[dict]
    rows: int | None
    mapping: dict
    dataset: dict
    needed: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "columns": self.columns,
            "sample": self.sample,
            "rows": self.rows,
            "mapping": self.mapping,
            "dataset": self.dataset,
            "needed": self.needed,
            "notes": self.notes,
        }


def inspect(head: pa.Table, rows: int | None, filename: str) -> Preview:
    """What the file holds, and a proposed mapping to a timeseries."""
    names = head.column_names
    columns = [
        {
            "name": n,
            "type": str(head.schema.field(n).type),
            "looks_like": looks_like(head[n]),
            "sample": [None if v is None else str(v) for v in head[n].to_pylist()[:5]],
        }
        for n in names
    ]
    mapping: dict = {**_guess(names, head), "series_key_value": None, "timezone": None}
    needed, notes = [], []
    if mapping["series_key"] is None:
        needed.append(
            "mapping.series_key (a column) or mapping.series_key_value (one series)"
        )
    for target in ("value_time", "value"):
        if mapping[target] is None:
            needed.append(f"mapping.{target}")
    for target in ("value_time", "reference_time"):
        column = mapping[target]
        if column and _has_zone(head[column]) is False:
            needed.append("mapping.timezone")
            notes.append(
                f"'{column}' times have no zone offset; set mapping.timezone "
                "(e.g. UTC or America/Denver)"
            )
            break
    if rows is None:
        notes.append("row count is known after loading (CSV)")
    needed += ["dataset.role", "dataset.variable", "dataset.unit", "dataset.statistic"]
    return Preview(
        columns=columns,
        sample=json_rows(head.slice(0, SAMPLE_ROWS)),
        rows=rows,
        mapping=mapping,
        dataset={"name": dataset_name(filename)},
        needed=needed,
        notes=notes,
    )
