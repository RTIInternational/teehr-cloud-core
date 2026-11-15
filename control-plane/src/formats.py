"""Body formats: what dataset writes accept and what reads return.

JSON is parsed with the dataset's declared column types, never by guessing from
the text, and then goes through the same checks as Arrow and Parquet
(``tables.conform``). Timestamps follow the same rule in every format: a
``timestamptz`` value needs a zone offset (``Z`` or ``+05:00``), a ``timestamp``
value must not have one, and sub-microsecond precision is refused, not truncated.

CSV isn't accepted here: files people already have (CSV included) go through
staged upload, where they're inspected first (Phase 3).
"""

import base64
import io
import json
from datetime import date, datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.json as pjson
import pyarrow.parquet as pq

from src.tables import DataMismatch, iceberg_schema

ARROW_STREAM = "application/vnd.apache.arrow.stream"
PARQUET = "application/vnd.apache.parquet"
NDJSON = "application/x-ndjson"
JSON = "application/json"
WRITE_MEDIA_TYPES = (ARROW_STREAM, PARQUET, NDJSON, JSON)


class UnsupportedMediaType(ValueError):
    """Not a body format writes accept."""


def _text_types(columns: list[dict]) -> pa.Schema:
    """Declared types, except timestamps and dates, which JSON reads as text so the
    shared conversion applies the zone-offset rule."""
    target = iceberg_schema(columns).as_arrow()
    return pa.schema(
        pa.field(f.name, pa.large_string())
        if pa.types.is_timestamp(f.type) or pa.types.is_date(f.type)
        else pa.field(f.name, f.type)
        for f in target
    )


def decode(media_type: str, body: bytes, columns: list[dict]) -> pa.Table:
    """The rows in a request body, before the shared checks against ``columns``."""
    try:
        if media_type == ARROW_STREAM:
            return pa.ipc.open_stream(pa.BufferReader(body)).read_all()
        if media_type == PARQUET:
            return pq.read_table(pa.BufferReader(body))
        if media_type in (NDJSON, JSON):
            if any(c["type"] == "binary" for c in columns):
                raise DataMismatch(
                    "this dataset has binary columns; send Arrow or Parquet"
                )
            if media_type == JSON:
                rows = json.loads(body)
                if not isinstance(rows, list) or not all(
                    isinstance(r, dict) for r in rows
                ):
                    raise DataMismatch("JSON body must be an array of objects")
                if not rows:
                    return pa.table({})
                body = "\n".join(json.dumps(r) for r in rows).encode()
            return pjson.read_json(
                io.BytesIO(body),
                parse_options=pjson.ParseOptions(
                    explicit_schema=_text_types(columns),
                    unexpected_field_behavior="error",
                ),
            )
    except DataMismatch:
        raise
    except (pa.ArrowInvalid, OSError, ValueError) as exc:
        raise DataMismatch(f"could not read the {media_type} body: {exc}") from exc
    raise UnsupportedMediaType(f"send one of: {', '.join(WRITE_MEDIA_TYPES)}")


# --- responses ----------------------------------------------------------------

READ_MEDIA_TYPES = (JSON, ARROW_STREAM, PARQUET)


def negotiate(accept: str | None) -> str:
    """The response format for an ``Accept`` header: Arrow or Parquet when asked
    for, JSON otherwise (as in VeriRace)."""
    accept = accept or ""
    if ARROW_STREAM in accept:
        return ARROW_STREAM
    if PARQUET in accept:
        return PARQUET
    return JSON


def _json_value(value):
    if isinstance(value, float) and (value != value or value in (_INF, -_INF)):
        return None  # NaN / infinity aren't JSON
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, bytes):
        return base64.b64encode(value).decode()
    if isinstance(value, Decimal):
        return float(value)  # quantiles: 4 decimal places, exact as a JSON number
    return value


_INF = float("inf")


def json_rows(table: pa.Table) -> list[dict]:
    """Rows as JSON-ready dicts: times ISO 8601 (UTC with offset), binary base64,
    decimals as numbers, NaN as null."""
    return [{k: _json_value(v) for k, v in row.items()} for row in table.to_pylist()]


def encode_binary(table: pa.Table, media_type: str) -> bytes:
    sink = pa.BufferOutputStream()
    if media_type == ARROW_STREAM:
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
    else:
        pq.write_table(table, sink)
    return sink.getvalue().to_pybytes()
