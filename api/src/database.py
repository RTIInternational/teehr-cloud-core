"""
Database connection and query utilities.
"""

import logging
import re
import time
from datetime import datetime

import pandas as pd
from trino.dbapi import connect

from .config import config

# Configure logging
logger = logging.getLogger("teehr-api.database")


# Trino connection configuration from config
trino_host = config.TRINO_HOST
trino_port = config.TRINO_PORT
trino_user = config.TRINO_USER
trino_catalog = config.TRINO_CATALOG
trino_schema = config.TRINO_SCHEMA

# Connection pool settings from config
MAX_RETRIES = config.MAX_RETRIES
RETRY_DELAY = 1  # seconds

EVENT_ID_PATTERN = re.compile(
    r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"
    r"-(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})$"
)

_THRESHOLD_ALIASES = {
    "10th": "10th",
    "q_10th": "10th",
    "p10": "10th",
    "0.1": "10th",
    "0.10": "10th",
    "25th": "25th",
    "q_25th": "25th",
    "p25": "25th",
    "0.25": "25th",
    "50th": "50th",
    "q_50th": "50th",
    "p50": "50th",
    "0.5": "50th",
    "0.50": "50th",
    "75th": "75th",
    "q_75th": "75th",
    "p75": "75th",
    "0.75": "75th",
    "90th": "90th",
    "q_90th": "90th",
    "p90": "90th",
    "0.9": "90th",
    "0.90": "90th",
}


def sanitize_string(value: str | None) -> str:
    """
    Sanitize string to prevent SQL injection by allowing only alphanumeric
    characters, underscores, hyphens, and dots. Raise an error if invalid
    characters are found.
    """
    if value is None:
        raise ValueError("Value cannot be None")
    if not re.match(r"^[a-zA-Z0-9_\-\.]+$", value):
        raise ValueError(
            f"Invalid characters in value: {value}. "
            f"Only alphanumeric characters, underscores, hyphens, and "
            f"dots are allowed."
        )
    return value


def get_trino_connection():
    """Create and return a Trino database connection."""
    return connect(
        host=trino_host,
        port=trino_port,
        user=trino_user,
        catalog=trino_catalog,
        schema=trino_schema,
    )


def validate_event_id(value: str | None) -> tuple[datetime, datetime]:
    """Validate and parse an event ID encoded as "start-end" timestamps."""
    if value is None:
        raise ValueError("Event ID cannot be None")

    match = EVENT_ID_PATTERN.match(value)
    if not match:
        raise ValueError(
            "Invalid event ID format. Expected YYYY-MM-DD HH:MM:SS-YYYY-MM-DD HH:MM:SS"
        )

    start_time = datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S")
    end_time = datetime.strptime(match.group(2), "%Y-%m-%d %H:%M:%S")

    if end_time < start_time:
        raise ValueError("Event ID end time must be after start time")

    return start_time, end_time


def normalize_threshold_token(value: str | None) -> str:
    """Normalize threshold aliases to canonical tokens (10th/25th/50th/75th/90th)."""
    if value is None:
        raise ValueError("Threshold value cannot be None")

    token = value.strip().lower()
    canonical = _THRESHOLD_ALIASES.get(token)
    if canonical is None:
        allowed = ", ".join(["10th", "25th", "50th", "75th", "90th"])
        raise ValueError(f"Unsupported threshold token: {value}. Use one of: {allowed}")
    return canonical


def execute_query(
    query: str, max_rows: int | None = None, retry_count: int = 0
) -> pd.DataFrame:
    """Execute a query and return results as a pandas DataFrame.

    Args:
        query: SQL query to execute
        max_rows: Maximum number of rows to return (only applied if specified)
        retry_count: Current retry attempt
    """
    logger.debug(
        f"Executing query (attempt {retry_count + 1}/{MAX_RETRIES + 1}): {query}"
    )

    # Only add LIMIT clause if max_rows is explicitly specified
    if max_rows and "LIMIT" not in query.upper():
        query = f"{query} LIMIT {max_rows}"
        logger.debug(f"Added LIMIT {max_rows} to query")

    try:
        with get_trino_connection() as conn:
            query_start = time.time()
            df = pd.read_sql(query, conn)
            query_time = time.time() - query_start

            logger.debug(
                f"Query completed in {query_time:.3f} seconds, returned {len(df)} rows"
            )

            # Warning for large result sets
            if len(df) > 10000:
                logger.warning(
                    f"Large result set ({len(df)} rows). "
                    f"this may cause processing delays"
                )

            return df

    except Exception as e:
        logger.error(f"Query failed (attempt {retry_count + 1}): {str(e)}")

        # Retry logic for transient errors
        if retry_count < MAX_RETRIES and should_retry_error(e):
            delay = RETRY_DELAY * (2**retry_count)
            logger.info(f"Retrying query in {delay} seconds...")
            time.sleep(delay)
            return execute_query(query, max_rows, retry_count + 1)
        else:
            raise e


def execute_query_params(
    query: str,
    params: list | tuple | None = None,
    max_rows: int | None = None,
    retry_count: int = 0,
) -> pd.DataFrame:
    """Execute a parameterized query and return results as a pandas DataFrame."""
    logger.debug(
        f"Executing parameterized query (attempt {retry_count + 1}/{MAX_RETRIES + 1}): {query}"  # noqa: E501
    )

    if max_rows and "LIMIT" not in query.upper():
        query = f"{query} LIMIT {max_rows}"
        logger.debug(f"Added LIMIT {max_rows} to parameterized query")

    parameters = list(params) if params is not None else []

    try:
        with get_trino_connection() as conn:
            query_start = time.time()
            cursor = conn.cursor()
            cursor.execute(query, parameters)
            rows = cursor.fetchall()
            columns = (
                [col[0] for col in cursor.description] if cursor.description else []
            )
            query_time = time.time() - query_start

            df = pd.DataFrame(rows, columns=columns)

            logger.debug(
                f"Parameterized query completed in {query_time:.3f} seconds, "
                f"returned {len(df)} rows"
            )

            if len(df) > 10000:
                logger.warning(
                    f"Large result set ({len(df)} rows). "
                    f"this may cause processing delays"
                )

            return df

    except Exception as e:
        logger.error(
            f"Parameterized query failed (attempt {retry_count + 1}): {str(e)}"
        )

        if retry_count < MAX_RETRIES and should_retry_error(e):
            delay = RETRY_DELAY * (2**retry_count)
            logger.info(f"Retrying parameterized query in {delay} seconds...")
            time.sleep(delay)
            return execute_query_params(query, parameters, max_rows, retry_count + 1)
        else:
            raise e


def should_retry_error(error: Exception) -> bool:
    """Determine if an error should trigger a retry."""
    error_str = str(error).lower()

    # Retry on common transient errors
    transient_errors = [
        "all connection attempts failed",
        "failed to establish a new connection",
        "connection reset",
        "connection timeout",
        "connection refused",
        "max retries exceeded",
        "temporary failure",
        "server busy",
    ]

    return any(transient_error in error_str for transient_error in transient_errors)  # noqa: E501
