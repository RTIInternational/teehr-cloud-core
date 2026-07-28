"""Compatibility layer for developer Spark/Polaris helpers.

Canonical implementations now live in spark_session_utils to keep one shared
utility surface for local and remote Polaris support.
"""

import sys
from pathlib import Path

_CURRENT_DIR = Path(__file__).resolve().parent
if str(_CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(_CURRENT_DIR))

from spark_session_utils import apply_polaris_token_to_spark
from spark_session_utils import create_minio_spark_session
from spark_session_utils import create_spark_session
from spark_session_utils import ensure_broker_session_token
from spark_session_utils import ensure_fresh_polaris_token_via_broker
from spark_session_utils import ensure_fresh_polaris_user_token
from spark_session_utils import mint_polaris_user_token
from spark_session_utils import refresh_polaris_user_token
from spark_session_utils import request_broker_polaris_token


DEV_LOCATION_ID_LIST = [
    # CONUS
    "usgs-02424000",
    "usgs-03068800",
    "usgs-01570500",
    "usgs-01347000",
    "usgs-05443500",
    "usgs-06770500",
    "usgs-08313000",
    "usgs-11421000",
    "usgs-14319500",
    # Alaska
    "usgs-15200280",
    "usgs-15209700",
    "usgs-15209750",
    "usgs-15214000",
    # Hawaii
    "usgs-16010000",
    "usgs-16019000",
    "usgs-16031000",
    "usgs-16060000",
    # Puerto Rico
    "usgs-50010500",
    "usgs-50011000",
    "usgs-50011085",
    "usgs-50011128"
]


__all__ = [
    "DEV_LOCATION_ID_LIST",
    "apply_polaris_token_to_spark",
    "create_minio_spark_session",
    "create_spark_session",
    "ensure_broker_session_token",
    "ensure_fresh_polaris_token_via_broker",
    "ensure_fresh_polaris_user_token",
    "mint_polaris_user_token",
    "refresh_polaris_user_token",
    "request_broker_polaris_token",
]