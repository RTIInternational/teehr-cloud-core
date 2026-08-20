"""
Configuration settings for the TEEHR API.
"""

import os


class Config:
    """Application configuration."""

    # Timeout settings
    WORKER_TIMEOUT = int(os.environ.get("WORKER_TIMEOUT", "300"))  # 5 minutes
    REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "300"))  # 5 minutes
    DB_TIMEOUT = int(os.environ.get("DB_TIMEOUT", "300"))  # 5 minutes

    # Query limits (not used for now)
    # MAX_ROWS_DEFAULT = int(os.environ.get("MAX_ROWS_DEFAULT", "50000"))
    # MAX_TIMESERIES_DEFAULT = int(os.environ.get("MAX_TIMESERIES_DEFAULT", "10000"))

    # Performance settings
    CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", "1000"))
    MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))

    # Database settings
    TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
    TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
    TRINO_USER = os.environ.get("TRINO_USER", "teehr")
    TRINO_CATALOG = os.environ.get("TRINO_CATALOG", "iceberg")
    TRINO_SCHEMA = os.environ.get("TRINO_SCHEMA", "teehr")

    # CORS settings
    CORS_ORIGINS = os.environ.get("CORS_ORIGINS", "*")

    # Authentication settings
    KEYCLOAK_ISSUER_URL = os.environ.get(
        "KEYCLOAK_ISSUER_URL",
        "https://auth.teehr.local.app.garden/realms/teehr",
    )
    KEYCLOAK_JWKS_URL = os.environ.get("KEYCLOAK_JWKS_URL", "")
    KEYCLOAK_AUDIENCE = os.environ.get("KEYCLOAK_AUDIENCE", "teehr-api")
    KEYCLOAK_ALLOWED_AUDIENCES = os.environ.get(
        "KEYCLOAK_ALLOWED_AUDIENCES",
        "teehr-api,teehr-frontend,jupyterhub",
    )
    KEYCLOAK_AUTH_URL = os.environ.get(
        "KEYCLOAK_AUTH_URL",
        f"{KEYCLOAK_ISSUER_URL}/protocol/openid-connect/auth",
    )
    KEYCLOAK_TOKEN_URL = os.environ.get(
        "KEYCLOAK_TOKEN_URL",
        f"{KEYCLOAK_ISSUER_URL}/protocol/openid-connect/token",
    )
    KEYCLOAK_SWAGGER_CLIENT_ID = os.environ.get(
        "KEYCLOAK_SWAGGER_CLIENT_ID",
        "teehr-frontend",
    )

    # API key storage settings
    API_KEYS_DB_DSN = os.environ.get(
        "API_KEYS_DB_DSN",
        "postgresql://keycloak:keycloak123@keycloak-pg:5432/teehr_api",
    )
    API_KEY_PREFIX = os.environ.get("API_KEY_PREFIX", "thk_")
    API_KEY_HASH_SALT = os.environ.get(
        "API_KEY_HASH_SALT",
        "local-dev-change-me",
    )

    # Rate limit settings (requests per minute)
    ANON_RATE_LIMIT_RPM = int(os.environ.get("ANON_RATE_LIMIT_RPM", "20"))
    AUTH_RATE_LIMIT_RPM = int(os.environ.get("AUTH_RATE_LIMIT_RPM", "120"))

    # Polaris token broker settings
    BROKER_TOKEN_EXCHANGE_ENABLED = (
        os.environ.get("BROKER_TOKEN_EXCHANGE_ENABLED", "true").strip().lower()
        in {"1", "true", "t", "yes", "y", "on"}
    )
    BROKER_TOKEN_ENDPOINT = os.environ.get(
        "BROKER_TOKEN_ENDPOINT",
        KEYCLOAK_TOKEN_URL,
    )
    BROKER_OAUTH_CLIENT_ID = os.environ.get("BROKER_OAUTH_CLIENT_ID", "teehr-api")
    BROKER_OAUTH_CLIENT_SECRET = os.environ.get("BROKER_OAUTH_CLIENT_SECRET", "")
    BROKER_TARGET_AUDIENCE = os.environ.get("BROKER_TARGET_AUDIENCE", "account")
    BROKER_DEFAULT_SCOPE = os.environ.get("BROKER_DEFAULT_SCOPE", "openid profile email")
    BROKER_MIN_TTL_SECONDS = int(os.environ.get("BROKER_MIN_TTL_SECONDS", "120"))
    BROKER_MAX_TTL_SECONDS = int(os.environ.get("BROKER_MAX_TTL_SECONDS", "900"))
    BROKER_REQUEST_TIMEOUT_SECONDS = int(
        os.environ.get("BROKER_REQUEST_TIMEOUT_SECONDS", "10")
    )
    BROKER_SUBJECT_CLIENT_ID = os.environ.get("BROKER_SUBJECT_CLIENT_ID", "jupyterhub")
    BROKER_SUBJECT_CLIENT_SECRET = os.environ.get("BROKER_SUBJECT_CLIENT_SECRET", "")
    BROKER_DELEGATED_SESSION_TTL_SECONDS = int(
        os.environ.get("BROKER_DELEGATED_SESSION_TTL_SECONDS", "43200")
    )
    BROKER_SESSION_SIGNING_SECRET = os.environ.get(
        "BROKER_SESSION_SIGNING_SECRET",
        API_KEY_HASH_SALT,
    )
    BROKER_REFRESH_TOKEN_ENCRYPTION_SECRET = os.environ.get(
        "BROKER_REFRESH_TOKEN_ENCRYPTION_SECRET",
        BROKER_SESSION_SIGNING_SECRET,
    )

    # Role-based record/page limits
    ROW_LIMIT_ANON = int(os.environ.get("ROW_LIMIT_ANON", "200"))
    ROW_LIMIT_API_KEY = int(os.environ.get("ROW_LIMIT_API_KEY", "50000"))
    ROW_LIMIT_BASIC_USER = int(os.environ.get("ROW_LIMIT_BASIC_USER", "50000"))
    ROW_LIMIT_AUTH = int(os.environ.get("ROW_LIMIT_AUTH", "10000"))

    # Logging
    LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")


config = Config()
