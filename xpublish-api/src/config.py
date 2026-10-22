import os


class Config:
    KEYCLOAK_ISSUER_URL: str = os.getenv(
        "KEYCLOAK_ISSUER_URL", ""
    )
    # Optional override — if set, skips OpenID Connect discovery.
    # Set to the internal cluster URL to avoid routing through the ingress.
    KEYCLOAK_JWKS_URL: str = os.getenv("KEYCLOAK_JWKS_URL", "")
    # Comma-separated list of accepted aud/azp values in JWT tokens.
    KEYCLOAK_ALLOWED_AUDIENCES: str = os.getenv(
        "KEYCLOAK_ALLOWED_AUDIENCES", "teehr-frontend"
    )
    # Read access by Keycloak realm role (JSON); parsed and validated in authz.parse_access_rules
    TEEHR_ACCESS_RULES: str = os.getenv("TEEHR_ACCESS_RULES", "{}")
    # Role assumed to create notebook S3 credentials (credentials.py); empty where there's no STS
    ICECHUNK_ACCESS_ROLE_ARN: str = os.getenv("ICECHUNK_ACCESS_ROLE_ARN", "")
    # "true" (local S3, or no access role yet): the credentials route returns the repo list only
    ICECHUNK_STS_UNAVAILABLE: bool = os.getenv("ICECHUNK_STS_UNAVAILABLE", "true").strip().lower() == "true"


config = Config()
