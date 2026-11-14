"""The /docs page offers a Keycloak sign-in when a docs client is configured."""

from fastapi.openapi.utils import get_openapi
from sqlalchemy.engine import make_url

from src.config import Settings
from src.main import LOGIN_SCHEME, add_keycloak_login, app

ISSUER = "https://auth.test/realms/teehr"


def _schema(**settings_kwargs):
    settings = Settings(database_url=make_url("sqlite://"), **settings_kwargs)
    base = get_openapi(title=app.title, version=app.version, routes=app.routes)
    return add_keycloak_login(base, settings)


def test_login_scheme_points_at_keycloak():
    schema = _schema(
        keycloak_issuer_url=ISSUER, docs_client_id="teehr-control-plane-docs"
    )
    flow = schema["components"]["securitySchemes"][LOGIN_SCHEME]["flows"][
        "authorizationCode"
    ]
    assert flow["authorizationUrl"] == f"{ISSUER}/protocol/openid-connect/auth"
    assert flow["tokenUrl"] == f"{ISSUER}/protocol/openid-connect/token"


def test_protected_routes_offer_login_and_public_routes_do_not():
    schema = _schema(
        keycloak_issuer_url=ISSUER, docs_client_id="teehr-control-plane-docs"
    )
    projects_post = schema["paths"]["/v1/projects"]["post"]
    assert {LOGIN_SCHEME: ["openid"]} in projects_post["security"]
    assert {"BearerAuth": []} in projects_post["security"]
    assert "security" not in schema["paths"]["/health"]["get"]


def test_no_login_scheme_without_docs_client():
    schema = _schema(keycloak_issuer_url=ISSUER)
    assert LOGIN_SCHEME not in schema.get("components", {}).get("securitySchemes", {})
