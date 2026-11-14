"""TEEHR project control plane (Phase 1, epic #148).

Owns projects, members, datasets and runs.
"""

import logging
import threading
from contextlib import asynccontextmanager
from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException
from fastapi.openapi.utils import get_openapi
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from src.config import Settings, get_settings
from src.db import get_engine
from src.provisioning import get_keycloak, get_polaris, reconcile
from src.routes import projects

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s"
)
logger = logging.getLogger("control-plane")

LOGIN_SCHEME = "KeycloakLogin"


def _reconcile_on_start() -> None:
    try:
        count = reconcile(get_engine(), get_polaris(), get_keycloak())
        if count:
            logger.info(
                "Startup reconcile: provisioned %d project(s) (system + unready)", count
            )
    except Exception:  # never take the service down over this
        logger.exception("Startup reconcile failed")


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # Background, so a slow or down Polaris never delays startup or readiness.
    threading.Thread(target=_reconcile_on_start, name="reconcile", daemon=True).start()
    yield


def _swagger_oauth(settings: Settings) -> dict[str, Any] | None:
    """Pre-fill the /docs Authorize dialog: public client, PKCE, no secret."""
    if not settings.docs_client_id:
        return None
    return {
        "clientId": settings.docs_client_id,
        "usePkceWithAuthorizationCodeGrant": True,
        "scopes": "openid profile email",
    }


app = FastAPI(
    title="TEEHR control plane",
    version="0.1.0",
    description=(
        "Projects and membership. Use **Authorize** to sign in with Keycloak, "
        "or paste an access token under BearerAuth."
    ),
    swagger_ui_init_oauth=_swagger_oauth(get_settings()),
    lifespan=lifespan,
)
app.include_router(projects.router)


def add_keycloak_login(schema: dict[str, Any], settings: Settings) -> dict[str, Any]:
    """Offer a Keycloak sign-in on every route that takes a bearer token."""
    if not (settings.docs_client_id and settings.oidc_base_url):
        return schema
    schemes = schema.setdefault("components", {}).setdefault("securitySchemes", {})
    schemes[LOGIN_SCHEME] = {
        "type": "oauth2",
        "description": "Sign in with Keycloak (authorization code + PKCE)",
        "flows": {
            "authorizationCode": {
                "authorizationUrl": f"{settings.oidc_base_url}/auth",
                "tokenUrl": f"{settings.oidc_base_url}/token",
                "scopes": {
                    "openid": "OpenID Connect",
                    "profile": "Profile",
                    "email": "Email",
                },
            }
        },
    }
    for path_item in schema.get("paths", {}).values():
        for operation in path_item.values():
            security = (
                operation.get("security") if isinstance(operation, dict) else None
            )
            if security and any("BearerAuth" in req for req in security):
                operation["security"] = [{LOGIN_SCHEME: ["openid"]}, *security]
    return schema


def _openapi() -> dict[str, Any]:
    if app.openapi_schema is None:
        schema = get_openapi(
            title=app.title,
            version=app.version,
            description=app.description,
            routes=app.routes,
        )
        app.openapi_schema = add_keycloak_login(schema, get_settings())
    return app.openapi_schema


app.openapi = _openapi


@app.get("/health", operation_id="health")
def health() -> dict[str, str]:
    """Liveness: the process is up. Does not touch the database."""
    return {"status": "ok"}


@app.get("/ready", operation_id="ready")
def ready(engine: Annotated[Engine, Depends(get_engine)]) -> dict[str, str]:
    """Readiness: the database answers."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="database unavailable") from exc
    return {"status": "ready"}
