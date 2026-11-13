"""Shared fixtures: a migrated SQLite database and real signed tokens."""

import time
from pathlib import Path

import jwt
import pytest
from alembic import command
from alembic.config import Config
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient

from src.auth import TokenVerifier, get_verifier
from src.config import Settings, get_settings
from src.db import get_engine, make_engine
from src.main import app

ROOT = Path(__file__).resolve().parents[1]
ISSUER = "https://auth.test/realms/teehr"
AUDIENCES = ("teehr-api", "teehr-frontend")
ADMIN_ROLE = "admin"

_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)


def make_token(
    sub: str,
    *,
    roles: tuple[str, ...] = (),
    issuer: str = ISSUER,
    aud: str | None = "teehr-api",
    azp: str | None = None,
    expires_in: int = 300,
    key=_KEY,
) -> str:
    now = int(time.time())
    claims = {
        "sub": sub,
        "preferred_username": sub,
        "iss": issuer,
        "iat": now,
        "exp": now + expires_in,
        "realm_access": {"roles": list(roles)},
    }
    if aud is not None:
        claims["aud"] = aud
    if azp is not None:
        claims["azp"] = azp
    return jwt.encode(claims, key, algorithm="RS256", headers={"kid": "test"})


def auth(sub: str, **kwargs) -> dict[str, str]:
    return {"Authorization": f"Bearer {make_token(sub, **kwargs)}"}


def alembic_config(url: str) -> Config:
    cfg = Config(str(ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(ROOT / "migrations"))
    cfg.attributes["database_url"] = url
    return cfg


@pytest.fixture
def db_url(tmp_path) -> str:
    url = f"sqlite:///{tmp_path / 'cp.db'}"
    command.upgrade(alembic_config(url), "head")
    return url


@pytest.fixture
def client(db_url):
    engine = make_engine(db_url)
    settings = Settings(
        database_url=engine.url,
        keycloak_issuer_url=ISSUER,
        allowed_audiences=AUDIENCES,
        admin_roles=(ADMIN_ROLE,),
    )
    verifier = TokenVerifier(ISSUER, AUDIENCES, lambda _token: _KEY.public_key())
    app.dependency_overrides[get_engine] = lambda: engine
    app.dependency_overrides[get_settings] = lambda: settings
    app.dependency_overrides[get_verifier] = lambda: verifier
    yield TestClient(app)
    app.dependency_overrides.clear()
