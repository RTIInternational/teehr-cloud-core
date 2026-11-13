"""Skeleton checks: health, readiness, and the migration chain (SQLite)."""

from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from sqlalchemy import inspect

from src.db import get_engine, make_engine
from src.main import app

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def client():
    yield TestClient(app)
    app.dependency_overrides.clear()


def alembic_config(url: str) -> Config:
    cfg = Config(str(ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(ROOT / "migrations"))
    cfg.attributes["database_url"] = url
    return cfg


def test_health(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_ready_when_database_answers(client, tmp_path):
    engine = make_engine(f"sqlite:///{tmp_path / 'cp.db'}")
    app.dependency_overrides[get_engine] = lambda: engine
    resp = client.get("/ready")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ready"}


def test_not_ready_when_database_is_down(client):
    engine = make_engine("postgresql+psycopg://u:p@127.0.0.1:1/none")
    app.dependency_overrides[get_engine] = lambda: engine
    resp = client.get("/ready")
    assert resp.status_code == 503


def test_migrations_upgrade_and_downgrade(tmp_path):
    url = f"sqlite:///{tmp_path / 'cp.db'}"
    cfg = alembic_config(url)

    command.upgrade(cfg, "head")
    tables = inspect(make_engine(url)).get_table_names()
    assert "alembic_version" in tables

    command.downgrade(cfg, "base")
