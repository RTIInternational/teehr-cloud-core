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


def test_jobs_tokens_move_into_sessions_and_back(tmp_path):
    import uuid

    from sqlalchemy import text

    url = f"sqlite:///{tmp_path / 'cp.db'}"
    cfg = alembic_config(url)
    command.upgrade(cfg, "0017")
    engine = make_engine(url)
    job, project = uuid.uuid4().hex, uuid.uuid4().hex
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO projects (id, name, kind, status, created_by) "
                "VALUES (:p, 'flood', 'TENANT', 'READY', 'alice')"
            ),
            {"p": project},
        )
        conn.execute(
            text(
                "INSERT INTO jobs (id, kind, project_id, created_by, status, "
                "grant_hash, grant_expires_at, refresh_token) VALUES (:id, 'run', "
                ":p, 'alice', 'RUNNING', 'h', '2030-01-01 00:00:00', 'sealed')"
            ),
            {"id": job, "p": project},
        )

    command.upgrade(cfg, "0018")
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT s.subject, s.kind, s.refresh_token FROM jobs j "
                "JOIN delegated_sessions s ON s.id = j.session_id"
            )
        ).one()
    assert tuple(row) == ("alice", "job", "sealed")

    command.downgrade(cfg, "0017")
    with engine.connect() as conn:
        assert conn.execute(text("SELECT refresh_token FROM jobs")).scalar() == "sealed"
