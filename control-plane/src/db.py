"""Database engine and schema handling.

Every control-plane table lives in the ``control_plane`` Postgres schema. That keeps
the service apart from the other users of ``app-pg`` (``api/``'s API keys and
delegated sessions), and removing it is one ``DROP SCHEMA control_plane CASCADE``.
SQLite (standalone use, tests) has no schemas, so the name is translated away there.
"""

from functools import lru_cache

from sqlalchemy import MetaData, create_engine
from sqlalchemy.engine import URL, Engine
from sqlalchemy.orm import DeclarativeBase

from src.config import get_settings

SCHEMA = "control_plane"


class Base(DeclarativeBase):
    metadata = MetaData(schema=SCHEMA)


def make_engine(url: URL | str) -> Engine:
    engine = create_engine(url, pool_pre_ping=True)
    if engine.dialect.name == "sqlite":
        return engine.execution_options(schema_translate_map={SCHEMA: None})
    return engine


@lru_cache
def get_engine() -> Engine:
    """The process-wide engine; a FastAPI dependency that tests override."""
    return make_engine(get_settings().database_url)
