"""Database engine, sessions and schema handling.

Every control-plane table lives in the ``control_plane`` Postgres schema. That keeps
the service apart from the other users of ``app-pg`` (``api/``'s API keys and
delegated sessions), and removing it is one ``DROP SCHEMA control_plane CASCADE``.
SQLite (standalone use, tests) has no schemas, so the name is translated away there.
"""

from collections.abc import Iterator
from functools import lru_cache
from typing import Annotated

from fastapi import Depends
from sqlalchemy import MetaData, create_engine, event
from sqlalchemy.engine import URL, Engine
from sqlalchemy.orm import DeclarativeBase, Session

from src.config import get_settings

SCHEMA = "control_plane"


class Base(DeclarativeBase):
    metadata = MetaData(schema=SCHEMA)


def _sqlite_foreign_keys_on(dbapi_conn, _record) -> None:
    # SQLite ignores foreign keys (and ON DELETE CASCADE) unless asked.
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


def make_engine(url: URL | str) -> Engine:
    engine = create_engine(url, pool_pre_ping=True)
    if engine.dialect.name == "sqlite":
        event.listen(engine, "connect", _sqlite_foreign_keys_on)
        return engine.execution_options(schema_translate_map={SCHEMA: None})
    return engine


@lru_cache
def get_engine() -> Engine:
    """The process-wide engine; a FastAPI dependency that tests override."""
    return make_engine(get_settings().database_url)


def get_session(engine: Annotated[Engine, Depends(get_engine)]) -> Iterator[Session]:
    with Session(engine) as session:
        yield session
