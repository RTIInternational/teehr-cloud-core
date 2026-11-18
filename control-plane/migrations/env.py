"""Alembic environment for the control plane.

Run as a deploy step (the Deployment's init container), never at app startup.
On Postgres the ``control_plane`` schema is created first, because Alembic's own
version table lives inside it.
"""

from logging.config import fileConfig

from alembic import context
from sqlalchemy import text

import src.models  # noqa: F401  (registers the tables on Base.metadata)
from src.config import get_settings
from src.db import SCHEMA, Base, make_engine

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)


def _only_control_plane_schema(name, type_, _parent_names) -> bool:
    """Keep Alembic out of every other schema on app-pg.

    Without this, autogenerate would see api/'s tables in ``public`` (api_keys,
    delegated_sessions) as unknown and write a migration that drops them.
    """
    if type_ == "schema":
        return name == SCHEMA
    return True


def run_migrations_online() -> None:
    url = config.attributes.get("database_url") or get_settings().database_url
    engine = make_engine(url)
    is_postgres = engine.dialect.name == "postgresql"

    with engine.connect() as conn:
        if is_postgres:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"'))
            conn.commit()
        context.configure(
            connection=conn,
            target_metadata=Base.metadata,
            version_table_schema=SCHEMA if is_postgres else None,
            include_schemas=is_postgres,
            include_name=_only_control_plane_schema if is_postgres else None,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    raise SystemExit("Offline (SQL script) migrations are not supported.")

run_migrations_online()
