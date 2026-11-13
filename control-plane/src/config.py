"""Control-plane settings, read from the environment."""

import os
from dataclasses import dataclass

from sqlalchemy.engine import URL, make_url


@dataclass(frozen=True)
class Settings:
    database_url: URL


def get_settings() -> Settings:
    """Build settings from the environment.

    ``CONTROL_PLANE_DB_URL`` wins when set (tests, standalone SQLite). Otherwise a
    deployed service gives the Postgres parts separately so the password never has
    to be URL-escaped by hand. With neither, fall back to a local SQLite file.
    """
    explicit = os.getenv("CONTROL_PLANE_DB_URL")
    if explicit:
        return Settings(database_url=make_url(explicit))

    host = os.getenv("CONTROL_PLANE_DB_HOST")
    if not host:
        return Settings(database_url=make_url("sqlite:///./control_plane.db"))

    return Settings(
        database_url=URL.create(
            "postgresql+psycopg",
            username=os.environ["CONTROL_PLANE_DB_USER"],
            password=os.environ["CONTROL_PLANE_DB_PASSWORD"],
            host=host,
            port=int(os.getenv("CONTROL_PLANE_DB_PORT", "5432")),
            database=os.environ["CONTROL_PLANE_DB_NAME"],
        )
    )
