"""TEEHR project control plane (Phase 1, epic #148).

Owns projects, members, datasets and runs. Skeleton for now: health and readiness
only; the project API arrives in #152.
"""

from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from src.db import get_engine

app = FastAPI(title="TEEHR control plane", version="0.1.0")


@app.get("/health")
def health() -> dict[str, str]:
    """Liveness: the process is up. Does not touch the database."""
    return {"status": "ok"}


@app.get("/ready")
def ready(engine: Annotated[Engine, Depends(get_engine)]) -> dict[str, str]:
    """Readiness: the database answers."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="database unavailable") from exc
    return {"status": "ready"}
