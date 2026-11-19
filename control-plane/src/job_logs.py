"""Job logs: the lines a job's pods log (a run's plan, shards and writes; a load's
steps), sent through the job's grant and kept with the job, so they outlive its
pods and read in order across them. ``GET .../runs/{id}/logs`` and
``GET .../uploads/{id}/logs`` read them."""

from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.models import JobLog

#: Lines kept per job; past this, one line says the rest were dropped.
MAX_LINES = 20_000
TRUNCATED = "the log is truncated: later lines weren't kept"


def add(session: Session, job_id, pod: str | None, lines: list[dict]) -> int:
    """Keep ``lines`` ({at, level, message}) for the job, up to MAX_LINES in all;
    returns how many were kept."""
    have = session.scalar(select(func.count()).where(JobLog.job_id == job_id)) or 0
    room = MAX_LINES - have
    if room <= 0:
        return 0
    kept = lines[:room]
    for line in kept:
        session.add(JobLog(job_id=job_id, pod=pod, at=line["at"], level=line["level"],
                           message=line["message"]))  # fmt: skip
    if len(lines) > room:
        session.add(JobLog(job_id=job_id, pod=pod, at=datetime.now(UTC),
                           level="WARNING", message=TRUNCATED))  # fmt: skip
    session.commit()
    return len(kept)


def read(session: Session, job_id, after: int | None, limit: int) -> list[JobLog]:
    """The job's lines after line ``after`` (all when None), oldest first."""
    query = select(JobLog).where(JobLog.job_id == job_id)
    if after is not None:
        query = query.where(JobLog.id > after)
    return list(session.scalars(query.order_by(JobLog.id).limit(limit)))
