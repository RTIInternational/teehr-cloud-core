"""A run's shard queue, which every pod of the run works (ADR-0017).

Pod 0 plans the run and queues its shards; every pod, pod 0 included, claims shards
one at a time, runs them, and records what each wrote; pod 0 then merges and
commits. The queue lives here, in the control plane's database; pods reach it only
through their job grant (``routes/job_grants.py``).

Claiming takes the lowest shard that is queued, or whose pod stopped heartbeating
(its heartbeat older than the lapse), by raising its ``attempt`` only if nobody
changed the row since. Every later call names that attempt, so a pod that lost its
shard can't record it. A shard is tried ``MAX_ATTEMPTS`` times (VeriRace's rules).
"""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import and_, delete, or_, select, update
from sqlalchemy.orm import Session

from src.models import Run, RunShard

MAX_ATTEMPTS = 3
QUEUED, RUNNING, DONE, FAILED = "queued", "running", "done", "failed"


def _now() -> datetime:
    return datetime.now(UTC)


def queue(
    session: Session, run: Run, plan: dict, shards: list[list[str] | None]
) -> None:
    """Replace the run's queue with ``shards`` (the observed keys of each; None:
    every series, in one shard)."""
    session.execute(delete(RunShard).where(RunShard.run_id == run.id))
    run.shard_plan = plan
    session.add_all(
        RunShard(
            run_id=run.id,
            index=i,
            keys=None if keys is None else list(keys),
            status=QUEUED,
            attempt=0,
        )  # fmt: skip
        for i, keys in enumerate(shards)
    )
    session.commit()


@dataclass
class Claim:
    shard: RunShard | None
    settled: bool  # every shard is done or failed: nothing more will come


def claim(session: Session, run: Run, worker: str, lapse: timedelta) -> Claim:
    """The next shard for ``worker``, if one can be had now."""
    stale = _now() - lapse
    while True:
        candidate = session.scalar(
            select(RunShard)
            .where(
                RunShard.run_id == run.id,
                or_(
                    RunShard.status == QUEUED,
                    and_(RunShard.status == RUNNING, RunShard.heartbeat_at < stale),
                ),
            )
            .order_by(RunShard.index)
            .limit(1)
        )
        if candidate is None:
            open_ = session.scalar(
                select(RunShard.id).where(
                    RunShard.run_id == run.id, RunShard.status.in_((QUEUED, RUNNING))
                )
            )
            return Claim(None, settled=open_ is None)
        if candidate.status == RUNNING and candidate.attempt >= MAX_ATTEMPTS:
            _settle(session, candidate, RUNNING, FAILED,
                    error=f"its pod stopped responding ({candidate.attempt} tries)")  # fmt: skip
            continue
        taken = session.execute(
            update(RunShard)
            .where(
                RunShard.id == candidate.id,
                RunShard.attempt == candidate.attempt,
                RunShard.status == candidate.status,
            )
            .values(
                status=RUNNING,
                attempt=candidate.attempt + 1,
                worker=worker,
                heartbeat_at=_now(),
                error=None,
            )  # fmt: skip
        ).rowcount
        session.commit()
        if taken:
            session.refresh(candidate)
            return Claim(candidate, settled=False)
        # Another pod took it first: look again.


def _settle(session: Session, shard: RunShard, was: str, status: str, **values) -> bool:
    done = session.execute(
        update(RunShard)
        .where(
            RunShard.id == shard.id,
            RunShard.attempt == shard.attempt,
            RunShard.status == was,
        )
        .values(**{"status": status, "finished_at": _now(), **values})
    ).rowcount
    session.commit()
    return bool(done)


def _mine(session: Session, run: Run, index: int, attempt: int) -> RunShard | None:
    """The shard, if ``attempt`` still holds it (running, not taken over)."""
    return session.scalar(
        select(RunShard).where(
            RunShard.run_id == run.id,
            RunShard.index == index,
            RunShard.attempt == attempt,
            RunShard.status == RUNNING,
        )
    )


def heartbeat(session: Session, run: Run, index: int, attempt: int) -> bool:
    """Whether the shard is still this attempt's (and keep it so)."""
    shard = _mine(session, run, index, attempt)
    if shard is None:
        return False
    shard.heartbeat_at = _now()
    session.commit()
    return True


def done(session: Session, run: Run, index: int, attempt: int, outputs: dict) -> bool:
    shard = _mine(session, run, index, attempt)
    return shard is not None and _settle(session, shard, RUNNING, DONE, outputs=outputs)


def fail(
    session: Session, run: Run, index: int, attempt: int, error: str, retry: bool
) -> bool:
    """The attempt failed: queue the shard again (up to MAX_ATTEMPTS), or fail it."""
    shard = _mine(session, run, index, attempt)
    if shard is None:
        return False
    if retry and shard.attempt < MAX_ATTEMPTS:
        return _settle(session, shard, RUNNING, QUEUED, error=error, finished_at=None)
    return _settle(session, shard, RUNNING, FAILED, error=error)


def summary(session: Session, run: Run) -> dict:
    """Counts by status, and the outputs of the shards done (in order)."""
    rows = session.scalars(
        select(RunShard).where(RunShard.run_id == run.id).order_by(RunShard.index)
    ).all()
    counts = dict.fromkeys((QUEUED, RUNNING, DONE, FAILED), 0)
    for row in rows:
        counts[row.status] += 1
    return {
        "counts": counts,
        "done": [
            {"index": r.index, "outputs": r.outputs} for r in rows if r.status == DONE
        ],  # fmt: skip
        "failed": [
            {"index": r.index, "error": r.error} for r in rows if r.status == FAILED
        ],  # fmt: skip
    }
