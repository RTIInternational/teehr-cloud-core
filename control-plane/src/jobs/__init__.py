"""Jobs the API starts, run as Kubernetes Jobs that act as their caller (ADR-0014).

The control plane side: start a job (exchange the caller's token, record the job
with a grant, create its Kubernetes Job), find a job by its grant, bring a job's
state up to date with its Kubernetes Job, and cancel it. The pod side is
``python -m src.jobs`` (``__main__.py``).
"""

import hashlib
import secrets
from datetime import UTC, datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.config import Settings
from src.delegation import Delegation, Sealer
from src.kube import Kube, job_manifest, job_state
from src.models import Job, JobStatus, Project
from src.sessions import JOB, open_session, revoke

GRANT_PREFIX = "tjg_"
GRANT_TTL = timedelta(hours=24)
ACTIVE = (JobStatus.QUEUED, JobStatus.RUNNING)


class JobRefused(Exception):
    """The job can't be started now (e.g. too many running in the project)."""


def grant_hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def start_job(
    session: Session,
    settings: Settings,
    kube: Kube,
    delegation: Delegation,
    sealer: Sealer,
    *,
    kind: str,
    project: Project,
    caller: str,
    caller_token: str,
) -> Job:
    """Record and start a job acting as the caller. The caller's token is
    exchanged first, so a job never starts without a way to act as them.
    Raises DelegationError, KubeError, JobRefused."""
    active = session.scalar(
        select(func.count())
        .select_from(Job)
        .where(Job.project_id == project.id, Job.status.in_(ACTIVE))
    )
    if (active or 0) >= settings.max_active_jobs_per_project:
        raise JobRefused(
            f"'{project.name}' already has {active} jobs running; try again when "
            "one finishes"
        )
    tokens = delegation.exchange(caller_token)
    grant = GRANT_PREFIX + secrets.token_urlsafe(32)
    expires = datetime.now(UTC) + GRANT_TTL
    job = Job(
        kind=kind,
        project_id=project.id,
        created_by=caller,
        grant_hash=grant_hash(grant),
        grant_expires_at=expires,
        session=open_session(
            sealer, subject=caller, kind=JOB, tokens=tokens, expires_at=expires
        ),
    )
    session.add(job)
    session.flush()
    job.k8s_name = kube.create_job(job_manifest(settings, kind, str(job.id), grant))
    return job


def by_grant(session: Session, token: str) -> Job | None:
    """The active job a grant is for, or None (unknown, ended or expired)."""
    if not token.startswith(GRANT_PREFIX):
        return None
    job = session.scalar(select(Job).where(Job.grant_hash == grant_hash(token)))
    if job is None or job.status not in ACTIVE:
        return None
    if job.grant_expires_at.replace(tzinfo=job.grant_expires_at.tzinfo or UTC) <= (
        datetime.now(UTC)
    ):
        return None
    return job


def end(job: Job, status: JobStatus, error: str | None = None) -> None:
    """Mark a job ended; its grant stops working, and its own session (not one it
    shares, such as a personal token's) is revoked."""
    job.status = status
    job.error = error
    job.finished_at = datetime.now(UTC)
    if job.session is not None and job.session.kind == JOB:
        revoke(job.session)


def reconcile(kube: Kube | None, job: Job) -> bool:
    """Fail an active job whose pod is gone or died without reporting. Returns
    whether it changed."""
    if kube is None or job.status not in ACTIVE or not job.k8s_name:
        return False
    state = job_state(kube, job.k8s_name)
    if not state.ended:
        return False
    reason = state.reason or "its pod ended without reporting a result"
    end(job, JobStatus.FAILED, f"The job stopped: {reason}")
    return True


def cancel(kube: Kube, job: Job) -> None:
    if job.k8s_name:
        kube.delete_job(job.k8s_name)
    end(job, JobStatus.CANCELLED)
