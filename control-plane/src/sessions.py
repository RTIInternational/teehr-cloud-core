"""Delegated sessions: the control plane acting as a user (ADR-0018).

A session holds the user's refresh token, sealed, and the access token last minted
from it, sealed and cached. :func:`access_token` serves the cached one while it
has more than ``MARGIN`` left and refreshes otherwise. Refresh tokens aren't
rotated in the realm, so two refreshes racing both succeed and the later one is
kept: nothing locks.
"""

from datetime import UTC, datetime, timedelta

from sqlalchemy.orm import Session

from src.delegation import Delegation, DelegationError, Sealer, Tokens
from src.models import DelegatedSession

#: A cached access token is served while it has at least this long left.
MARGIN = timedelta(seconds=60)
JOB, PERSONAL = "job", "personal"


def _now() -> datetime:
    return datetime.now(UTC)


def _aware(t: datetime) -> datetime:
    return t.replace(tzinfo=t.tzinfo or UTC)


def open_session(
    sealer: Sealer,
    *,
    subject: str,
    kind: str,
    tokens: Tokens,
    expires_at: datetime | None = None,
) -> DelegatedSession:
    """A new session from tokens Keycloak just issued (not yet added)."""
    return DelegatedSession(
        subject=subject,
        kind=kind,
        refresh_token=sealer.seal(tokens.refresh_token),
        access_token=sealer.seal(tokens.access_token),
        access_expires_at=datetime.fromtimestamp(tokens.expires_at, UTC),
        expires_at=expires_at,
    )


def access_token(
    db: Session,
    session: DelegatedSession,
    delegation: Delegation,
    sealer: Sealer,
    fresh: bool = False,
) -> tuple[str, datetime]:
    """An access token as the session's user and when it expires: the cached one,
    unless it's near its end or ``fresh`` is asked for (e.g. the user's roles
    changed). Raises DelegationError if the session is over."""
    now = _now()
    if session.revoked_at is not None or not session.refresh_token:
        raise DelegationError("the session was revoked")
    if session.expires_at is not None and _aware(session.expires_at) <= now:
        raise DelegationError("the session has expired")
    cached = session.access_token and session.access_expires_at
    if cached and not fresh and _aware(session.access_expires_at) - now > MARGIN:
        token = sealer.open(session.access_token)
        expires = _aware(session.access_expires_at)
    else:
        tokens = delegation.refresh(sealer.open(session.refresh_token))
        token = tokens.access_token
        expires = datetime.fromtimestamp(tokens.expires_at, UTC)
        session.refresh_token = sealer.seal(tokens.refresh_token)
        session.access_token = sealer.seal(token)
        session.access_expires_at = expires
    session.last_used_at = now
    db.commit()
    return token, expires


def revoke(session: DelegatedSession) -> None:
    """End the session: its tokens are dropped and it can't be used again."""
    session.revoked_at = _now()
    session.refresh_token = None
    session.access_token = None
    session.access_expires_at = None
