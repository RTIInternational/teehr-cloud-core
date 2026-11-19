"""Personal tokens (ADR-0018): bearer secrets for scripts, CI and the CLI.

Made by a device flow the control plane runs on its jobs client: :func:`start`
asks Keycloak for an **offline** session and returns the code the user approves in
their browser, with a sealed handle (the flow's state, so nothing is stored until
it's approved); :func:`finish` polls with the handle and, once approved, keeps the
offline session sealed and returns the new token's secret, once. Whoever approves
is who the token is for, so starting a flow needs no sign-in.

A token's secret is stored hashed. It acts as its user through its session,
within its scopes (``read``, ``write``) and, if set, its projects.
"""

import hashlib
import json
import secrets
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.delegation import OFFLINE_SCOPE, Delegation, DelegationError, Sealer
from src.models import PersonalToken
from src.sessions import PERSONAL, open_session, revoke

PREFIX = "tpat_"
SCOPES = ("read", "write")
DEFAULT_DAYS = 90
#: The realm caps offline sessions at a year too (keycloak-bootstrap).
MAX_DAYS = 365


class FlowFailed(Exception):
    """The device flow can't finish: denied, expired, or not one of ours."""


def secret_hash(secret: str) -> str:
    return hashlib.sha256(secret.encode()).hexdigest()


def start(
    delegation: Delegation,
    sealer: Sealer,
    *,
    name: str,
    scopes: list[str],
    projects: list[str] | None,
    days: int,
) -> dict:
    """Begin making a token: the code and link for the user, and the handle to
    poll with."""
    flow = delegation.start_device(OFFLINE_SCOPE)
    state = {"device_code": flow.device_code, "name": name, "scopes": scopes,
             "projects": projects, "days": days,
             "until": time.time() + flow.expires_in}  # fmt: skip
    return {
        "user_code": flow.user_code,
        "verification_uri": flow.verification_uri,
        "verification_uri_complete": flow.verification_uri_complete,
        "interval": flow.interval,
        "expires_in": flow.expires_in,
        "poll": sealer.seal(json.dumps(state)),
    }


def finish(
    db: Session,
    delegation: Delegation,
    sealer: Sealer,
    verify: Callable,
    handle: str,
) -> tuple[PersonalToken, str]:
    """The new token and its secret, once the user has approved. Raises
    DevicePending until then, FlowFailed if it can't finish."""
    try:
        state = json.loads(sealer.open(handle))
    except (DelegationError, ValueError):
        raise FlowFailed("not a sign-in this control plane started") from None
    if state["until"] < time.time():
        raise FlowFailed("the sign-in expired; start again")
    try:
        tokens = delegation.poll_device(state["device_code"])
    except DelegationError:
        raise FlowFailed("the sign-in was denied, expired or already used") from None
    who = verify(tokens.access_token)
    expires = datetime.now(UTC) + timedelta(days=state["days"])
    secret = PREFIX + secrets.token_urlsafe(32)
    token = PersonalToken(
        name=state["name"],
        subject=who.subject,
        username=who.username,
        token_hash=secret_hash(secret),
        hint=secret[: len(PREFIX) + 4],
        scopes=state["scopes"],
        projects=state["projects"],
        session=open_session(
            sealer,
            subject=who.subject,
            kind=PERSONAL,
            tokens=tokens,
            expires_at=expires,
        ),  # fmt: skip
        expires_at=expires,
    )
    db.add(token)
    db.commit()
    return token, secret


def by_secret(db: Session, secret: str) -> PersonalToken | None:
    """The live token a secret is for (not revoked or expired), or None."""
    if not secret.startswith(PREFIX):
        return None
    token = db.scalar(
        select(PersonalToken).where(PersonalToken.token_hash == secret_hash(secret))
    )
    if token is None or token.revoked_at is not None:
        return None
    if token.expires_at.replace(tzinfo=token.expires_at.tzinfo or UTC) <= (
        datetime.now(UTC)
    ):
        return None
    return token


def revoke_token(token: PersonalToken) -> None:
    """The token stops working, and its session with it (jobs it started too)."""
    token.revoked_at = datetime.now(UTC)
    revoke(token.session)
