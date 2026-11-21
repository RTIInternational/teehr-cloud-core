"""Personal tokens (ADR-0018): made by a device flow, listed and revoked.

``POST /v1/tokens/device`` starts a sign-in the user approves in their browser;
``POST /v1/tokens/device/poll`` returns the new token's secret once they have.
Neither needs a sign-in (whoever approves is who the token is for); starts are
rate-limited per client. ``teehr auth login`` is this flow.
"""

import threading
import time
import uuid
from collections import deque
from datetime import datetime
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from pydantic import BaseModel, Field
from sqlalchemy import select

from src import personal_tokens
from src.auth import TokenVerifier, get_verifier
from src.delegation import (
    Delegation,
    DelegationError,
    DevicePending,
    Sealer,
    get_delegation,
    get_sealer,
)
from src.models import PersonalToken
from src.routes.projects import IdentityDep, SessionDep, SettingsDep
from src.schemas import PROJECT_NAME_PATTERN, ErrorOut, ValidationErrorOut

router = APIRouter(prefix="/v1/tokens", tags=["tokens"])

DelegationDep = Annotated[Delegation | None, Depends(get_delegation)]
SealerDep = Annotated[Sealer | None, Depends(get_sealer)]

#: Device-flow starts allowed per client address per minute.
STARTS_PER_MINUTE = 10
_starts: dict[str, deque] = {}
_starts_lock = threading.Lock()


def _errors(**codes: str) -> dict:
    """OpenAPI ``responses``: these error codes (``e401="..."``) as problem
    details, and the 422 every route can return."""
    out: dict = {int(k[1:]): {"model": ErrorOut, "description": v}
                 for k, v in codes.items()}  # fmt: skip
    out[422] = {"model": ValidationErrorOut, "description": "Invalid request"}
    return out


SIGNED_IN = {"e401": "Missing or invalid access token or personal token"}
CONFIGURED = {"e503": "Personal tokens aren't configured"}


def _limit(request: Request) -> None:
    who = request.client.host if request.client else "unknown"
    now = time.monotonic()
    with _starts_lock:
        recent = _starts.setdefault(who, deque())
        while recent and now - recent[0] > 60:
            recent.popleft()
        if len(recent) >= STARTS_PER_MINUTE:
            raise HTTPException(
                status.HTTP_429_TOO_MANY_REQUESTS,
                "Too many sign-ins started; try again in a minute",
            )
        recent.append(now)


def _configured(delegation: Delegation | None, sealer: Sealer | None):
    if delegation is None or sealer is None:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, "Personal tokens aren't configured"
        )
    return delegation, sealer


class TokenRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100, examples=["laptop"])
    scopes: list[Literal["read", "write"]] = Field(
        default=["read", "write"], min_length=1
    )
    projects: list[Annotated[str, Field(pattern=PROJECT_NAME_PATTERN)]] | None = Field(
        default=None,
        max_length=100,
        description="Only these projects (default: every project you can use)",
    )
    expires_in_days: int = Field(
        default=personal_tokens.DEFAULT_DAYS, ge=1, le=personal_tokens.MAX_DAYS
    )


class DeviceFlowOut(BaseModel):
    user_code: str
    verification_uri: str
    verification_uri_complete: str = Field(description="Open this to approve")
    interval: int = Field(description="Seconds between polls")
    expires_in: int
    poll: str = Field(description="Send to /v1/tokens/device/poll")


class Poll(BaseModel):
    poll: str = Field(max_length=8000)


class TokenOut(BaseModel):
    id: uuid.UUID
    name: str
    username: str | None
    hint: str = Field(description="The start of the secret")
    scopes: list[str]
    projects: list[str] | None
    created_at: datetime
    expires_at: datetime
    last_used_at: datetime | None
    revoked_at: datetime | None
    current: bool = Field(default=False, description="The token this request used")


class TokenCreated(TokenOut):
    token: str = Field(description="The secret: shown this once")


def _out(row: PersonalToken, model=TokenOut, **extra) -> TokenOut:
    fields = {k: getattr(row, k) for k in TokenOut.model_fields if k != "current"}
    return model(**fields, **extra)


@router.post(
    "/device",
    operation_id="start_token_sign_in",
    responses=_errors(
        e429="Too many sign-ins started from here; try again in a minute",
        e502="Keycloak wouldn't start a sign-in",
        **CONFIGURED,
    ),
)
def start_sign_in(
    body: TokenRequest,
    request: Request,
    delegation: DelegationDep,
    sealer: SealerDep,
) -> DeviceFlowOut:
    """Start making a personal token: open ``verification_uri_complete`` and
    approve, then poll with ``poll``. No sign-in needed; the token is for whoever
    approves."""
    _limit(request)
    delegation, sealer = _configured(delegation, sealer)
    try:
        flow = personal_tokens.start(
            delegation, sealer, name=body.name, scopes=sorted(set(body.scopes)),
            projects=body.projects, days=body.expires_in_days,
        )  # fmt: skip
    except DelegationError as exc:
        raise HTTPException(
            status.HTTP_502_BAD_GATEWAY, "Keycloak wouldn't start a sign-in"
        ) from exc
    return DeviceFlowOut(**flow)


@router.post(
    "/device/poll",
    operation_id="finish_token_sign_in",
    responses={
        202: {"description": "Not approved yet; poll again"},
        **_errors(
            e400="The sign-in was denied, expired, already used or not ours",
            **CONFIGURED,
        ),
    },
)
def finish_sign_in(
    body: Poll,
    response: Response,
    session: SessionDep,
    delegation: DelegationDep,
    sealer: SealerDep,
    verifier: Annotated[TokenVerifier, Depends(get_verifier)],
) -> TokenCreated | dict:
    """The new token, once the sign-in is approved (its secret is shown once);
    202 until then."""
    delegation, sealer = _configured(delegation, sealer)
    try:
        token, secret = personal_tokens.finish(
            session, delegation, sealer, verifier.verify, body.poll
        )
    except DevicePending as pending:
        response.status_code = status.HTTP_202_ACCEPTED
        return {"status": "pending", "slow_down": pending.slow_down}
    except personal_tokens.FlowFailed as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc
    return _out(token, TokenCreated, token=secret)


@router.get(
    "",
    operation_id="list_tokens",
    responses=_errors(**SIGNED_IN, e403="all=true is for admins"),
)
def list_tokens(
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
    all: bool = False,
) -> list[TokenOut]:
    """Your personal tokens (``all``: everyone's, for admins), newest first."""
    query = select(PersonalToken).order_by(PersonalToken.created_at.desc())
    if all:
        if not identity.has_any_role(settings.admin_roles):
            raise HTTPException(status.HTTP_403_FORBIDDEN, "Admins only")
    else:
        query = query.where(PersonalToken.subject == identity.subject)
    return [
        _out(t, current=t.id == identity.personal_token_id)
        for t in session.scalars(query)
    ]


@router.delete(
    "/{token_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    operation_id="revoke_token",
    responses=_errors(**SIGNED_IN, e404="No such token (or not yours)"),
)
def revoke_token(
    token_id: uuid.UUID,
    session: SessionDep,
    identity: IdentityDep,
    settings: SettingsDep,
) -> None:
    """Revoke a token (yours, or anyone's for admins). Jobs it started stop being
    able to act as you."""
    token = session.get(PersonalToken, token_id)
    if token is None or (
        token.subject != identity.subject
        and not identity.has_any_role(settings.admin_roles)
    ):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "No such token")
    if token.revoked_at is None:
        personal_tokens.revoke_token(token)
        session.commit()
