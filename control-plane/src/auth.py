"""Caller identity from a Keycloak access token, or a personal token.

Same rules as ``api/src/auth.py`` (RS256, issuer check, `aud` or `azp` in an
allow-list, roles from ``realm_access.roles``) but its own code, so ``api/`` stays
untouched.

A personal token (``tpat_...``, ADR-0018) is resolved to an access token from its
delegated session, which is then checked the same way: the caller is its user,
with their roles, within the token's scopes and projects.
"""

import dataclasses
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import lru_cache
from typing import Annotated, Any

import jwt
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.orm import Session

from src import personal_tokens, sessions
from src.config import get_settings
from src.db import get_session
from src.delegation import (
    Delegation,
    DelegationError,
    Sealer,
    get_delegation,
    get_sealer,
)


@dataclass(frozen=True)
class Identity:
    subject: str
    username: str | None
    roles: frozenset[str]
    # The verified bearer token, so the API can act as the caller toward Polaris.
    token: str = field(default="", repr=False, compare=False)
    # Set when the caller used a personal token: its id and its session, which
    # jobs they start act through (an offline session can't be exchanged).
    personal_token_id: uuid.UUID | None = None
    session_id: uuid.UUID | None = None

    def has_any_role(self, roles: tuple[str, ...]) -> bool:
        return not self.roles.isdisjoint(roles)


def _unauthorized(detail: str = "Invalid access token") -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=detail,
        headers={"WWW-Authenticate": "Bearer"},
    )


class TokenVerifier:
    """Checks an access token and turns it into an :class:`Identity`.

    ``key_for_token`` finds the public key for a token (its ``kid``); in service it
    is a cached JWKS lookup, in tests a fixed key.
    """

    def __init__(
        self,
        issuer: str,
        allowed_audiences: tuple[str, ...],
        key_for_token: Callable[[str], Any],
    ):
        self._issuer = issuer
        self._audiences = allowed_audiences
        self._key_for_token = key_for_token

    def verify(self, token: str) -> Identity:
        try:
            key = self._key_for_token(token)
        except jwt.PyJWKClientConnectionError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Authentication provider temporarily unavailable",
            ) from exc
        except jwt.PyJWTError as exc:
            raise _unauthorized() from exc

        try:
            claims = jwt.decode(
                token,
                key,
                algorithms=["RS256"],
                issuer=self._issuer,
                options={"verify_aud": False, "require": ["exp", "iat", "sub"]},
            )
        except jwt.PyJWTError as exc:
            raise _unauthorized() from exc

        if self._audiences:
            aud = claims.get("aud") or []
            token_audiences = [aud] if isinstance(aud, str) else list(aud)
            if not (
                set(token_audiences) & set(self._audiences)
                or claims.get("azp") in self._audiences
            ):
                raise _unauthorized()

        roles = (claims.get("realm_access") or {}).get("roles") or []
        return Identity(
            subject=claims["sub"],
            username=claims.get("preferred_username"),
            roles=frozenset(str(r) for r in roles),
            token=token,
        )


@lru_cache
def get_verifier() -> TokenVerifier:
    settings = get_settings()
    if not (settings.keycloak_issuer_url and settings.keycloak_jwks_url):
        raise RuntimeError("KEYCLOAK_ISSUER_URL is not configured")
    client = jwt.PyJWKClient(
        settings.keycloak_jwks_url, cache_keys=True, lifespan=300, timeout=10
    )
    return TokenVerifier(
        settings.keycloak_issuer_url,
        settings.allowed_audiences,
        lambda token: client.get_signing_key_from_jwt(token).key,
    )


_bearer = HTTPBearer(
    auto_error=False,
    scheme_name="BearerAuth",
    description="Paste a Keycloak access token or a personal token (tpat_...)",
)
_READS = ("GET", "HEAD", "OPTIONS")


def get_identity(
    request: Request,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
    verifier: Annotated[TokenVerifier, Depends(get_verifier)],
    db: Annotated[Session, Depends(get_session)],
    delegation: Annotated[Delegation | None, Depends(get_delegation)],
    sealer: Annotated[Sealer | None, Depends(get_sealer)],
) -> Identity:
    """The signed-in caller. Every control-plane route needs one."""
    if credentials is None:
        raise _unauthorized("Not authenticated")
    secret = credentials.credentials
    if not secret.startswith(personal_tokens.PREFIX):
        return verifier.verify(secret)
    token = personal_tokens.by_secret(db, secret)
    if token is None:
        raise _unauthorized("Unknown, revoked or expired personal token")
    if delegation is None or sealer is None:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, "Personal tokens aren't configured"
        )
    token.last_used_at = datetime.now(UTC)
    try:
        access, _ = sessions.access_token(db, token.session, delegation, sealer)
    except DelegationError as exc:
        raise _unauthorized(
            "This personal token's session has ended; make a new token"
        ) from exc
    if "write" not in token.scopes and request.method not in _READS:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "This token is read-only")
    project = request.path_params.get("name")
    if (
        token.projects is not None
        and request.url.path.startswith("/v1/projects/")
        and project is not None
        and project not in token.projects
    ):
        raise HTTPException(
            status.HTTP_403_FORBIDDEN, f"This token isn't for project '{project}'"
        )
    return dataclasses.replace(
        verifier.verify(access), personal_token_id=token.id, session_id=token.session_id
    )
