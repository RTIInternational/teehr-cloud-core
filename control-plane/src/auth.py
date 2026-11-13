"""Caller identity from a Keycloak access token.

Same rules as ``api/src/auth.py`` (RS256, issuer check, `aud` or `azp` in an
allow-list, roles from ``realm_access.roles``) but its own code, so ``api/`` stays
untouched.
"""

from collections.abc import Callable
from dataclasses import dataclass
from functools import lru_cache
from typing import Annotated, Any

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from src.config import get_settings


@dataclass(frozen=True)
class Identity:
    subject: str
    username: str | None
    roles: frozenset[str]

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
    description="Paste a Keycloak access token",
)


def get_identity(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
    verifier: Annotated[TokenVerifier, Depends(get_verifier)],
) -> Identity:
    """The signed-in caller. Every control-plane route needs one."""
    if credentials is None:
        raise _unauthorized("Not authenticated")
    return verifier.verify(credentials.credentials)
