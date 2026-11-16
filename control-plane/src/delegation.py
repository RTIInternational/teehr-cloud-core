"""Acting as the caller after their request ends (ADR-0014).

When a job starts, the caller's access token is exchanged with Keycloak (standard
token exchange) for a refresh token bound to their login session. It's stored
encrypted with the job, and the control plane uses it to give the job's pod fresh
access tokens as the caller. The pod never holds it. It stops working when the
caller's session ends (or they're logged out).

Routes and jobs depend on :func:`get_delegation`, which tests replace.
"""

import base64
import hashlib
import time
from dataclasses import dataclass
from functools import lru_cache
from typing import Protocol

import httpx
from cryptography.fernet import Fernet, InvalidToken

from src.config import Settings, get_settings

TOKEN_EXCHANGE = "urn:ietf:params:oauth:grant-type:token-exchange"
ACCESS_TOKEN = "urn:ietf:params:oauth:token-type:access_token"
REFRESH_TOKEN = "urn:ietf:params:oauth:token-type:refresh_token"


class DelegationError(Exception):
    """Keycloak wouldn't exchange or refresh (session over, or misconfigured)."""


@dataclass(frozen=True)
class Tokens:
    access_token: str
    expires_at: float
    refresh_token: str


class Delegation(Protocol):
    def exchange(self, user_token: str) -> Tokens:
        """The caller's access token -> tokens bound to their login session."""

    def refresh(self, refresh_token: str) -> Tokens:
        """A fresh access token (and possibly a new refresh token)."""


class KeycloakDelegation:
    def __init__(self, settings: Settings):
        self._url = settings.keycloak_token_url
        self._client = (
            (settings.exchange_client_id, settings.exchange_client_secret)
            if settings.exchange_client_id
            else (settings.client_id, settings.client_secret)
        )

    def _post(self, data: dict) -> Tokens:
        client_id, secret = self._client
        try:
            resp = httpx.post(
                self._url,
                data={**data, "client_id": client_id, "client_secret": secret},
                timeout=30,
            )
        except httpx.HTTPError as exc:
            raise DelegationError(f"Keycloak unreachable: {exc}") from exc
        if resp.status_code != 200:
            raise DelegationError(
                f"Keycloak said {resp.status_code}: {resp.text[:200]}"
            )
        body = resp.json()
        return Tokens(
            access_token=body["access_token"],
            expires_at=time.time() + int(body.get("expires_in", 60)),
            refresh_token=body.get("refresh_token") or data.get("refresh_token", ""),
        )

    def exchange(self, user_token: str) -> Tokens:
        tokens = self._post(
            {
                "grant_type": TOKEN_EXCHANGE,
                "subject_token": user_token,
                "subject_token_type": ACCESS_TOKEN,
                "requested_token_type": REFRESH_TOKEN,
            }
        )
        if not tokens.refresh_token:
            raise DelegationError("Keycloak returned no refresh token")
        return tokens

    def refresh(self, refresh_token: str) -> Tokens:
        return self._post(
            {"grant_type": "refresh_token", "refresh_token": refresh_token}
        )


class Sealer:
    """Encrypts refresh tokens at rest (Fernet; key derived from a secret)."""

    def __init__(self, secret: str):
        key = base64.urlsafe_b64encode(hashlib.sha256(secret.encode()).digest())
        self._fernet = Fernet(key)

    def seal(self, value: str) -> str:
        return self._fernet.encrypt(value.encode()).decode()

    def open(self, sealed: str) -> str:
        try:
            return self._fernet.decrypt(sealed.encode()).decode()
        except InvalidToken as exc:
            raise DelegationError("stored token can't be decrypted") from exc


@lru_cache
def get_delegation() -> Delegation | None:
    """Token exchange with Keycloak, or None when it isn't configured."""
    settings = get_settings()
    client = (
        (settings.exchange_client_id, settings.exchange_client_secret)
        if settings.exchange_client_id
        else (settings.client_id, settings.client_secret)
    )
    if not (settings.keycloak_token_url and all(client)):
        return None
    return KeycloakDelegation(settings)


@lru_cache
def get_sealer() -> Sealer | None:
    settings = get_settings()
    return Sealer(settings.job_token_key) if settings.job_token_key else None
