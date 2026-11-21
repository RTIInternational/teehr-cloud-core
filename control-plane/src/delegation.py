"""Acting as a user after their request ends (ADR-0014, ADR-0018).

Two ways to get a user's refresh token, both on the control plane's jobs client:

- **Token exchange**, when a job starts: the caller's access token becomes a
  refresh token bound to their login session (it ends with that session).
- **A device flow**, for a personal token: the user approves in the browser, and
  the control plane gets an **offline** refresh token (it outlives the browser
  session; Keycloak won't issue offline tokens by exchange).

Either is kept sealed in a delegated session (``src/sessions.py``), never handed
out.

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
DEVICE_CODE = "urn:ietf:params:oauth:grant-type:device_code"
#: What a personal token's device flow asks for: an offline session.
OFFLINE_SCOPE = "openid offline_access"
ACCESS_TOKEN = "urn:ietf:params:oauth:token-type:access_token"
REFRESH_TOKEN = "urn:ietf:params:oauth:token-type:refresh_token"


class DelegationError(Exception):
    """Keycloak wouldn't exchange or refresh (session over, or misconfigured)."""


class DevicePending(Exception):
    """The user hasn't approved (or denied) the device flow yet; ``slow_down``:
    Keycloak asked for less frequent polls."""

    def __init__(self, slow_down: bool = False):
        super().__init__("authorization pending")
        self.slow_down = slow_down


@dataclass(frozen=True)
class DeviceStart:
    device_code: str
    user_code: str
    verification_uri: str
    verification_uri_complete: str
    interval: int
    expires_in: int


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

    def start_device(self, scope: str) -> DeviceStart:
        """Begin a device flow the user approves in their browser."""

    def poll_device(self, device_code: str) -> Tokens:
        """The tokens once approved; raises DevicePending until then, and
        DelegationError if denied or expired."""


class KeycloakDelegation:
    def __init__(self, settings: Settings):
        self._url = settings.keycloak_token_url
        self._device_url = settings.keycloak_token_url.removesuffix("/token") + (
            "/auth/device"
        )
        self._client = (
            (settings.exchange_client_id, settings.exchange_client_secret)
            if settings.exchange_client_id
            else (settings.client_id, settings.client_secret)
        )

    def _send(self, url: str, data: dict) -> httpx.Response:
        client_id, secret = self._client
        try:
            return httpx.post(
                url,
                data={**data, "client_id": client_id, "client_secret": secret},
                timeout=30,
            )
        except httpx.HTTPError as exc:
            raise DelegationError(f"Keycloak unreachable: {exc}") from exc

    def _post(self, data: dict) -> Tokens:
        resp = self._send(self._url, data)
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

    def start_device(self, scope: str) -> DeviceStart:
        resp = self._send(self._device_url, {"scope": scope})
        if resp.status_code != 200:
            raise DelegationError(
                f"Keycloak said {resp.status_code}: {resp.text[:200]}"
            )
        body = resp.json()
        return DeviceStart(
            device_code=body["device_code"],
            user_code=body["user_code"],
            verification_uri=body["verification_uri"],
            verification_uri_complete=body.get("verification_uri_complete")
            or body["verification_uri"],
            interval=int(body.get("interval", 5)),
            expires_in=int(body.get("expires_in", 600)),
        )

    def poll_device(self, device_code: str) -> Tokens:
        resp = self._send(
            self._url, {"grant_type": DEVICE_CODE, "device_code": device_code}
        )
        if resp.status_code == 400:
            error = resp.json().get("error") if resp.content else None
            if error in ("authorization_pending", "slow_down"):
                raise DevicePending(slow_down=error == "slow_down")
        if resp.status_code != 200:
            raise DelegationError(
                f"Keycloak said {resp.status_code}: {resp.text[:200]}"
            )
        body = resp.json()
        return Tokens(
            access_token=body["access_token"],
            expires_at=time.time() + int(body.get("expires_in", 60)),
            refresh_token=body["refresh_token"],
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
