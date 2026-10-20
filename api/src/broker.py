import time
import uuid
from asyncio import Lock

import httpx
from fastapi import HTTPException
from jose import JWTError, jwt

from .config import config
from .delegated_session_store import DelegatedSessionStore


TOKEN_EXCHANGE_GRANT = "urn:ietf:params:oauth:grant-type:token-exchange"
ACCESS_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:access_token"


_DELEGATED_SESSION_STORE: DelegatedSessionStore | None = None
_DELEGATED_SESSION_STORE_LOCK = Lock()


async def set_delegated_session_store(store: DelegatedSessionStore):
    async with _DELEGATED_SESSION_STORE_LOCK:
        global _DELEGATED_SESSION_STORE
        _DELEGATED_SESSION_STORE = store


async def _get_delegated_session_store() -> DelegatedSessionStore:
    async with _DELEGATED_SESSION_STORE_LOCK:
        if _DELEGATED_SESSION_STORE is None:
            raise HTTPException(status_code=503, detail="Delegated session store unavailable")
        return _DELEGATED_SESSION_STORE


def clamp_requested_ttl(requested_ttl_seconds: int | None) -> int:
    requested = requested_ttl_seconds or config.BROKER_MAX_TTL_SECONDS
    return max(config.BROKER_MIN_TTL_SECONDS, min(requested, config.BROKER_MAX_TTL_SECONDS))


async def exchange_token_for_polaris(
    *,
    subject_token: str,
    audience: str,
    requested_ttl_seconds: int | None,
) -> dict:
    if not config.BROKER_TOKEN_EXCHANGE_ENABLED:
        raise HTTPException(status_code=503, detail="Token broker is disabled")

    trace_id = uuid.uuid4().hex
    clamped_ttl = clamp_requested_ttl(requested_ttl_seconds)

    payload = {
        "grant_type": TOKEN_EXCHANGE_GRANT,
        "client_id": config.BROKER_OAUTH_CLIENT_ID,
        "subject_token": subject_token,
        "subject_token_type": ACCESS_TOKEN_TYPE,
        "requested_token_type": ACCESS_TOKEN_TYPE,
        "audience": audience,
        "scope": config.BROKER_DEFAULT_SCOPE,
    }
    if config.BROKER_OAUTH_CLIENT_SECRET:
        payload["client_secret"] = config.BROKER_OAUTH_CLIENT_SECRET

    try:
        async with httpx.AsyncClient(timeout=config.BROKER_REQUEST_TIMEOUT_SECONDS) as client:
            response = await client.post(config.BROKER_TOKEN_ENDPOINT, data=payload)

        if response.status_code >= 400:
            upstream_error = None
            upstream_error_description = None
            try:
                upstream_payload = response.json()
                upstream_error = upstream_payload.get("error")
                upstream_error_description = upstream_payload.get("error_description")
            except ValueError:
                upstream_error_description = response.text[:500]

            raise HTTPException(
                status_code=502,
                detail={
                    "error": "token_exchange_failed",
                    "message": "Broker failed to exchange token with identity provider",
                    "trace_id": trace_id,
                    "upstream_status": response.status_code,
                    "upstream_error": upstream_error,
                    "upstream_error_description": upstream_error_description,
                },
            )

        token_payload = response.json()
        access_token = token_payload.get("access_token")
        if not access_token:
            raise HTTPException(
                status_code=502,
                detail={
                    "error": "invalid_upstream_response",
                    "message": "Identity provider did not return access_token",
                    "trace_id": trace_id,
                },
            )

        expires_in = int(token_payload.get("expires_in", clamped_ttl))
        expires_at_epoch_seconds = int(time.time()) + max(expires_in, 1)

        # Prefer JWT exp claim when present.
        try:
            claims = jwt.get_unverified_claims(access_token)
            exp = int(claims.get("exp", 0))
            if exp > 0:
                expires_at_epoch_seconds = exp
        except (JWTError, ValueError, TypeError):
            pass

        return {
            "access_token": access_token,
            "token_type": token_payload.get("token_type", "Bearer"),
            "expires_in_seconds": max(expires_at_epoch_seconds - int(time.time()), 1),
            "expires_at_epoch_seconds": expires_at_epoch_seconds,
            "trace_id": trace_id,
        }
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "broker_connectivity_error",
                "message": "Unable to contact identity provider for token exchange",
                "trace_id": trace_id,
            },
        ) from exc


async def _refresh_subject_access_token(refresh_token: str) -> tuple[str, str | None]:
    payload = {
        "grant_type": "refresh_token",
        "client_id": config.BROKER_SUBJECT_CLIENT_ID,
        "refresh_token": refresh_token,
    }
    if config.BROKER_SUBJECT_CLIENT_SECRET:
        payload["client_secret"] = config.BROKER_SUBJECT_CLIENT_SECRET

    async with httpx.AsyncClient(timeout=config.BROKER_REQUEST_TIMEOUT_SECONDS) as client:
        response = await client.post(config.BROKER_TOKEN_ENDPOINT, data=payload)

    if response.status_code >= 400:
        try:
            upstream_payload = response.json()
            detail = upstream_payload.get("error_description") or upstream_payload.get("error")
        except ValueError:
            detail = response.text[:500]
        raise HTTPException(
            status_code=401,
            detail={
                "error": "subject_refresh_failed",
                "message": "Unable to refresh delegated subject token",
                "upstream_status": response.status_code,
                "upstream_detail": detail,
            },
        )

    token_payload = response.json()
    access_token = token_payload.get("access_token")
    if not access_token:
        raise HTTPException(
            status_code=502,
            detail={
                "error": "invalid_upstream_response",
                "message": "Identity provider did not return refreshed access_token",
            },
        )

    return access_token, token_payload.get("refresh_token")


async def create_delegated_broker_session(
    *,
    subject: str,
    user_id: str,
    session_id: str,
    realm: str,
    catalog: str,
    audience: str,
    refresh_token: str,
) -> dict:
    if not refresh_token:
        raise HTTPException(status_code=400, detail="refresh_token is required")

    now = int(time.time())
    ttl = max(config.BROKER_DELEGATED_SESSION_TTL_SECONDS, 300)
    expires_at = now + ttl
    delegated_session_id = uuid.uuid4().hex

    claims = {
        "sid": delegated_session_id,
        "sub": subject,
        "uid": user_id,
        "exp": expires_at,
        "iat": now,
        "typ": "teehr-broker-session",
    }

    broker_session_token = jwt.encode(
        claims,
        config.BROKER_SESSION_SIGNING_SECRET,
        algorithm="HS256",
    )

    store = await _get_delegated_session_store()
    await store.put_session(
        sid=delegated_session_id,
        subject=subject,
        user_id=user_id,
        session_id=session_id,
        realm=realm,
        catalog=catalog,
        audience=audience,
        refresh_token=refresh_token,
        expires_at_epoch_seconds=expires_at,
    )

    return {
        "broker_session_token": broker_session_token,
        "expires_at_epoch_seconds": expires_at,
    }


async def exchange_token_for_polaris_via_broker_session(
    *,
    broker_session_token: str,
    user_id: str,
    session_id: str,
    realm: str,
    requested_ttl_seconds: int | None,
) -> dict:
    if not broker_session_token:
        raise HTTPException(status_code=401, detail="broker session token required")

    try:
        claims = jwt.decode(
            broker_session_token,
            config.BROKER_SESSION_SIGNING_SECRET,
            algorithms=["HS256"],
        )
    except JWTError as exc:
        raise HTTPException(status_code=401, detail="Invalid broker session token") from exc

    delegated_session_id = claims.get("sid")
    if not delegated_session_id:
        raise HTTPException(status_code=401, detail="Invalid broker session token")

    store = await _get_delegated_session_store()
    record = await store.get_session(delegated_session_id)

    if not record:
        raise HTTPException(status_code=401, detail="Delegated broker session not found")

    if int(record.get("expires_at", 0)) <= int(time.time()):
        await store.delete_session(delegated_session_id)
        raise HTTPException(status_code=401, detail="Delegated broker session expired")

    if (
        record.get("user_id") != user_id
        or record.get("session_id") != session_id
        or record.get("realm") != realm
    ):
        raise HTTPException(status_code=403, detail="Session identity mismatch")

    refreshed_subject_token, maybe_new_refresh_token = await _refresh_subject_access_token(
        record["refresh_token"]
    )

    if maybe_new_refresh_token:
        await store.update_refresh_token(delegated_session_id, maybe_new_refresh_token)

    # Return the refreshed subject token directly — it preserves the user's
    # group claims and is accepted by Polaris for per-user permission enforcement.
    # Token exchange with a different audience would strip group claims and
    # prevent Polaris from mapping the user to their correct principal roles.
    trace_id = uuid.uuid4().hex
    expires_at_epoch_seconds = int(time.time()) + clamp_requested_ttl(requested_ttl_seconds)
    try:
        token_claims = jwt.get_unverified_claims(refreshed_subject_token)
        exp = int(token_claims.get("exp", 0))
        if exp > 0:
            expires_at_epoch_seconds = exp
    except (JWTError, ValueError, TypeError):
        pass

    return {
        "access_token": refreshed_subject_token,
        "token_type": "Bearer",
        "expires_in_seconds": max(expires_at_epoch_seconds - int(time.time()), 1),
        "expires_at_epoch_seconds": expires_at_epoch_seconds,
        "trace_id": trace_id,
    }
