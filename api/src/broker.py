import time
import uuid

import httpx
from fastapi import HTTPException
from jose import JWTError, jwt

from .config import config


TOKEN_EXCHANGE_GRANT = "urn:ietf:params:oauth:grant-type:token-exchange"
ACCESS_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:access_token"


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
