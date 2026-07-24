from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from ..auth import (
    AuthIdentity,
    extract_bearer_token_from_request,
    get_admin_identity,
    get_authenticated_identity,
    get_request_identity,
)
from ..broker import exchange_token_for_polaris
from ..config import config

router = APIRouter(prefix="/auth", tags=["Auth"])


class ApiKeyCreateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    scopes: list[str] = Field(default_factory=list)


class PolarisTokenRequest(BaseModel):
    user_id: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    realm: str = Field(min_length=1)
    catalog: str = Field(default="iceberg", min_length=1)
    groups: list[str] = Field(default_factory=list)
    requested_ttl_seconds: int = Field(default=600, ge=1)
    audience: str = Field(default_factory=lambda: config.BROKER_TARGET_AUDIENCE)


class PolarisTokenIssuedFor(BaseModel):
    user_id: str
    session_id: str
    realm: str


class PolarisTokenResponse(BaseModel):
    access_token: str
    token_type: str
    expires_at_epoch_seconds: int
    expires_in_seconds: int
    issued_for: PolarisTokenIssuedFor
    trace_id: str


@router.get("/me")
async def me(identity: AuthIdentity = Depends(get_request_identity)):
    return {
        "subject": identity.subject,
        "auth_type": identity.auth_type,
        "roles": identity.roles,
        "scopes": identity.scopes,
        "authenticated": identity.is_authenticated,
    }


@router.get("/api-keys")
async def list_api_keys(
    request: Request,
    identity: AuthIdentity = Depends(get_admin_identity),
):
    keys = await request.app.state.api_key_store.list_keys()
    return {"items": keys}


@router.post("/api-keys", status_code=201)
async def create_api_key(
    request: Request,
    payload: ApiKeyCreateRequest,
    identity: AuthIdentity = Depends(get_admin_identity),
):
    created = await request.app.state.api_key_store.create_key(
        owner_sub=identity.subject,
        name=payload.name,
        scopes=payload.scopes,
    )
    return created


@router.delete("/api-keys/{key_id}", status_code=204)
async def revoke_api_key(
    key_id: str,
    request: Request,
    identity: AuthIdentity = Depends(get_admin_identity),
):
    revoked = await request.app.state.api_key_store.revoke_key(identity.subject, key_id)
    if not revoked:
        raise HTTPException(status_code=404, detail="API key not found")


@router.post("/polaris-token", response_model=PolarisTokenResponse)
async def polaris_token(
    request: Request,
    payload: PolarisTokenRequest,
    identity: AuthIdentity = Depends(get_authenticated_identity),
):
    if identity.auth_type != "jwt":
        raise HTTPException(status_code=403, detail="JWT identity required")

    allowed_user_ids = {identity.subject}
    if identity.preferred_username:
        allowed_user_ids.add(identity.preferred_username)

    if payload.user_id not in allowed_user_ids:
        raise HTTPException(
            status_code=403,
            detail="Requested user_id does not match authenticated identity",
        )

    bearer_token = extract_bearer_token_from_request(request)
    exchanged = await exchange_token_for_polaris(
        subject_token=bearer_token,
        audience=payload.audience,
        requested_ttl_seconds=payload.requested_ttl_seconds,
    )

    return PolarisTokenResponse(
        access_token=exchanged["access_token"],
        token_type=exchanged["token_type"],
        expires_at_epoch_seconds=exchanged["expires_at_epoch_seconds"],
        expires_in_seconds=exchanged["expires_in_seconds"],
        issued_for=PolarisTokenIssuedFor(
            user_id=payload.user_id,
            session_id=payload.session_id,
            realm=payload.realm,
        ),
        trace_id=exchanged["trace_id"],
    )
