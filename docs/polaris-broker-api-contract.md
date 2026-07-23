# Polaris Broker API Contract (Prototype v0)

Last updated: 2026-07-23

## Purpose

Define a minimal broker contract that allows Spark-side Iceberg AuthManager code to acquire and rotate short-lived Polaris bearer tokens without exposing refresh tokens in notebooks.

## Scope

This contract is intentionally narrow:

- one token mint endpoint for interactive notebook Spark sessions
- strict caller/session binding
- short-lived access tokens only
- no refresh token returned to Spark

## Endpoint

- Method: `POST`
- Path: `/v1/polaris/token`
- Authn: broker validates caller using cluster-local identity and request signature/session binding
- Content-Type: `application/json`

## Request Body

```json
{
  "user_id": "user@example.local",
  "session_id": "jupyter-6f9f99b5f9-l9z6k",
  "realm": "teehr",
  "catalog": "iceberg",
  "groups": ["iceberg-user", "hydrology-team"],
  "requested_ttl_seconds": 600,
  "audience": "polaris"
}
```

## Request Field Notes

- `user_id`: stable user identifier. Prefer immutable subject (`sub`) if available; username is acceptable for prototype.
- `session_id`: Jupyter notebook server identity (pod UID or equivalent) for caller binding and audit.
- `realm`: Polaris realm to target.
- `catalog`: optional catalog hint for policy/audit context.
- `groups`: optional hint for diagnostics only. Broker should derive trusted groups from validated identity when possible.
- `requested_ttl_seconds`: bounded by broker policy (for example 120 to 900).
- `audience`: should be constrained to Polaris data-plane usage.

## Success Response

- Status: `200 OK`

```json
{
  "access_token": "<jwt>",
  "token_type": "Bearer",
  "expires_at_epoch_seconds": 1785169492,
  "expires_in_seconds": 600,
  "issued_for": {
    "user_id": "user@example.local",
    "session_id": "jupyter-6f9f99b5f9-l9z6k",
    "realm": "teehr"
  },
  "trace_id": "f8af1e8b3d21469a9adf99c9185d2d10"
}
```

## Error Responses

- `400 Bad Request`: validation failures
- `401 Unauthorized`: caller identity not valid
- `403 Forbidden`: caller/session mismatch or policy denies issuance
- `429 Too Many Requests`: per-user/session throttle exceeded
- `500/502/503`: transient issuer/broker failure

Error payload shape:

```json
{
  "error": "forbidden",
  "message": "session is not authorized for requested user_id",
  "trace_id": "f8af1e8b3d21469a9adf99c9185d2d10"
}
```

## Security Requirements

- Broker must not trust notebook-supplied `groups` blindly.
- Broker must validate caller identity from trusted transport identity (mTLS, workload identity, signed upstream token, or equivalent).
- Broker must bind `user_id` to `session_id` and reject mismatches.
- Broker-issued tokens must be short-lived and audience-restricted.
- Broker credentials used for exchange/mint operations must never be exposed to notebook runtimes.

## Audit Requirements

At minimum, log:

- `trace_id`
- broker caller identity
- `user_id`
- `session_id`
- `realm`
- requested and granted TTL
- outcome (`issued`, `denied`, `error`)
- downstream issuer status/latency

## Spark AuthManager Expectations

The Spark-side AuthManager should:

- request new token on first use
- cache token in-memory
- proactively rotate before expiry (for example 60 seconds early)
- retry once on auth failure when token is near-expiry
- never persist tokens to logs or Spark history

## Non-goals (v0)

- multi-tenant broker routing policies
- user-delegated arbitrary scope requests
- long-lived refresh token distribution to Spark
- batch token mint APIs
