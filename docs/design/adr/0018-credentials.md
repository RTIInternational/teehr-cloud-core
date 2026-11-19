# ADR-0018: Credentials: the control plane keeps delegated sessions; users get personal tokens and a CLI login

- **Status:** Proposed
- **Date:** 2026-11-18
- **Deciders:** TEEHR team
- **Related:** #148, ADR-0004 (the caller's identity), ADR-0014 (job grants), ADR-0017 (runs
  across pods), ADR-0011 (interface parity with VeriRace)

## Context

Three things act as a user without the user's browser in the loop, each built separately:

- **Job grants** (control plane, ADR-0014): the caller's token is exchanged for a refresh token
  bound to their login session, stored sealed on the job; each `/token` call refreshes it.
- **Delegated sessions** (legacy API broker, `/auth/polaris-token`): a user's refresh token,
  stored encrypted, refreshed on every request, for Spark in Jupyter.
- **API keys** (legacy API, `/auth/api-keys`): hashed keys, offered to admins only.

Neither delegation caches access tokens or serializes refreshes. Today that is harmless: the realm
doesn't rotate refresh tokens (`revokeRefreshToken` is off), so concurrent refreshes all succeed.
It becomes a problem if rotation is turned on, and costs a Keycloak call per request regardless;
runs across pods (ADR-0017) multiply those calls.

What's missing: every user creating tokens for scripts, CI and agents (not admins only), and a
command-line login (`teehr auth login`) that sends the user to the browser and back, as
VeriRace's does. Both are part of interface parity with VeriRace (ADR-0011). A token that works
after the browser is closed can't hang off a login session: a session-bound refresh token stops
when the session ends (jobs report "the caller's login session has ended" then).

The fork: one credentials model in the control plane, or a mechanism per use.

## Decision

**The control plane keeps delegated sessions: stored, sealed refresh tokens for a user, from which
it hands out short-lived access tokens, cached and refreshed under a lock. Job grants and personal
access tokens are secrets that map to one. The CLI logs in with the OAuth device flow and holds
its own tokens.**

- **Delegated sessions.** One table: the user, the kind (job, personal token), the sealed refresh
  token, the cached access token and its expiry, created / expires / revoked. An access token is
  served from the cache while it has more than a minute left; otherwise the session's row is
  locked, refreshed once, and the new tokens stored. Job grants move onto this.
- **Personal access tokens, for every user.** Any signed-in user creates, lists and revokes their
  own: a name, an expiry (capped), scopes (read, write), optionally limited to some projects. The
  secret (prefixed, e.g. `tpat_`) is shown once and stored hashed. Admins can list and revoke
  anyone's. A personal token's session is an **offline** one (Keycloak `offline_access`), so it
  outlives the browser session; the realm sets offline session limits to match the cap.
- **Using a personal token.** As a bearer token to the control plane's API, authenticated as its
  user within its scopes and projects. For direct catalog access (Spark, PyIceberg, DuckDB), the
  API exchanges it for a short-lived access token as the user (`teehr auth token`).
- **The CLI.** `teehr auth login` runs the device flow against a public Keycloak client with
  device grants enabled: it prints a code, opens the browser, and polls until the user approves.
  Tokens are kept per API URL in the user's config directory (readable by them only) and
  refreshed by the CLI, under a file lock so processes sharing them don't race.
  `teehr auth login --token` stores a personal token instead (CI); `teehr auth token` prints a
  current access token; `teehr tokens create|list|revoke` manage personal tokens.
- **Legacy pieces stay as they are** (the broker's delegated sessions, admin API keys) and are
  retired with direct Jupyter access and the legacy API, not migrated.
- **Order:** delegated sessions with job grants on them (before runs fan out widely), personal
  tokens for every user, then the CLI login.

## Consequences

- **Positive:** one place that holds refresh tokens, seals them, caches access tokens and can
  revoke them; turning on refresh-token rotation becomes safe. Every user can automate without an
  admin. Jobs, personal tokens and the CLI all act as the real user, so Polaris enforces their
  roles as everywhere else (ADR-0004).
- **Negative / cost:** offline sessions are long-lived credentials; they need caps, expiry,
  revocation and an audit trail (created, last used). The control plane is on the path of every
  personal-token request. Realm changes (offline access, a device-flow client, session limits).
- **Tradeoff accepted:** a personal token's scopes and projects bind at the control plane's API;
  an access token minted from it for direct catalog access carries the user's roles (see Notes).

## Alternatives considered

- **Keep a mechanism per use** (grants, broker sessions, API keys). Three places holding refresh
  tokens, none caching; personal tokens and the CLI would add more.
- **Keycloak impersonation** (the control plane mints tokens for any user with its own
  credentials). No stored refresh tokens, but a service that can become anyone; we keep acting as
  a user to tokens that user handed over.
- **Personal tokens as Keycloak service accounts, one per token.** Keycloak would hold them, but
  service accounts aren't the user: their roles would have to be copied and kept in step.
- **Per-pod grants and sessions for runs across pods** (VeriRace gives each worker a grant).
  Works without caching, but multiplies sessions per run; one cached session per run is simpler.

## Notes

- Spike first: Keycloak standard token exchange returning an **offline** refresh token
  (`scope=offline_access`, and the `enableRefreshRequestedTokenType` setting, now
  `SAME_SESSION`), or the personal-token page doing its own login with `offline_access`.
- Session-bound refresh tokens end at the realm's SSO session maximum (Keycloak's default is 10
  hours): a job longer than that loses its caller. Revisit the limit, or give long runs offline
  sessions, when runs get that long.
- Open: should a read-scoped personal token mint catalog tokens at all, since the minted token
  carries the user's full roles? Until downscoped tokens exist, only write-scoped personal tokens
  may.
- The frontend's admin API-keys page becomes everyone's personal-tokens page.
