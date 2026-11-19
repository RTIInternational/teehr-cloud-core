# ADR-0018: Credentials: the control plane keeps delegated sessions; users get personal tokens and a CLI login

- **Status:** Accepted (ratified by the team, 2026-11-19)
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

Neither delegation caches access tokens: every request is a Keycloak refresh, and runs across
pods (ADR-0017) multiply those calls. Concurrent refreshes are safe because the realm doesn't
rotate refresh tokens (`revokeRefreshToken` is off: a used refresh token stays valid).

What's missing: every user creating tokens for scripts, CI and agents (not admins only), and a
command-line login (`teehr auth login`) that sends the user to the browser and back, as
VeriRace's does. Both are part of interface parity with VeriRace (ADR-0011). A token that works
after the browser is closed can't hang off a login session: a session-bound refresh token stops
when the session ends (jobs report "the caller's login session has ended" then).

The fork: one credentials model in the control plane, or a mechanism per use.

## Decision

**The control plane keeps delegated sessions: stored, sealed refresh tokens for a user, from which
it hands out short-lived access tokens, cached. Job grants and personal access tokens are secrets
that map to one. A personal token is made by a device flow the control plane runs, and the CLI
logs in by making one.**

**Assumption: refresh tokens aren't rotated** (`revokeRefreshToken` stays off in the realm). So
any number of pods or processes may refresh one session at once, and nothing here locks.

- **Delegated sessions.** One table: the user, the kind (job, personal token), the sealed refresh
  token, the cached access token and its expiry, created / expires / revoked. An access token is
  served from the cache while it has more than a minute left; otherwise the session is refreshed
  and the new access token cached (two refreshes racing both succeed; the later one is kept).
  Job grants move onto this.
- **Personal access tokens, for every user.** A name, an expiry (at most a year), scopes (read,
  write), optionally limited to some projects. The secret (prefixed `tpat_`) is shown once and
  stored hashed. Users list and revoke their own; admins can list and revoke anyone's.
- **Making one: a device flow the control plane runs.** A personal token's session must be an
  **offline** one (Keycloak `offline_access`), to outlive the browser session, and token
  exchange can't make one (Keycloak refuses `offline_access` in an exchange). So the control plane
  starts a device authorization on its jobs client with `offline_access` and returns the code and
  link. The user approves in the browser. The poller then receives the new token's secret, once,
  and the control plane keeps the offline session, sealed. Starting one needs no sign-in: who the
  token is for is whoever approves. The same flow serves the CLI, CI setup and `/docs`. The realm
  caps offline sessions at a year (idle and total).
- **Using a personal token.** As a bearer token to the control plane's API: the control plane
  serves an access token from the token's session (cached) and authenticates the request as that
  user, within the token's scopes and projects. Jobs started with a personal token act through
  its session (no exchange; revoking the token stops them). For direct catalog access (Spark,
  PyIceberg, DuckDB), `teehr auth token` gets a short-lived access token from the session.
- **The CLI.** `teehr auth login` makes a personal token named for the machine: it prints the
  code, opens the browser, and polls. It keeps only that token, per API URL, in the user's config
  directory (readable by them only); no Keycloak tokens on users' disks, and no CLI client in
  Keycloak. `teehr auth login --token` stores an existing token (CI); `teehr tokens
  create|list|revoke` manage them.
- **Legacy pieces stay as they are** (the broker's delegated sessions, admin API keys) and are
  retired with direct Jupyter access and the legacy API, not migrated. **The frontend doesn't
  change until then:** its admin API-keys page stays on the legacy API; personal tokens are
  created through the control plane's API (`/docs`) and the CLI.
- **Order:** delegated sessions with job grants on them, personal tokens and their device flow,
  then the CLI.

## Consequences

- **Positive:** one place that holds refresh tokens, seals them, caches access tokens and can
  revoke them. Every user can automate without an
  admin. Jobs, personal tokens and the CLI all act as the real user, so Polaris enforces their
  roles as everywhere else (ADR-0004).
- **Negative / cost:** offline sessions are long-lived credentials; they need caps, expiry,
  revocation and an audit trail (created, last used). The control plane is on the path of every
  personal-token request. Realm changes: the device grant on the jobs client, offline session
  limits. Anyone can start a device flow (no sign-in), so its starts are rate-limited.
- **Tradeoff accepted:** a personal token's scopes and projects bind at the control plane's API;
  an access token minted from it for direct catalog access carries the user's roles (see Notes).
  Refresh tokens stay unrotated, so a leaked one stays valid until its session ends or is
  revoked; sealing them and keeping them in the control plane only is the mitigation.

## Alternatives considered

- **Keep a mechanism per use** (grants, broker sessions, API keys). Three places holding refresh
  tokens, none caching; personal tokens and the CLI would add more.
- **Keycloak impersonation** (the control plane mints tokens for any user with its own
  credentials). No stored refresh tokens, but a service that can become anyone; we keep acting as
  a user to tokens that user handed over.
- **Personal tokens as Keycloak service accounts, one per token.** Keycloak would hold them, but
  service accounts aren't the user: their roles would have to be copied and kept in step.
- **The CLI holds its own Keycloak tokens** (a public device-flow client). An offline CLI
  session can't be exchanged to start a job (Keycloak refuses refresh tokens from offline
  sessions), and a session-bound one ends after the realm's idle timeout (30 minutes). It would
  also put long-lived Keycloak tokens on users' disks.
- **Make personal tokens by exchanging the caller's token.** Keycloak won't issue offline tokens
  by exchange.
- **Per-pod grants and sessions for runs across pods** (VeriRace gives each worker a grant).
  Works without caching, but multiplies sessions per run; one cached session per run is simpler.

## Notes

- Spike (KinD, Keycloak 26.8, 2026-11-19): a device flow on the jobs client with
  `scope=openid offline_access` gives an `Offline` refresh token expiring at the realm's one-year
  cap; the control plane refreshes it with its client credentials (still `Offline`). The access
  tokens carry the user's realm roles, `aud` `account` (Polaris's) and `azp`
  `teehr-control-plane-jobs` (the control plane's allowed list): no audience mapper needed.
- Session-bound refresh tokens end at the realm's SSO session maximum (Keycloak's default is 10
  hours): a job longer than that loses its caller. Revisit the limit, or give long runs offline
  sessions, when runs get that long.
- Open: should a read-scoped personal token mint catalog tokens at all, since the minted token
  carries the user's full roles? Until downscoped tokens exist, only write-scoped personal tokens
  may.
- If refresh-token rotation is ever turned on, sessions need a lock (refresh once, under a row
  lock) and the CLI a file lock; revisit this ADR then.
- When the legacy API is retired, the frontend's API-keys page moves to personal tokens.
