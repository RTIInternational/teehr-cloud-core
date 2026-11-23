# ADR-0020: Project membership is the one source of access; service accounts are members

- **Status:** Accepted
- **Date:** 2026-11-23
- **Deciders:** TEEHR team
- **Related:** #266, #167 (access model review), #206 (remote gates), ADR-0001 (system
  projects), ADR-0004 (the API as the access path), ADR-0014 (jobs act as their caller),
  ADR-0018 (credentials), ADR-0019 (platform producers)

## Context

Who may do what in a project is decided in more than one place, and the places disagree:

| Who | Access from | Checked by |
| --- | --- | --- |
| Project members | the membership table, turned into Keycloak project roles and Polaris grants | API and Polaris |
| Platform admins | `control-plane-admin` realm role | API |
| System-project writers | `teehr-platform-write` realm role, given the projects' write catalog roles (`SYSTEM_PROJECT_WRITER_ROLES`) | Polaris only |
| System-project readers | `teehr-read-only` (`SYSTEM_PROJECT_READER_ROLES`) in Polaris; any signed-in user in the API | each on its own |

So the v1 Prefect flows, which act as `prefect-polaris` (holding `teehr-platform-write`), can
write `platform`'s tables in Polaris but can't start a run in `platform` through the API. A
scheduled flow that runs the platform's evaluations needed a person-like workaround: a Keycloak
user made a member, signed in through a browser once a year for a personal token (ADR-0018),
kept in a secret.

The way out is for automation to be a member like anyone else, which needs two things:

- **The API to accept a service account's token**: its client needs the
  `teehr-control-plane-jobs` audience, as the user-facing clients have.
- **Jobs to act for a service account.** A job acts as its caller through a delegated session
  made by exchanging the caller's token for a refresh token (ADR-0014, ADR-0018). A spike (#266)
  against Keycloak 26.8.1, the clusters' version, found:

  | Subject token | Exchange for a refresh token (`SAME_SESSION`) |
  | --- | --- |
  | a user's | works (today) |
  | a service account's, default client credentials | refused: "creating a new session is needed" |
  | a service account's, client with `client_credentials.use_refresh_token: true` | works; refreshes past the access token's life; tokens keep the account's roles |

  The same spike confirmed a service account's user (`service-account-<client>`) is found by the
  exact-username lookup member management uses, its `sub` is that user's id, and a role granted
  to it is in its next token.

The fork: keep realm roles as a second way to grant project access (and teach the API to honor
them), or make membership the only way, with service accounts as members.

## Decision

We will make **project membership the one source of project access**, for people and service
accounts alike. Keycloak project roles and Polaris grants stay derived from it, as they are for
tenant projects today; nothing grants project access on its own.

1. **A service account is a member like a user.** Its Keycloak user, `service-account-<client>`,
   is added with a role (`teehr projects members add service-account-<client> --role write`).
   A client used this way (an *automation client*) has `client_credentials.use_refresh_token`
   on and the `audience-teehr-control-plane-jobs` mapper. Its token then works against the API,
   and a job it starts acts as it through the existing exchange path: no new mechanism in the
   control plane.
2. **The system projects' writers are members**, named in configuration:
   `SYSTEM_PROJECT_WRITERS` (usernames). The startup reconcile makes each a write member of
   `public` and `platform` (and leaves other members alone). This replaces
   `SYSTEM_PROJECT_WRITER_ROLES` and the `teehr-platform-write` role, which are removed once
   nothing depends on them.
3. **The v1 flows get their own automation client, `teehr-platform-automation`**, the one system
   writer: they write the warehouse (ADR-0019) and start runs as it. `prefect-polaris` stays as
   it is for the legacy flows and retires with them (ADR-0015).
4. **Readers are unchanged for now.** Any signed-in user reads the system projects through the
   API, and `SYSTEM_PROJECT_READER_ROLES` gives readers Polaris access (Jupyter, Trino). Roles or
   groups as members, which would fold this into membership too, wait for a need: a group
   granted access to a tenant project, or the legacy readers retiring.

## Consequences

- **Positive:** one question, *is X a member, and with what role?*, answers access in the API
  and in Polaris, so they can't disagree. Automation needs no person-like user, browser sign-in
  or yearly token. The remote gate tests (#206) can use a test service account the same way.
- **Negative / cost:** each deployment gets one more client and secret. With
  `use_refresh_token` on, every client-credentials grant opens a Keycloak session (gone at the
  realm's idle timeout), so it belongs on clients that fetch tokens modestly, which is why the
  v1 flows don't reuse `prefect-polaris`.
- **Tradeoff accepted:** the system projects' writer list is configuration the reconcile
  enforces, not an API-managed list; a writer removed by hand comes back on restart.

## Alternatives considered

- **Honor `SYSTEM_PROJECT_WRITER_ROLES` in the API.** Starts runs, but jobs still can't act for
  a default client-credentials caller, and access stays decided in two places.
- **A person-like bot user with a personal token.** Works today with no change; a browser
  sign-in and a secret to rotate every year, for a machine.
- **The control plane keeps automation clients' secrets and mints their tokens.** No Keycloak
  setting, but the control plane could then act as any client it holds, the kind of power
  ADR-0018 declined.
- **Token-exchange impersonation** (the control plane becomes the caller). A preview Keycloak
  feature; rejected in ADR-0018 for the same reason.

## Notes

Order: the `teehr-platform-automation` client in the realm; `SYSTEM_PROJECT_WRITERS` in the
control plane; the v1 flows switched to the new client (and `run-platform-evaluations` to its
token, dropping `prefect-control-plane-token`); then `SYSTEM_PROJECT_WRITER_ROLES` and
`teehr-platform-write` removed. #167's remaining questions (who edits users' projects, audit log)
are unchanged by this.
