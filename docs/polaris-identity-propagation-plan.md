# TEEHR Hub Identity Propagation and Fine-Grained Data Authorization Plan

Last updated: 2026-06-17

## Goals

Establish a unified identity and authorization architecture for all TEEHR Hub services that access Iceberg-backed data so that:

- Keycloak remains the single source of truth for user identity and group membership.
- JupyterHub, FastAPI, and other user-facing entry points propagate end-user identity forward instead of collapsing users into a shared technical principal.
- Apache Polaris becomes the primary enforcement point for fine-grained catalog and table permissions.
- Spark, Trino, and other compute/query services access Iceberg through Polaris in a way that preserves user-level authorization context.
- Existing services continue to work during migration, with clear phases and fallback paths.

## Principles

1. **Identity source of truth:** Keycloak owns users, groups, and role/group assignments.
2. **Data authorization source of truth:** Polaris owns fine-grained data authorization for Iceberg catalogs, namespaces, tables, and write operations.
3. **Policy enforcement at the data plane:** JupyterHub and FastAPI should not be the final authority for what data a user can read or write. They should authenticate the user, propagate identity, and rely on downstream systems to enforce.
4. **End-user identity propagation:** Where a user initiates an action, downstream data access should occur with a credential or context representing that user.
5. **Coarse vs fine-grained separation:** Keycloak groups remain coarse platform entitlements; Polaris policies hold fine-grained data access rules.
6. **Hybrid authorization model:** Policies are assigned primarily to groups, but requests should always carry the individual user identity for auditability, traceability, and exception handling.
7. **Least privilege:** Shared Kubernetes, AWS, or service identities must not undermine per-user data authorization.
8. **Short-lived credentials:** Prefer short-lived, revocable credentials/tokens over long-lived shared secrets inside notebook or app runtimes.

## Target Architecture

### Flow A: Keycloak → JupyterHub → Polaris → Iceberg/Spark

1. User authenticates to JupyterHub with Keycloak OIDC.
2. JupyterHub stores auth state and extracts a minimal set of claims/groups needed for spawn-time decisions and downstream identity propagation.
3. Jupyter single-user server receives either:
   - a short-lived access token for the logged-in user, or
   - a short-lived exchanged token/credential derived from the user identity.
4. Notebook clients (Spark, PyIceberg, direct REST catalog clients) present that per-user credential to Polaris.
5. Polaris evaluates both the individual user principal and the user’s Keycloak-derived groups.
6. Iceberg operations proceed only if Polaris authorizes them.
7. Underlying object storage access is constrained so users cannot bypass Polaris with broad direct bucket permissions.

### Flow B: Keycloak → FastAPI → Trino → Polaris → Iceberg

1. User authenticates to FastAPI with a Keycloak JWT.
2. FastAPI validates the JWT and extracts user identity plus coarse claims.
3. For data-plane calls, FastAPI forwards end-user identity to Trino using a supported secure mechanism.
4. Trino uses that end-user context when resolving catalog access through Polaris.
5. Polaris evaluates both the individual user principal and the user’s Keycloak-derived groups.
6. FastAPI remains responsible for app-level capabilities and route access, but not for warehouse data authorization beyond coarse gatekeeping.

## Identity Model

### Keycloak responsibilities

Keycloak should continue to manage:

- users
- default baseline access
- group membership
- OIDC clients for frontend, JupyterHub, FastAPI, and any service-to-service integrations

The existing coarse group model is a strong starting point:

- `basic-user`
- `jupyter-user`
- `jupyter-admin`
- `iceberg-user`
- `key-management-admin`
- `prefect-admin`
- `webapi-admin`

### Recommended group strategy

Keep Keycloak groups coarse and human-manageable. Suggested semantics:

- `basic-user`: baseline authenticated application access
- `jupyter-user`: allowed to access JupyterHub
- `jupyter-admin`: JupyterHub admin rights only
- `iceberg-user`: allowed to use catalog-backed data tools
- optional team/domain groups: e.g. `hydrology-team`, `forecast-team`, `operations-team`

Avoid expressing every dataset/table permission directly in Keycloak groups. Instead, use Keycloak groups as the default policy-assignment mechanism in Polaris and reserve direct per-user grants for exceptional cases.

### Hybrid authorization model

The target authorization model should be hybrid:

- Every human-originated request carries the individual Keycloak user identity end to end.
- Polaris policy assignment is primarily group-based.
- Individual user grants are exceptions, not the default.
- Non-human workloads use distinct service principals with narrowly scoped rights.
- Audit logs should preserve both the individual principal and the effective groups used for authorization.

Rule of thumb:

> Policies are assigned mostly to groups, but requests always carry the individual user identity.

### Claims to propagate

At minimum, downstream systems should have access to:

- subject (`sub`)
- preferred username or stable user identifier
- groups
- optionally realm roles if needed for coarse app behavior
- issuer (`iss`)
- audience/client context as needed for validation

## Polaris Authorization Model

### Polaris as the fine-grained authority

Polaris should become the primary location for:

- catalog-level permissions
- namespace/schema permissions
- table/view permissions
- read vs write separation
- admin/management operations
- team/project-specific access

### Mapping strategy

Prefer these patterns, in order:

1. group-based policy mapping for team/domain entitlements
2. direct user principal mapping for limited exceptions
3. distinct service principals for non-human automation paths

Examples:

- `iceberg-user` grants general eligibility to use catalog-backed services
- `hydrology-team` maps to read access on selected namespaces/tables
- `forecast-team` maps to write access on forecast-derived tables only
- a specific user may receive a temporary direct grant for a narrow exception case
- automation service accounts get narrowly scoped non-human privileges

### Project namespace model

Eligible human users should be able to create and manage project namespaces for exploratory, collaborative, and intermediate work.

Recommended rules:

- Project namespace creation is allowed only for users with the required coarse entitlement, such as `iceberg-user`.
- Project namespaces live under the `projects` prefix and use a user-provided name: `projects.<provided_namespace_name>`.
- Namespace names must satisfy validation and uniqueness rules.
- Project namespaces are created lazily on first use.
- The creating user becomes the initial owner and receives read, write, and manage permissions for that namespace.
- New project namespaces default to `private` visibility.
- The owner may optionally configure the namespace to allow read-only access for all eligible Iceberg users.
- Shared/team/production namespaces remain governed primarily by group-based Polaris policies and separate governance.

Recommended sharing modes for the initial implementation:

- `private`: only the owner may access the namespace, except for narrowly scoped admin or maintenance access
- `all-iceberg-users-read`: all eligible Iceberg users may read objects in the namespace, while only the owner may write or manage objects

This model intentionally uses project namespaces as the initial self-service workspace primitive. A separate `users.*` personal-namespace model is deferred unless later usage demonstrates a clear need for a distinct personal workspace class.

### Project namespace naming and governance rules

Recommended v1 naming rules for `projects.<provided_namespace_name>`:

- use lowercase letters, numbers, and hyphens only
- must start with a letter
- should be globally unique under `projects.*`
- names that differ only by case should be treated as the same name
- reserve selected names and prefixes such as `admin`, `system`, `default`, `prod`, `production`, and `shared`
- apply a reasonable maximum length to the provided namespace component, such as 50 characters

Recommended v1 governance rules:

- the creating user becomes the initial owner
- the owner receives read, write, and manage permissions for the namespace
- new namespaces default to `private`
- the owner may switch visibility between `private` and `all-iceberg-users-read`
- arbitrary custom ACLs, explicit collaborators, and self-service ownership transfer are out of scope for v1
- ownership transfer is admin-controlled in the initial implementation
- admins may recover, reassign, archive, or otherwise govern orphaned namespaces when an owner leaves or loses entitlement
- apply an initial per-user limit on the number of project namespaces, such as 10, with admin override if needed

### Human vs automation separation

Polaris policy should explicitly separate:

- human interactive access from JupyterHub and FastAPI
- delegated execution on behalf of a human user
- non-human automation such as Prefect, ingestion jobs, and maintenance workflows

Non-human automation should use distinct service principals and should not impersonate human users by default.

## JupyterHub Design

### JupyterHub responsibilities

JupyterHub should:

- authenticate users with Keycloak
- authorize JupyterHub access using coarse groups
- persist auth state securely
- pass minimal downstream identity into notebook runtimes
- optionally shape spawn behavior based on groups

JupyterHub should not be the final authority for Iceberg permissions.

### Recommended implementation pattern

1. Enable and secure `auth_state` persistence.
2. At login or pre-spawn time, read:
   - `sub`
   - username
   - groups
   - token expiry metadata
   - access token only if needed
3. Pass into notebook pods:
   - minimal identity env vars for UX and telemetry
   - a short-lived token or exchanged credential for Polaris access
4. Avoid passing refresh tokens into notebook environments unless absolutely necessary.
5. Use spawn hooks to gate profiles/features, not to implement table-level authorization.

### Notebook runtime behavior

Notebook runtimes should:

- authenticate to Polaris as the end user
- use catalog-aware clients for Iceberg access
- avoid direct object-store access patterns that bypass Polaris
- allow creation and management of project namespaces for users with the required entitlement

### Spark integration

Spark jobs launched from Jupyter should preserve the originating end-user identity when accessing Polaris. This likely means:

- Spark driver receives user credential/context from the notebook environment
- Spark catalog configuration uses Polaris endpoints and auth settings
- executor-side access follows the driver’s authenticated catalog interactions or other supported user-context mechanism
- audit context should preserve both the initiating user identity and the effective groups used for authorization

Exact mechanics depend on the Spark + Iceberg + Polaris auth model you select, but the design goal is unchanged: no shared “all notebooks are the same person” catalog identity.

### Kubernetes and AWS identity caution

The current shared `jupyter` service account and shared IRSA role are acceptable for platform operations only if they do not grant blanket data access that bypasses Polaris.

Recommended direction:

- keep shared pod identity narrow
- do not rely on shared IRSA for warehouse authorization
- ensure direct S3/object-store permissions are minimized relative to Polaris-mediated access

## FastAPI Design

### FastAPI responsibilities

FastAPI should:

- validate Keycloak JWTs
- enforce application-level route permissions and coarse feature gates
- propagate end-user identity to Trino/data clients
- not substitute a shared privileged warehouse identity for end-user requests

### Existing repo alignment

The API already validates Keycloak JWTs and extracts realm roles. This is a good foundation for:

- app-level authorization
- request identity extraction
- future downstream identity propagation

### Forwarding identity to Trino

The exact Trino integration should be chosen based on supported secure mechanisms, but the plan should require:

- preserving a stable end-user identity into the Trino session
- preserving effective Keycloak groups or equivalent authorization context where supported
- preventing FastAPI from always querying as a single technical user for end-user traffic
- aligning Trino catalog access with Polaris-enforced permissions

Candidate approaches to evaluate:

1. user identity forwarded as authenticated session principal
2. OAuth/OIDC-aware Trino integration if supported by chosen deployment
3. trusted proxy/service pattern only if it still preserves distinguishable end-user principals and auditable enforcement

## Service-by-Service Policy Split

### Keycloak

Owns:

- authentication
- user lifecycle
- groups/roles
- client registration

Does not own:

- fine-grained Iceberg table permissions

### JupyterHub

Owns:

- notebook login authorization
- admin access to JupyterHub
- spawn-time feature gating

Does not own:

- final warehouse data authorization

### FastAPI

Owns:

- API authentication
- app feature authorization
- rate limiting / route protection / business rules

Does not own:

- final Iceberg table authorization for end-user data access

### Polaris

Owns:

- fine-grained catalog and table authorization
- group-based policy evaluation as the default mechanism
- limited direct user grants for exceptional cases
- project namespace ownership and namespace-level self-service rules
- data-access decisions for Iceberg-aware clients/services

### Trino / Spark

Owns:

- execution under propagated user identity
- honoring Polaris-backed catalog authorization
- preserving auditability of the human initiator where applicable

### Automation services

Own:

- non-human scheduled or background execution under distinct service principals

Do not own:

- human-interactive identity or authorization decisions

## Implementation Checklist

### Repository and application changes

#### JupyterHub

- Locate current JupyterHub auth configuration.
- Confirm Keycloak OIDC integration path.
- Verify whether `auth_state` is enabled and persisted securely.
- Identify where pre-spawn hooks can extract username, `sub`, groups, and token expiry metadata.
- Decide what minimal identity context should be injected into notebook runtimes.
- Decide whether notebook runtimes receive a direct short-lived user token or an exchanged credential.
- Prototype notebook-side access to Polaris as the end user.
- Identify where project namespace create-on-first-use logic should live for Jupyter-driven workflows.

#### FastAPI

- Locate JWT validation and auth dependency code.
- Confirm where username, subject, roles, and groups are extracted today.
- Add or refine a canonical request identity object.
- Trace every FastAPI path that triggers Trino or Iceberg-backed access.
- Identify where end-user identity must be forwarded downstream.
- Determine whether project namespace operations will be exposed through API endpoints.
- If project namespace operations are exposed through the API, define behavior for create namespace, get namespace visibility, set namespace visibility, and list owned namespaces.

#### Trino integration

- Locate current Trino client or session creation code.
- Identify how user identity is currently represented in Trino sessions.
- Determine where group or authorization context could be forwarded.
- Document whether current behavior uses a shared technical principal.
- Define required code and configuration changes for per-user session context.

#### Spark and notebook data access

- Locate Spark catalog configuration used by notebooks.
- Determine how Polaris would be configured as the Iceberg catalog.
- Verify where Spark receives user auth context.
- Determine whether executor behavior preserves user-context semantics.
- Prototype read and write in a user-created project namespace.

#### Project namespace workflow

- Define the canonical create-on-first-use workflow.
- Define validation for `projects.<provided_namespace_name>`.
- Implement or prototype name validation rules.
- Define the visibility enum for v1: `private` and `all-iceberg-users-read`.
- Define owner permissions.
- Define admin-only ownership transfer handling.
- Define behavior when a namespace already exists.
- Define behavior when the creating user lacks `iceberg-user`.

#### Audit and observability

- Identify where to log initiating user identity.
- Identify where to log effective groups.
- Identify where to log namespace creation and visibility changes.
- Define correlation points between Keycloak user, Jupyter session or API request, Trino or Spark execution, and Polaris authorization decisions.

### Platform and infrastructure changes

#### Keycloak

- Confirm the final group model for v1.
- Confirm that groups claims are present in tokens where needed.
- Confirm username stability expectations.
- Confirm whether `preferred_username` is sufficient for namespace naming.
- Confirm whether immutable `sub` should also be logged for ownership and audit.

#### Polaris

- Confirm the supported authentication method for end-user principals.
- Confirm how Polaris consumes user and group information.
- Confirm namespace creation APIs or workflow.
- Confirm the grant model for namespace owner read/write/manage, all-iceberg-users read-only, and admin recovery access.
- Confirm whether visibility toggling maps cleanly to grant changes.
- Confirm how to model project namespace ownership operationally.

#### Object storage, AWS, and IAM

- Inventory current object-store access paths.
- Identify any direct bucket permissions that bypass Polaris.
- Narrow shared IRSA or service-account access where needed.
- Define operational and admin exceptions.
- Confirm whether project namespace creation requires additional storage-side setup.

#### Kubernetes and deployment

- Locate JupyterHub deployment configuration.
- Locate FastAPI deployment configuration.
- Identify the secret and token handling mechanism.
- Confirm how short-lived credentials would be passed and rotated.
- Identify configuration surfaces for Polaris endpoints and auth settings.

#### Admin and governance operations

- Define the admin process for namespace reassignment.
- Define the admin process for orphaned namespaces.
- Define the namespace quota override process.
- Define the reserved-name management process.

### Validation spikes and unknowns

- Validate whether Polaris can directly authenticate Keycloak-issued user tokens.
- Validate whether Polaris can evaluate group-based policy from those tokens.
- Validate the exact grant model needed for namespace ownership and read-only sharing.
- Validate whether a notebook can authenticate to Polaris as the actual user.
- Validate the safest token propagation pattern for notebooks.
- Validate how long-lived notebook sessions behave when tokens expire.
- Validate whether Spark can access Polaris with preserved user context.
- Validate whether user attribution is maintained only at the driver level or throughout execution.
- Validate whether Trino can preserve end-user identity in the way Polaris needs.
- Validate whether group context can be propagated or reconstructed for Trino-driven requests.
- Validate whether project namespaces can be created lazily without fragile race conditions.
- Validate how duplicate creation attempts should behave.
- Validate how visibility changes will be represented in Polaris grants.
- Validate which current credentials or access paths still allow storage bypass.

### Recommended implementation order

1. Validate the Polaris authentication and policy model.
2. Validate the Jupyter-to-Polaris end-user flow.
3. Validate the FastAPI-to-Trino-to-Polaris flow.
4. Inventory current storage bypass paths.
5. Implement the first vertical slice around project namespace creation and default private access.
6. Validate owner read/write access, denied access for a second user, and read-only access after switching visibility to `all-iceberg-users-read`.
7. Add audit logging, admin recovery flows, quotas, and broader Spark/API parity.

## Migration Phases

### Phase 0: Discovery and capability validation

Validate product capabilities and constraints before committing implementation details:

- how Polaris authenticates principals and consumes OIDC/user identity
- how Iceberg clients authenticate to Polaris
- how Trino integrates with Polaris and preserves user identity
- how Spark integrates with Polaris and preserves user identity
- how groups/claims can be surfaced to Polaris, directly or indirectly
- how project namespace creation, sharing, and ownership can be represented in Polaris policy
- whether token exchange, service delegation, or direct bearer-token auth is preferred
- what object-store permissions are still required beneath Polaris

Deliverables:

- architecture decision record
- supported auth flow matrix for Jupyter, Spark, FastAPI, Trino
- gap list for unsupported assumptions

### Phase 1: Identity inventory and policy model

Define the canonical identity and policy model:

- inventory existing Keycloak groups and roles
- identify coarse platform groups to keep
- define any new team/domain groups
- define how Keycloak groups map into Polaris principals/policies
- define criteria for when direct user grants are allowed
- define naming conventions for users, groups, namespaces, catalogs
- define the project namespace naming, entitlement, and visibility model

Deliverables:

- identity map
- group-to-Polaris policy map
- per-user exception policy
- project namespace policy
- example access-control matrix for target state

### Phase 2: JupyterHub identity propagation foundation

Implement the JupyterHub foundation for user-context propagation:

- enable auth state persistence
- add pre-spawn logic to extract minimal claims
- inject minimal identity metadata into notebook sessions
- evaluate secure handling for short-lived user token or exchanged credential
- validate notebook-to-Polaris authentication path
- validate audit visibility of user identity and effective groups
- validate project namespace create/read/write behavior for eligible users

Deliverables:

- JupyterHub configuration changes
- secret-handling model
- proof of concept notebook access path
- audit-context validation notes
- project namespace proof of concept

### Phase 3: Spark + Iceberg + Polaris user-context path

Implement Spark access through Polaris under end-user identity:

- configure Spark Iceberg catalog for Polaris
- validate read/write behavior by user/group
- validate create/read/write behavior in user-created project namespaces
- ensure job submissions launched from Jupyter preserve user context
- confirm executor/runtime behavior does not collapse to a shared catalog principal
- confirm how delegated execution is attributed in logs and policy evaluation

Deliverables:

- Spark catalog configuration
- end-to-end auth test cases
- operational notes for debugging and token expiry
- delegated execution audit model
- project namespace Spark validation cases

### Phase 4: FastAPI → Trino user-context propagation

Implement user-context propagation for API-driven data access:

- formalize request identity object in FastAPI
- define Trino session principal propagation strategy
- validate per-user access behavior through Polaris
- validate group-based authorization behavior through Polaris
- validate create/read/write behavior for project namespaces where API workflows support it
- keep existing app-level route authorization intact

Deliverables:

- FastAPI integration design
- Trino integration configuration
- end-to-end API authz test cases
- request-to-query audit mapping
- project namespace API validation cases

### Phase 5: Storage hardening / bypass prevention

Reduce or eliminate bypass paths that would undermine Polaris:

- audit direct S3/object-store permissions for Jupyter, Spark, Trino, and service accounts
- minimize shared credentials with broad warehouse access
- ensure intended clients access warehouse data through Polaris-mediated paths
- define exceptions explicitly for admin/maintenance automation
- ensure automation principals are separated from human interactive access

Deliverables:

- credential inventory
- least-privilege policy changes
- documented exception list
- human-vs-automation access boundary documentation

### Phase 6: Rollout, observability, and migration cleanup

Roll out incrementally and verify behavior:

- pilot with a small set of users/groups
- compare current vs target behavior
- add audit logging and request tracing where possible
- deprecate old shared-identity assumptions
- update docs and developer notebooks/examples
- document the user experience for project namespace creation and visibility configuration

Deliverables:

- rollout checklist
- audit/observability plan
- migration completion checklist
- project namespace user guidance

## Open Design Questions

1. What auth mechanism does the selected Polaris deployment support for end-user principals?
2. Can Polaris directly evaluate Keycloak-issued JWTs, or is an intermediate exchange/delegation layer needed?
3. How are Keycloak groups or equivalent authorization attributes surfaced to Polaris policy evaluation?
4. What naming validation and uniqueness rules should apply to `projects.<provided_namespace_name>`?
5. Should project namespace visibility initially support only `private` and `all-iceberg-users-read`, or additional sharing modes?
6. What is the supported user-identity propagation mechanism from Trino to Polaris?
7. What is the supported user-identity propagation mechanism from Spark to Polaris?
8. Do PyIceberg and any direct notebook clients need separate auth handling from Spark?
9. How will token refresh work for long-lived notebook sessions and Spark jobs?
10. What direct object-store permissions are still needed, and how do we prevent them from bypassing Polaris?
11. Which existing services besides Jupyter, FastAPI, Spark, and Trino also need user-context-aware Iceberg access?
12. How should non-human automation be separated from human end-user access?
13. What audit trail is required to correlate Keycloak user, notebook/API request, Trino/Spark execution, effective groups, project namespace ownership, and Polaris decision?
14. What governance process should control direct per-user exceptions?
15. What quotas, lifecycle rules, or cleanup policies should apply to project namespaces?

## Immediate Next Steps

1. Inspect current JupyterHub config and identify where to enable `auth_state` and pre-spawn claim handling.
2. Document current FastAPI → Trino call paths and whether requests already preserve end-user context.
3. Inventory all Iceberg-accessing services in the repo and classify them as human-initiated vs automation.
4. Research and document the chosen Polaris auth capabilities and Trino/Spark integration constraints.
5. Draft the target group-to-policy mapping for current TEEHR personas and datasets.
6. Define the exception policy for direct user grants and the boundary for automation principals.
7. Define the project namespace naming, entitlement, and visibility model.

## Non-Goals

This plan does not attempt to:

- move all fine-grained authorization into Keycloak
- use notebook-side Python logic as the primary enforcement point
- keep broad shared storage credentials as the long-term authorization model
- make direct per-user grants the default authorization strategy
- allow arbitrary self-created shared namespaces without governance
- finalize product-specific config syntax before validating supported auth paths
