"""Turn project membership into data access, in Keycloak and Polaris.

The control plane's membership table is the source of truth. For each member it
converges two places to "exactly the role for their membership, no other role on
this project":

- **Keycloak**: the realm role ``teehr-project-<name>-<role>`` (in their tokens, and
  what the ``polaris-principal-sync`` CronJob reads).
- **Polaris**: the same-named principal role, bound to the member's named
  principal (their username). Polaris uses a named principal's explicit bindings,
  not the token, so this is what actually grants or removes access - immediately,
  rather than at the CronJob's next run.

Every call is idempotent, so a partial failure is fixed by running it again.
"""

from src.keycloak_admin import KeycloakAdmin, KeycloakError
from src.models import Role
from src.polaris import PolarisAdmin, PolarisError, principal_role_name


class AccessError(Exception):
    """Membership couldn't be turned into access (Keycloak or Polaris failed)."""


def project_roles(project: str) -> dict[Role, str]:
    """Each project role's name - the same in Keycloak and Polaris."""
    return {role: principal_role_name(project, role) for role in Role}


def ensure_project_roles(keycloak: KeycloakAdmin, project: str) -> None:
    for role, name in project_roles(project).items():
        keycloak.ensure_realm_role(name, f"{role.value} access to project '{project}'")


def delete_project_roles(keycloak: KeycloakAdmin, project: str) -> None:
    """Delete the project's realm roles (Keycloak drops them from every user)."""
    for name in project_roles(project).values():
        keycloak.delete_realm_role(name)


def sync_member(
    keycloak: KeycloakAdmin,
    polaris: PolarisAdmin,
    project: str,
    subject: str,
    role: Role | None,
    username_hint: str | None = None,
) -> str | None:
    """Give a member exactly ``role`` on ``project`` (``None`` = no access).

    Returns the member's username (their Polaris principal), or None if they no
    longer exist in Keycloak and had nothing to remove there.
    """
    all_roles = set(project_roles(project).values())
    wanted = {principal_role_name(project, role)} if role else set()

    try:
        user = keycloak.get_user(subject)
        if user is None:
            if role is not None:
                raise AccessError(f"Keycloak user '{subject}' does not exist")
            username = username_hint  # removing a user who is already gone
        else:
            username = user["username"]
            held = keycloak.user_realm_roles(subject) & all_roles
            keycloak.remove_realm_roles(subject, held - wanted)
            keycloak.add_realm_roles(subject, wanted - held)
    except KeycloakError as exc:
        raise AccessError(f"Keycloak: {exc}") from exc

    if username is None:
        return None
    try:
        if wanted:
            polaris.ensure_principal(username)
        # Always read the bindings, also when the Keycloak user is gone: a removed
        # member must lose Polaris access even if their account was deleted first.
        bound = polaris.principal_roles_of(username) & all_roles
        for name in bound - wanted:
            polaris.revoke_principal_role(username, name)
        for name in wanted - bound:
            polaris.grant_principal_role(username, name)
    except PolarisError as exc:
        raise AccessError(f"Polaris: {exc}") from exc
    return username
