"""Read access to Icechunk repos and vector-tile layers by Keycloak realm role.

The rules map (``TEEHR_ACCESS_RULES``, JSON) has three optional keys::

    {"default": ["icechunk-read-only"],
     "icechunk": {"<repo>": ["<role>", ...]},
     "vector_tiles": {"<layer>": ["<role>", ...]}}

- ``default``: roles that can read any repo or layer not listed.
- A listed repo or layer replaces the default with its own roles; ``[]`` means nobody.
- Without a ``default``, unlisted items are open to every logged-in user.

See docs/icechunk-access-control.md.
"""
import json
from dataclasses import dataclass, field
from typing import Literal

from .auth import AuthIdentity

Kind = Literal["icechunk", "vector_tiles"]
KINDS: tuple[Kind, ...] = ("icechunk", "vector_tiles")


@dataclass(frozen=True)
class AccessRules:
    # None: no default, so unlisted items are open to every logged-in user
    default: tuple[str, ...] | None = None
    items: dict[str, dict[str, tuple[str, ...]]] = field(
        default_factory=lambda: {kind: {} for kind in KINDS}
    )


def _roles(value, where: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not all(isinstance(role, str) for role in value):
        raise ValueError(f"TEEHR_ACCESS_RULES: {where} must be a list of role names, got {value!r}")
    return tuple(value)


def parse_access_rules(raw: str) -> AccessRules:
    """Parse and validate the rules map; raise ValueError rather than ignore a mistake.

    With default-allow for unlisted items, a silently ignored typo could change who can read a repo.
    """
    try:
        data = json.loads(raw or "{}")
    except json.JSONDecodeError as exc:
        raise ValueError(f"TEEHR_ACCESS_RULES is not valid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError("TEEHR_ACCESS_RULES must be a JSON object")
    unknown = set(data) - {"default", *KINDS}
    if unknown:
        raise ValueError(f"TEEHR_ACCESS_RULES has unknown keys: {sorted(unknown)}")

    items = {}
    for kind in KINDS:
        entries = data.get(kind, {})
        if not isinstance(entries, dict):
            raise ValueError(f"TEEHR_ACCESS_RULES: {kind} must be an object of name -> roles")
        items[kind] = {name: _roles(roles, f"{kind}.{name}") for name, roles in entries.items()}
    default = _roles(data["default"], "default") if "default" in data else None
    return AccessRules(default=default, items=items)


def _holds_any(identity: AuthIdentity, roles: tuple[str, ...]) -> bool:
    return any(role in identity.roles for role in roles)


def has_default(rules: AccessRules, identity: AuthIdentity) -> bool:
    """Whether the user can read unlisted items: they hold a default role, or no default is set."""
    return identity.is_authenticated and (rules.default is None or _holds_any(identity, rules.default))


def is_permitted(rules: AccessRules, identity: AuthIdentity, kind: Kind, name: str) -> bool:
    """Default: roles that can read anything not listed. Listed: only these roles; [] = nobody."""
    if not identity.is_authenticated:
        return False
    listed = rules.items[kind].get(name)
    if listed is not None:
        return _holds_any(identity, listed)
    return has_default(rules, identity)


def unreadable_listed(rules: AccessRules, identity: AuthIdentity, kind: Kind) -> list[str]:
    """Listed items the user can't read; denied in the credential policy for default-role holders."""
    return sorted(name for name, roles in rules.items[kind].items() if not _holds_any(identity, roles))


def readable_listed(rules: AccessRules, identity: AuthIdentity, kind: Kind) -> list[str]:
    """Listed items the user can read; the only ones allowed for users without a default role."""
    return sorted(name for name, roles in rules.items[kind].items() if _holds_any(identity, roles))


def unmatched(rules: AccessRules, kind: Kind, existing: list[str]) -> list[str]:
    """Rules naming items that don't exist, e.g. a misspelled repo that stays readable by default."""
    return sorted(set(rules.items[kind]) - set(existing))

