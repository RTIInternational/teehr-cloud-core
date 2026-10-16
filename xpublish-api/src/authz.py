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


# --- Request paths ---

# Every route under /api/datasets/{id}/ that xpublish-api serves (custom routes, the tiles and EDR
# plugins). Anything else under that prefix is denied, so a route added by an xpublish or plugin
# upgrade can't skip the check: when upgrading, list the new routes here and in the tests.
DATASET_ROUTES = ("coords/", "variable-attrs", "edr/", "tiles/")
DENY = "deny"
RAW_DATA_SUFFIX = "_raw_data"


def _repo_candidates(dataset_id: str) -> list[str]:
    """Repos a dataset id can refer to; the provider matches the exact name, then strips ``_raw_data``."""
    candidates = [dataset_id]
    if dataset_id.endswith(RAW_DATA_SUFFIX) and dataset_id != RAW_DATA_SUFFIX:
        candidates.append(dataset_id[: -len(RAW_DATA_SUFFIX)])
    return candidates


def request_target(path: str) -> tuple[Kind, list[str]] | str | None:
    """What a request reads: ``(kind, names)``, None for no repo or layer, or DENY for an unrecognised path.

    A request is allowed only if the user may read every name, so an ambiguous dataset id (a repo
    itself named ``*_raw_data``) is checked against both repos it could mean.
    """
    if path in ("/api/datasets", "/api/datasets/"):
        return DENY  # xpublish's built-in list of every dataset; the frontend uses /api/dataset-keys
    if path.startswith("/api/datasets/"):
        dataset_id, _, rest = path[len("/api/datasets/"):].partition("/")
        if not dataset_id or not (rest == "variable-attrs" or rest.startswith(DATASET_ROUTES)):
            return DENY
        return "icechunk", _repo_candidates(dataset_id)
    if path.startswith("/api/dataset-variables/"):
        dataset_id = path[len("/api/dataset-variables/"):]
        if not dataset_id or "/" in dataset_id:
            return DENY
        return "icechunk", _repo_candidates(dataset_id)
    if path.startswith("/api/vector-tiles/"):
        archive = path[len("/api/vector-tiles/"):]
        if not archive.endswith(".pmtiles") or "/" in archive or archive == ".pmtiles":
            return DENY
        return "vector_tiles", [archive[: -len(".pmtiles")]]
    return None


def is_request_permitted(rules: AccessRules, identity: AuthIdentity, path: str) -> bool:
    """Whether the user may make a request; unrecognised paths under the dataset routes are denied."""
    target = request_target(path)
    if target is None:
        return True
    if target == DENY:
        return False
    kind, names = target
    return all(is_permitted(rules, identity, kind, name) for name in names)
