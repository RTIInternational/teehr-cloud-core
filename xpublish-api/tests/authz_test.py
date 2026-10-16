"""Unit tests for the access rules; run from xpublish-api/ with `python -m pytest tests`."""
import json

import pytest

from src.auth import AuthIdentity
from src.authz import (
    has_default,
    is_permitted,
    parse_access_rules,
    readable_listed,
    unmatched,
    unreadable_listed,
)

RULES = parse_access_rules(json.dumps({
    "default": ["icechunk-read-only"],
    "repos": {"isnobal_boise": ["boise-snow-reader"], "staged": []},
    "layers": {"rfc-basins": ["boise-snow-reader"]},
}))
OPEN = parse_access_rules("{}")


def user(*roles):
    return AuthIdentity(subject="u", auth_type="jwt", roles=list(roles))


ANONYMOUS = AuthIdentity(subject="anonymous", auth_type="anonymous")


def test_unlisted_repo_needs_a_default_role():
    assert is_permitted(RULES, user("icechunk-read-only"), "repos", "ua_swann_4km")
    assert not is_permitted(RULES, user("basic-user"), "repos", "ua_swann_4km")


def test_without_default_unlisted_repo_is_open_to_logged_in_users():
    assert is_permitted(OPEN, user(), "repos", "ua_swann_4km")
    assert not is_permitted(OPEN, ANONYMOUS, "repos", "ua_swann_4km")


def test_listed_repo_replaces_the_default():
    assert is_permitted(RULES, user("boise-snow-reader"), "repos", "isnobal_boise")
    assert not is_permitted(RULES, user("icechunk-read-only"), "repos", "isnobal_boise")


def test_empty_list_denies_everyone_including_admin():
    for identity in (user("icechunk-read-only"), user("admin"), user("boise-snow-reader")):
        assert not is_permitted(RULES, identity, "repos", "staged")


def test_layers_are_separate_from_repos():
    assert not is_permitted(RULES, user("icechunk-read-only"), "layers", "rfc-basins")
    assert is_permitted(RULES, user("icechunk-read-only"), "layers", "usgs-basins")
    assert is_permitted(RULES, user("boise-snow-reader"), "layers", "rfc-basins")


def test_anonymous_never_permitted():
    assert not is_permitted(RULES, ANONYMOUS, "repos", "ua_swann_4km")
    assert not has_default(OPEN, ANONYMOUS)


def test_has_default_picks_the_credential_policy_mode():
    assert has_default(RULES, user("icechunk-read-only"))
    assert not has_default(RULES, user("boise-snow-reader"))
    assert has_default(OPEN, user())


def test_listed_names_for_deny_and_allow_modes():
    default_holder = user("icechunk-read-only")
    assert unreadable_listed(RULES, default_holder, "repos") == ["isnobal_boise", "staged"]
    assert readable_listed(RULES, default_holder, "repos") == []
    team_only = user("boise-snow-reader")
    assert readable_listed(RULES, team_only, "repos") == ["isnobal_boise"]
    assert unreadable_listed(RULES, team_only, "repos") == ["staged"]


def test_unmatched_finds_rules_for_missing_items():
    assert unmatched(RULES, "repos", ["isnobal_boise", "ua_swann_4km"]) == ["staged"]
    assert unmatched(RULES, "layers", ["rfc-basins"]) == []


@pytest.mark.parametrize("raw", [
    "not json",
    "[]",
    '{"repos": {"a": ["r"]}, "unknown": {}}',
    '{"default": "icechunk-read-only"}',
    '{"repos": {"a": "admin"}}',
    '{"repos": {"a": [1]}}',
    '{"repos": ["a"]}',
    '{"icechunk": {"a": ["r"]}}',      # old key names fail, rather than being ignored
    '{"vector_tiles": {"a": ["r"]}}',
])
def test_invalid_rules_fail(raw):
    with pytest.raises(ValueError):
        parse_access_rules(raw)


def test_empty_or_missing_rules_mean_open():
    for raw in ("{}", ""):
        rules = parse_access_rules(raw)
        assert rules.default is None
        assert rules.items == {"repos": {}, "layers": {}}
