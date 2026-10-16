"""Unit tests for notebook credential policies; run from xpublish-api/ with `python -m pytest tests`."""
import fnmatch
import json

import pytest

import src.credentials as credentials
from src.auth import AuthIdentity
from src.authz import parse_access_rules
from src.credentials import (
    MAX_SESSION_POLICY_CHARS,
    PolicyTooLargeError,
    build_session_policy,
    policy_for,
    session_name,
    vend_credentials,
)

BUCKET, PREFIX = "dev-fved-icechunk-warehouse-rti-use1", "icechunk-ingests"
RULES = parse_access_rules(json.dumps({
    "default": ["icechunk-read-only"],
    "icechunk": {"isnobal_boise": ["boise-snow-reader"], "staged": []},
}))


def user(*roles, username="user"):
    return AuthIdentity(subject="sub-1", auth_type="jwt", roles=list(roles), preferred_username=username)


def readable(policy, key):
    """Whether a session policy lets GetObject read ``key`` (deny wins, as in IAM)."""
    arn = f"arn:aws:s3:::{BUCKET}/{key}"

    def matches(statement):
        resources = statement["Resource"]
        resources = resources if isinstance(resources, list) else [resources]
        return statement["Action"] == "s3:GetObject" and any(fnmatch.fnmatchcase(arn, r) for r in resources)

    statements = policy["Statement"]
    if any(s["Effect"] == "Deny" and matches(s) for s in statements):
        return False
    return any(s["Effect"] == "Allow" and matches(s) for s in statements)


def test_default_role_holder_gets_deny_mode():
    assert policy_for(RULES, user("icechunk-read-only")) == (True, ["isnobal_boise", "staged"])
    assert policy_for(RULES, user("icechunk-read-only", "boise-snow-reader")) == (True, ["staged"])


def test_user_without_default_role_gets_allow_mode():
    assert policy_for(RULES, user("boise-snow-reader")) == (False, ["isnobal_boise"])
    assert policy_for(RULES, user("basic-user")) == (False, [])


def test_deny_mode_reads_everything_but_denied_repos():
    policy = build_session_policy(BUCKET, PREFIX, True, ["isnobal_boise", "staged"])
    assert readable(policy, f"{PREFIX}/ua_swann_4km/refs/branch.main")
    assert not readable(policy, f"{PREFIX}/isnobal_boise/refs/branch.main")
    assert not readable(policy, f"{PREFIX}/staged/chunks/abc")
    assert not readable(policy, "other-prefix/ua_swann_4km/x")


def test_deny_mode_without_restrictions_has_no_deny_statements():
    policy = build_session_policy(BUCKET, PREFIX, True, [])
    assert all(s["Effect"] == "Allow" for s in policy["Statement"])


def test_allow_mode_reads_only_granted_repos():
    policy = build_session_policy(BUCKET, PREFIX, False, ["isnobal_boise"])
    assert readable(policy, f"{PREFIX}/isnobal_boise/refs/branch.main")
    assert not readable(policy, f"{PREFIX}/ua_swann_4km/refs/branch.main")


def test_repo_prefixes_end_with_slash():
    """Restricting or granting isnobal_boise never affects isnobal_boise_v2."""
    denied = build_session_policy(BUCKET, PREFIX, True, ["isnobal_boise"])
    assert readable(denied, f"{PREFIX}/isnobal_boise_v2/refs/branch.main")
    granted = build_session_policy(BUCKET, PREFIX, False, ["isnobal_boise"])
    assert not readable(granted, f"{PREFIX}/isnobal_boise_v2/refs/branch.main")
    list_prefixes = [
        p for s in granted["Statement"] if s["Action"] == "s3:ListBucket"
        for p in s["Condition"]["StringLike"]["s3:prefix"]
    ]
    assert list_prefixes == [f"{PREFIX}/isnobal_boise/", f"{PREFIX}/isnobal_boise/*"]


def test_realistic_policy_fits_the_limit():
    policy = build_session_policy(BUCKET, PREFIX, True, [f"restricted_repo_{i:02d}" for i in range(8)])
    assert len(json.dumps(policy, separators=(",", ":"))) <= MAX_SESSION_POLICY_CHARS


def test_oversized_policy_fails_before_sts(monkeypatch):
    def no_sts():
        raise AssertionError("STS must not be called")

    monkeypatch.setattr(credentials, "_sts_client", no_sts)
    policy = build_session_policy(BUCKET, PREFIX, True, [f"restricted_repo_{i:02d}" for i in range(40)])
    with pytest.raises(PolicyTooLargeError):
        vend_credentials("arn:aws:iam::1:role/x", user("icechunk-read-only"), policy)


def test_assume_role_call(monkeypatch):
    calls = []

    class FakeSts:
        def assume_role(self, **kwargs):
            calls.append(kwargs)
            return {"Credentials": {"AccessKeyId": "a"}}

    monkeypatch.setattr(credentials, "_sts_client", FakeSts)
    policy = build_session_policy(BUCKET, PREFIX, True, ["isnobal_boise"])
    assert vend_credentials("arn:aws:iam::1:role/x", user("icechunk-read-only"), policy) == {"AccessKeyId": "a"}
    assert calls[0]["RoleArn"] == "arn:aws:iam::1:role/x"
    assert calls[0]["RoleSessionName"] == "user"
    assert calls[0]["DurationSeconds"] == 900
    assert json.loads(calls[0]["Policy"]) == policy


@pytest.mark.parametrize("username,expected", [
    ("sam.lamont@rti.org", "sam.lamont@rti.org"),
    ("first last", "first-last"),
    ("a", "user-a"),
    ("x" * 80, "x" * 64),
    (None, "sub-1"),
])
def test_session_name_fits_sts_rules(username, expected):
    assert session_name(user(username=username)) == expected
