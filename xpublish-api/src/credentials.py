"""Short-lived, read-only S3 credentials for notebooks, limited to the Icechunk repos a user may read.

xpublish-api assumes the Icechunk access role with a session policy built from the access rules,
so AWS itself enforces the rules for notebook reads. Two policy modes keep the policy small however
many repos exist (session policies are capped at 2,048 characters):

- deny mode, for users with a default role: allow the Icechunk prefix, deny the listed repos they
  can't read;
- allow mode, for users without one: allow only the listed repos granted to them.
"""
import json
import re
from functools import lru_cache

import boto3
from pydantic import BaseModel

from .auth import AuthIdentity
from .authz import AccessRules, has_default, readable_listed, unreadable_listed

# The STS minimum. Each renewal refreshes the user's Keycloak token, which keeps the SSO session
# alive (idle timeout 30 min); a TTL above the idle timeout would let it expire between renewals.
CREDENTIALS_TTL_SECONDS = 900
# AWS's limit for an inline session policy, checked here so the error is clear rather than an STS one
MAX_SESSION_POLICY_CHARS = 2048


class S3SessionCredentials(BaseModel):
    access_key_id: str
    secret_access_key: str
    session_token: str


class IcechunkCredentialsResponse(BaseModel):
    credentials: S3SessionCredentials | None  # None when STS is unavailable or no repo is readable
    expires_at_epoch_seconds: int | None
    expires_in_seconds: int | None
    bucket: str
    prefix: str
    repos: list[str]
    trace_id: str


class PolicyTooLargeError(Exception):
    """The session policy exceeds AWS's limit: too many repo rules for one credential policy."""


def _repo_paths(bucket: str, prefix: str, repo: str) -> tuple[str, list[str]]:
    # Always end with "/", so a rule for "isnobal_boise" never matches "isnobal_boise_v2"
    repo_prefix = f"{prefix}/{repo}/"
    return f"arn:aws:s3:::{bucket}/{repo_prefix}*", [repo_prefix, f"{repo_prefix}*"]


def build_session_policy(bucket: str, prefix: str, deny_mode: bool, repos: list[str]) -> dict:
    """Session policy for one user: deny mode denies ``repos``; allow mode allows only ``repos``."""
    bucket_arn = f"arn:aws:s3:::{bucket}"
    object_arns, list_prefixes = [], []
    for repo in repos:
        object_arn, prefixes = _repo_paths(bucket, prefix, repo)
        object_arns.append(object_arn)
        list_prefixes.extend(prefixes)

    if deny_mode:
        statements = [
            {"Effect": "Allow", "Action": "s3:GetObject", "Resource": f"{bucket_arn}/{prefix}/*"},
            {
                "Effect": "Allow",
                "Action": "s3:ListBucket",
                "Resource": bucket_arn,
                "Condition": {"StringLike": {"s3:prefix": [f"{prefix}/", f"{prefix}/*"]}},
            },
        ]
        if repos:
            statements += [
                {"Effect": "Deny", "Action": "s3:GetObject", "Resource": object_arns},
                {
                    "Effect": "Deny",
                    "Action": "s3:ListBucket",
                    "Resource": bucket_arn,
                    "Condition": {"StringLike": {"s3:prefix": list_prefixes}},
                },
            ]
    else:
        statements = [
            {"Effect": "Allow", "Action": "s3:GetObject", "Resource": object_arns},
            {
                "Effect": "Allow",
                "Action": "s3:ListBucket",
                "Resource": bucket_arn,
                "Condition": {"StringLike": {"s3:prefix": list_prefixes}},
            },
        ]
    return {"Version": "2012-10-17", "Statement": statements}


def policy_for(rules: AccessRules, identity: AuthIdentity) -> tuple[bool, list[str]]:
    """The policy mode (True for deny mode) and its repos for one user."""
    if has_default(rules, identity):
        return True, unreadable_listed(rules, identity, "repos")
    return False, readable_listed(rules, identity, "repos")


def session_name(identity: AuthIdentity) -> str:
    """STS session name, shown in CloudTrail; STS allows ``[\\w+=,.@-]{2,64}``."""
    name = re.sub(r"[^\w+=,.@-]", "-", identity.preferred_username or identity.subject)[:64]
    return name if len(name) >= 2 else f"user-{name}"


@lru_cache(maxsize=1)
def _sts_client():
    return boto3.client("sts")


def vend_credentials(role_arn: str, identity: AuthIdentity, policy: dict) -> dict:
    """Assume the Icechunk access role with the user's session policy; returns STS's Credentials."""
    policy_json = json.dumps(policy, separators=(",", ":"))
    if len(policy_json) > MAX_SESSION_POLICY_CHARS:
        raise PolicyTooLargeError(
            f"Session policy is {len(policy_json)} characters (limit {MAX_SESSION_POLICY_CHARS}); "
            "too many repo rules for one credential policy"
        )
    response = _sts_client().assume_role(
        RoleArn=role_arn,
        RoleSessionName=session_name(identity),
        Policy=policy_json,
        DurationSeconds=CREDENTIALS_TTL_SECONDS,
    )
    return response["Credentials"]
