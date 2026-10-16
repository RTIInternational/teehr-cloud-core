#!/usr/bin/env python3
"""
Integration test: Icechunk access rules enforced by xpublish-api (local KinD).

Creates two throwaway Icechunk repos, authz_test_open and authz_test_restricted, then checks
what each test persona can see and open through xpublish-api with real Keycloak tokens:
dataset listings, dataset routes (plus _raw_data ids), the notebook credentials route, 401s,
the fail-closed paths and /health.

Expected access is worked out from the deployed rules (TEEHR_ACCESS_RULES, read from
xpublish-api's ConfigMap), so the test fits any deployment's rules. To exercise a restriction,
the deployment's local rules should restrict authz_test_restricted, e.g. to ["admin"]; without
that rule the restricted-repo checks are skipped. The repos are deleted at the end.
"""

import base64
import json
import os
import sys
import time

import botocore.session
import icechunk as ic
import requests
from botocore import UNSIGNED
from botocore.config import Config

KEYCLOAK_URL = "http://keycloak-service:8080"
REALM = "teehr"
XPUBLISH_URL = os.getenv("XPUBLISH_API_URL", "http://xpublish-api:8000")
S3_ENDPOINT = os.getenv("REMOTE_CATALOG_S3_ENDPOINT", "http://local-s3:9000")
BUCKET = os.getenv("ICECHUNK_BUCKET", "warehouse")
PREFIX = os.getenv("ICECHUNK_PREFIX", "icechunk-ingests")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "localadmin")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "localadmin123")
os.environ.setdefault("AWS_REGION", "us-east-1")
RULES = json.loads(os.getenv("TEEHR_ACCESS_RULES") or "{}")

OPEN_REPO = "authz_test_open"
RESTRICTED_REPO = "authz_test_restricted"
TEST_REPOS = [OPEN_REPO, RESTRICTED_REPO]
PERSONAS = ["admin", "poweruser", "user", "noaccess"]
# Repos are discovered on a TTL (REPO_DISCOVERY_TTL, default 60 s)
DISCOVERY_TIMEOUT = 150

failures = []


def check(ok: bool, message: str) -> None:
    print(f"    {'✓' if ok else '✗ ERROR:'} {message}")
    if not ok:
        failures.append(message)


# --- Setup helpers ---

def s3_client(anonymous: bool = False):
    session = botocore.session.get_session()
    config = Config(s3={"addressing_style": "path"}, signature_version=UNSIGNED if anonymous else None)
    kwargs = {} if anonymous else {
        "aws_access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
    }
    return session.create_client("s3", endpoint_url=S3_ENDPOINT, region_name=os.environ["AWS_REGION"],
                                 config=config, **kwargs)


def create_repo(name: str) -> None:
    storage = ic.s3_storage(
        bucket=BUCKET, prefix=f"{PREFIX}/{name}", endpoint_url=S3_ENDPOINT, allow_http=True,
        force_path_style=True, from_env=True, region=os.environ["AWS_REGION"],
    )
    ic.Repository.open_or_create(storage)


def delete_repo(name: str) -> None:
    s3 = s3_client()
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=f"{PREFIX}/{name}/"):
        keys = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
        if keys:
            s3.delete_objects(Bucket=BUCKET, Delete={"Objects": keys})


def get_token(username: str) -> str:
    response = requests.post(
        f"{KEYCLOAK_URL}/realms/{REALM}/protocol/openid-connect/token",
        data={
            "grant_type": "password",
            "client_id": "jupyterhub",
            "client_secret": os.getenv("JUPYTERHUB_CLIENT_SECRET", "local-jupyterhub-client-secret"),
            "username": username,
            "password": username,
        },
        timeout=20,
    )
    response.raise_for_status()
    return response.json()["access_token"]


def realm_roles(token: str) -> list[str]:
    payload = token.split(".")[1]
    payload += "=" * (-len(payload) % 4)
    return json.loads(base64.urlsafe_b64decode(payload)).get("realm_access", {}).get("roles", [])


def call(method: str, path: str, token: str | None = None) -> requests.Response:
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    return requests.request(method, f"{XPUBLISH_URL}{path}", headers=headers, timeout=30)


# --- Expected access, from the deployed rules (see docs/icechunk-access-control.md) ---

def expected_readable(kind: str, name: str, roles: list[str]) -> bool:
    listed = RULES.get(kind, {}).get(name)
    if listed is not None:
        return any(role in roles for role in listed)
    default = RULES.get("default")
    return default is None or any(role in roles for role in default)


# --- Checks ---

def wait_for_discovery(tokens: dict[str, str]) -> bool:
    """Wait until xpublish-api lists the open test repo for a persona expected to see it."""
    viewer = next((p for p in PERSONAS if expected_readable("repos", OPEN_REPO, realm_roles(tokens[p]))), None)
    if viewer is None:
        print(f"  (no persona may read {OPEN_REPO}; not waiting for discovery)")
        time.sleep(70)
        return True
    deadline = time.monotonic() + DISCOVERY_TIMEOUT
    while time.monotonic() < deadline:
        if OPEN_REPO in call("GET", "/api/dataset-keys", get_token(viewer)).json().get("datasets", []):
            return True
        time.sleep(5)
    return False


def check_persona(persona: str, token: str) -> None:
    roles = realm_roles(token)
    print(f"\n  {persona}: roles {sorted(r for r in roles if r.startswith(('icechunk-', 'teehr-', 'admin')))}")
    if persona == "noaccess":
        check("icechunk-read-only" not in roles and "teehr-read-only" not in roles,
              "noaccess token has no data roles (the default groups weren't added)")

    expected = {repo: expected_readable("repos", repo, roles) for repo in TEST_REPOS}
    listed = set(call("GET", "/api/dataset-keys", token).json()["datasets"]) & set(TEST_REPOS)
    check(listed == {r for r, ok in expected.items() if ok}, f"/api/dataset-keys lists {sorted(listed)}")

    credentials = call("POST", "/api/auth/icechunk-credentials", token)
    body = credentials.json()
    vended = set(body["repos"]) & set(TEST_REPOS)
    check(credentials.status_code == 200 and vended == {r for r, ok in expected.items() if ok},
          f"credentials route repos {sorted(vended)}, trace_id present: {bool(body.get('trace_id'))}")

    for repo in TEST_REPOS:
        if repo == RESTRICTED_REPO and RESTRICTED_REPO not in RULES.get("repos", {}):
            continue
        for dataset_id in (repo, f"{repo}_raw_data"):
            for path in (
                f"/api/datasets/{dataset_id}/variable-attrs",
                f"/api/datasets/{dataset_id}/coords/time",
                f"/api/datasets/{dataset_id}/tiles/WebMercatorQuad/0/0/0?variables=x",
                f"/api/datasets/{dataset_id}/edr/position?coords=POINT(0%200)",
                f"/api/dataset-variables/{dataset_id}",
            ):
                status = call("GET", path, token).status_code
                if expected[repo]:
                    check(status not in (401, 403), f"{path} allowed (got {status}, not 401/403)")
                else:
                    check(status == 403, f"{path} denied (got {status})")

    layers = call("GET", "/api/vector-tiles", token)
    for item in layers.json().get("items", []):
        check(expected_readable("layers", item["id"], roles),
              f"vector-tile layer {item['id']} listed and permitted")


def check_unauthenticated(admin_token: str) -> None:
    print("\n  Without a valid token / fail-closed paths:")
    for method, path in (("GET", "/api/dataset-keys"), ("GET", f"/api/datasets/{OPEN_REPO}/variable-attrs"),
                         ("POST", "/api/auth/icechunk-credentials")):
        check(call(method, path).status_code == 401, f"{method} {path} without a token is 401")
    check(call("GET", "/api/dataset-keys", "not.a.jwt").status_code == 401, "malformed token is 401")

    health = call("GET", "/health")
    check(health.status_code == 200 and health.json() == {"status": "ok"},
          f"/health is 200 with no dataset names: {health.text}")
    check(call("GET", "/api/datasets", admin_token).status_code == 403, "GET /api/datasets (every dataset) is 403")
    check(call("GET", f"/api/datasets/{OPEN_REPO}/zarr/.zmetadata", admin_token).status_code == 403,
          "unrecognised dataset route is 403 (fail closed)")
    traversal = call("GET", "/api/vector-tiles/..%2Fsecret.pmtiles", admin_token).status_code
    check(traversal in (400, 403, 404), f"path traversal in a layer name is never read (got {traversal})")

    try:
        s3_client(anonymous=True).list_objects_v2(Bucket=BUCKET, Prefix=f"{PREFIX}/{OPEN_REPO}/")
        check(False, "anonymous S3 listing of a test repo should fail")
    except Exception as exc:
        check(True, f"anonymous S3 listing of a test repo fails ({type(exc).__name__})")


def main() -> int:
    print(f"Access rules under test: {json.dumps(RULES)}")
    if RESTRICTED_REPO not in RULES.get("repos", {}):
        print(f"  NOTE: rules don't restrict {RESTRICTED_REPO}; its deny checks are skipped")
    try:
        for repo in TEST_REPOS:
            create_repo(repo)
        tokens = {persona: get_token(persona) for persona in PERSONAS}
        print("Waiting for xpublish-api to discover the test repos...")
        if not wait_for_discovery(tokens):
            check(False, f"xpublish-api did not list {OPEN_REPO} within {DISCOVERY_TIMEOUT}s")
            return 1
        # Tokens last 5 minutes; refresh them after the wait
        tokens = {persona: get_token(persona) for persona in PERSONAS}
        for persona in PERSONAS:
            check_persona(persona, tokens[persona])
        check_unauthenticated(tokens["admin"])
    finally:
        for repo in TEST_REPOS:
            delete_repo(repo)

    print(f"\n{'All checks passed' if not failures else f'{len(failures)} check(s) failed'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
