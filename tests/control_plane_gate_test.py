#!/usr/bin/env python3
"""
Integration test: Phase 1 gate - the project control plane (#148, #157).

Every caller is a real Keycloak user (or the ingest service account) using its own
token, the way Spark and Trino do. Validates:
- A user creates a project through the API and gets its own Polaris namespace and
  storage prefix.
- A non-member is denied, in the control plane and in Polaris.
- A member is limited to their own project: Polaris refuses other projects, and the
  storage credentials Polaris vends reach only the member's own table prefix.
- Membership is access: adding a reader grants it, removing them takes it away.
- The system projects: every user reads them, only ingest writes them.

Leaves nothing behind: tables and projects are removed at the end.
"""

import sys
import time
import uuid

import requests
from pyarrow import fs

KEYCLOAK_TOKEN_URL = (
    "http://keycloak-service:8080/realms/teehr/protocol/openid-connect/token"
)
CONTROL_PLANE = "http://teehr-control-plane:8000/v1/projects"
POLARIS = "http://polaris:8181/api/catalog/v1/teehr"
REALM = "teehr"
S3_ENDPOINT = "http://local-s3:9000"
BUCKET = "warehouse"
SCHEMA = {
    "type": "struct",
    "schema-id": 0,
    "fields": [{"id": 1, "name": "x", "type": "long", "required": False}],
}
MAX_RETRIES = 30
RETRY_DELAY = 2

results: list[bool] = []


def check(label, ok, detail=""):
    results.append(bool(ok))
    print(f"  {'✅' if ok else '❌'} {label}" + (f"  [{detail}]" if detail else ""))


def user_token(username):
    resp = requests.post(
        KEYCLOAK_TOKEN_URL,
        data={
            "grant_type": "password",
            "client_id": "jupyterhub",
            "client_secret": "local-jupyterhub-client-secret",
            "username": username,
            "password": username,
            "scope": "openid",
        },
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def ingest_token():
    resp = requests.post(
        KEYCLOAK_TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": "prefect-polaris",
            "client_secret": "local-prefect-polaris-client-secret",
        },
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def cp(username, method, path="", **kwargs):
    """Call the control plane as ``username`` (fresh token each time)."""
    headers = {"Authorization": f"Bearer {user_token(username)}"}
    return requests.request(
        method, CONTROL_PLANE + path, headers=headers, timeout=60, **kwargs
    )


def polaris(token, method, path, **kwargs):
    headers = {"Authorization": f"Bearer {token}", "X-Polaris-Realm": REALM}
    headers.update(kwargs.pop("headers", {}))
    return requests.request(method, POLARIS + path, headers=headers, timeout=30, **kwargs)


def create_table(token, namespace, name):
    return polaris(
        token,
        "POST",
        f"/namespaces/{namespace}/tables",
        json={"name": name, "schema": SCHEMA},
    )


def vended_credentials(token, namespace, table):
    """Load a table asking Polaris to vend storage credentials scoped to it."""
    resp = polaris(
        token,
        "GET",
        f"/namespaces/{namespace}/tables/{table}",
        headers={"X-Iceberg-Access-Delegation": "vended-credentials"},
    )
    resp.raise_for_status()
    body = resp.json()
    config = dict(body.get("config", {}))
    for cred in body.get("storage-credentials", []):
        config.update(cred.get("config", {}))
    return body["metadata"]["location"], config


def s3_with(config):
    endpoint = config.get("s3.endpoint", S3_ENDPOINT)
    return fs.S3FileSystem(
        access_key=config["s3.access-key-id"],
        secret_key=config["s3.secret-access-key"],
        session_token=config.get("s3.session-token"),
        endpoint_override=endpoint.split("://", 1)[-1],
        scheme=endpoint.split("://", 1)[0] if "://" in endpoint else "http",
        region=config.get("client.region", "us-east-1"),
        allow_bucket_creation=False,
    )


def s3_allowed(op):
    try:
        op()
        return True
    except OSError as exc:
        if "ACCESS_DENIED" in str(exc).upper() or "AccessDenied" in str(exc):
            return False
        raise


def wait_for_control_plane():
    for attempt in range(MAX_RETRIES):
        try:
            if requests.get(
                CONTROL_PLANE.rsplit("/v1", 1)[0] + "/ready", timeout=5
            ).ok:
                return
        except requests.RequestException:
            pass
        print(f"  Attempt {attempt + 1}/{MAX_RETRIES}: control plane not ready...")
        time.sleep(RETRY_DELAY)
    raise RuntimeError("control plane never became ready")


def project_gate(a, b):
    print("\n🏗️  Projects through the API")
    r = cp("user", "POST", json={"name": a})
    body = r.json()
    check(
        "user creates a project and it is provisioned",
        r.status_code == 201 and body.get("status") == "ready",
        f"{r.status_code} {body.get('status')} {body.get('status_detail') or ''}",
    )
    check(
        "it gets its own storage prefix",
        (body.get("storage_location") or "").rstrip("/").endswith(f"/{a}"),
        body.get("storage_location"),
    )
    r = cp("noaccess", "POST", json={"name": b})
    check("a second user creates another project", r.status_code == 201, str(r.status_code))
    user = user_token("user")
    check(
        "the creator sees their namespace in Polaris",
        polaris(user, "GET", f"/namespaces/{a}").status_code == 200,
    )


def non_member_gate(a):
    print("\n🚫 Non-members are denied")
    r = cp("noaccess", "GET", f"/{a}")
    check("non-member gets 404 from the control plane", r.status_code == 404, str(r.status_code))
    listed = {p["name"] for p in cp("noaccess", "GET").json()}
    check("non-member doesn't see it listed", a not in listed)
    other = user_token("noaccess")
    r = polaris(other, "GET", f"/namespaces/{a}/tables")
    check("non-member is denied by Polaris", r.status_code in (403, 404), str(r.status_code))
    r = create_table(other, a, "intruder")
    check("non-member can't create a table in it", r.status_code in (403, 404), str(r.status_code))


def isolation_gate(a, b):
    print("\n🔒 A member is limited to their own project")
    user, other = user_token("user"), user_token("noaccess")
    r = create_table(user, a, "t_a")
    check("member creates a table in their project", r.status_code == 200, str(r.status_code))
    r = create_table(other, b, "t_b")
    check("the other user creates a table in theirs", r.status_code == 200, str(r.status_code))
    r = polaris(user, "GET", f"/namespaces/{b}/tables/t_b")
    check("member can't load another project's table", r.status_code in (403, 404), str(r.status_code))

    location, config = vended_credentials(user, a, "t_a")
    check("Polaris vends scoped credentials", "s3.session-token" in config)
    own = location.split("://", 1)[-1].rstrip("/")
    check("the table lives under the project prefix", f"/{a}/" in own + "/", location)
    s3 = s3_with(config)
    probe = f"{own}/data/gate-{uuid.uuid4().hex[:6]}.txt"

    def write_own():
        with s3.open_output_stream(probe) as out:
            out.write(b"gate")

    check("vended creds write under the member's own table", s3_allowed(write_own))
    check(
        "vended creds read it back",
        s3_allowed(lambda: s3.open_input_stream(probe).read()),
    )
    _, other_config = vended_credentials(other, b, "t_b")
    other_prefix = f"{BUCKET}/{b}"
    for label, target in [
        ("another project's prefix", other_prefix),
        ("the legacy teehr prefix", f"{BUCKET}/teehr"),
        ("the bucket root", BUCKET),
    ]:
        check(
            f"vended creds can't list {label}",
            not s3_allowed(
                lambda t=target: s3.get_file_info(fs.FileSelector(t, recursive=False))
            ),
        )
    sneaky = f"{other_prefix}/gate-{uuid.uuid4().hex[:6]}.txt"

    def write_other():
        with s3.open_output_stream(sneaky) as out:
            out.write(b"nope")

    check("vended creds can't write into another project", not s3_allowed(write_other))
    s3.delete_file(probe)
    return other_config


def membership_gate(a):
    print("\n👥 Membership is access")
    r = cp("user", "POST", f"/{a}/members", json={"username": "noaccess", "role": "read"})
    check("owner adds a reader by username", r.status_code == 201, f"{r.status_code} {r.text[:100]}")
    subject = r.json().get("subject") if r.ok else None
    reader = user_token("noaccess")
    r = polaris(reader, "GET", f"/namespaces/{a}/tables/t_a")
    check("the reader can now load the table", r.status_code == 200, str(r.status_code))
    r = create_table(reader, a, "t_reader")
    check("but can't create one", r.status_code == 403, str(r.status_code))
    r = cp("user", "DELETE", f"/{a}/members/{subject}")
    check("owner removes the reader", r.status_code == 204, str(r.status_code))
    r = polaris(user_token("noaccess"), "GET", f"/namespaces/{a}/tables/t_a")
    check("removed reader is denied again, immediately", r.status_code in (403, 404), str(r.status_code))


def system_projects_gate():
    print("\n🌐 System projects")
    listed = {p["name"]: p for p in cp("user", "GET").json()}
    for name in ("public", "platform"):
        p = listed.get(name, {})
        check(
            f"every user sees '{name}', ready",
            p.get("kind") == "system" and p.get("status") == "ready",
            f"{p.get('kind')} {p.get('status')}",
        )
    ingest, user = ingest_token(), user_token("user")
    table = f"gate_{uuid.uuid4().hex[:8]}"
    r = create_table(ingest, "public", table)
    check("ingest writes public", r.status_code == 200, str(r.status_code))
    r = polaris(user, "GET", f"/namespaces/public/tables/{table}")
    check("a user reads public", r.status_code == 200, str(r.status_code))
    r = create_table(user, "public", table + "_u")
    check("a user can't write public", r.status_code == 403, str(r.status_code))
    r = create_table(user_token("poweruser"), "public", table + "_p")
    check("nor can a legacy teehr writer", r.status_code == 403, str(r.status_code))
    r = polaris(ingest, "DELETE", f"/namespaces/public/tables/{table}?purgeRequested=true")
    check("ingest drops and purges its public table", r.status_code == 204, str(r.status_code))


def cleanup(a, b):
    """Drop and purge the tables (their files too), then delete the projects
    through the API."""
    print("\n🧹 Cleanup")
    user, other = user_token("user"), user_token("noaccess")
    for token, ns, table in [(user, a, "t_a"), (other, b, "t_b")]:
        r = polaris(token, "DELETE", f"/namespaces/{ns}/tables/{table}?purgeRequested=true")
        check(f"member drops and purges {ns}.{table}", r.status_code == 204, str(r.status_code))
    for owner, name in [("user", a), ("noaccess", b)]:
        r = cp(owner, "DELETE", f"/{name}")
        check(f"owner deletes project {name}", r.status_code == 204, str(r.status_code))


def main():
    print("🧪 Phase 1 gate: project control plane")
    wait_for_control_plane()
    suffix = uuid.uuid4().hex[:8]
    a, b = f"gate_a_{suffix}", f"gate_b_{suffix}"
    try:
        project_gate(a, b)
        non_member_gate(a)
        isolation_gate(a, b)
        membership_gate(a)
        system_projects_gate()
    finally:
        cleanup(a, b)
    passed = sum(results)
    print(f"\n{passed}/{len(results)} checks passed")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
