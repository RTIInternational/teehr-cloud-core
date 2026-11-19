"""Personal tokens (ADR-0018): made by a device flow, used as bearer tokens."""

import time

import pytest
from sqlalchemy import select

from src.delegation import (
    DelegationError,
    DevicePending,
    DeviceStart,
    Tokens,
    get_delegation,
)
from src.main import app
from src.models import Job, PersonalToken
from src.routes import tokens as token_routes
from tests.conftest import make_token
from tests.test_datasets import ALICE
from tests.test_datasets import tables as tables  # noqa: F401  (fixture)
from tests.test_ingest import FakeDelegation, db, ingest, inspected
from tests.test_ingest import jobs as jobs  # noqa: F401  (fixture)
from tests.test_uploads import UPLOADS
from tests.test_uploads import staging as staging  # noqa: F401  (fixture)

ROLES = ("teehr-project-flood-admin",)


class FakeDevice(FakeDelegation):
    """Keycloak's device flow: pending until approved, then an offline session
    (once); refreshing it gives the user's token, roles and all."""

    def __init__(self):
        super().__init__()
        self.approved: str | bool | None = None
        self.used: set[str] = set()
        self.refreshes = 0
        self.started = 0

    def start_device(self, scope):
        assert scope == "openid offline_access"
        self.started += 1
        return DeviceStart(f"dc-{self.started}", "ABCD-EFGH", "https://kc/device",
                           "https://kc/device?user_code=ABCD-EFGH", 5, 600)  # fmt: skip

    def poll_device(self, device_code):
        if device_code in self.used or self.approved is False:
            raise DelegationError("denied or used")
        if self.approved is None:
            raise DevicePending()
        self.used.add(device_code)
        who = self.approved
        return Tokens(make_token(who, roles=ROLES), time.time() + 300,
                      f"offline-for-{who}")  # fmt: skip

    def refresh(self, refresh_token):
        if self.ended:
            raise DelegationError("session ended")
        self.refreshes += 1
        who = refresh_token.split("-for-")[1]
        return Tokens(make_token(who, roles=ROLES), time.time() + 300, refresh_token)


@pytest.fixture
def device(client, tables, jobs):  # noqa: F811
    fake = FakeDevice()
    app.dependency_overrides[get_delegation] = lambda: fake
    token_routes._starts.clear()
    return fake


def sign_in(client, device, who="alice", **body):
    flow = client.post("/v1/tokens/device", json={"name": "laptop", **body})
    assert flow.status_code == 200, flow.text
    flow = flow.json()
    pending = client.post("/v1/tokens/device/poll", json={"poll": flow["poll"]})
    assert pending.status_code == 202 and pending.json()["status"] == "pending"
    device.approved = who
    done = client.post("/v1/tokens/device/poll", json={"poll": flow["poll"]})
    assert done.status_code == 200, done.text
    return flow, done.json()


def bearer(secret):
    return {"Authorization": f"Bearer {secret}"}


def test_a_device_sign_in_makes_a_token_shown_once(client, device):
    flow, made = sign_in(client, device)
    assert flow["verification_uri_complete"].endswith("ABCD-EFGH")
    assert made["token"].startswith("tpat_") and made["username"] == "alice"
    assert made["scopes"] == ["read", "write"] and made["projects"] is None
    assert made["hint"] == made["token"][:9]
    # The flow can't be finished twice: the secret was shown once.
    again = client.post("/v1/tokens/device/poll", json={"poll": flow["poll"]})
    assert again.status_code == 400
    with db() as session:
        stored = session.scalar(select(PersonalToken))
        assert made["token"] not in stored.token_hash
        assert stored.session.kind == "personal" and stored.session.subject == "alice"
        assert "offline-for-alice" not in stored.session.refresh_token


def test_a_token_acts_as_its_user_through_a_cached_session(client, device):
    _, made = sign_in(client, device)
    for _ in range(3):
        resp = client.get("/v1/projects/flood", headers=bearer(made["token"]))
        assert resp.status_code == 200, resp.text
    assert device.refreshes == 0  # the access token from sign-in, cached
    listed = client.get("/v1/tokens", headers=bearer(made["token"])).json()
    assert [(t["name"], t["current"]) for t in listed] == [("laptop", True)]
    assert listed[0]["last_used_at"] is not None


def test_read_only_and_project_limits(client, device):
    _, reader = sign_in(client, device, scopes=["read"], projects=["flood"])
    assert (
        client.get("/v1/projects/flood", headers=bearer(reader["token"])).status_code
        == 200
    )
    resp = client.post("/v1/projects/flood/datasets", headers=bearer(reader["token"]),
                       json={"type": "table", "name": "t", "columns": []})  # fmt: skip
    assert resp.status_code == 403 and "read-only" in resp.json()["detail"]
    other = client.get("/v1/projects/drought", headers=bearer(reader["token"]))
    assert other.status_code == 403 and "drought" in other.json()["detail"]


def test_revoked_tokens_stop_working(client, device):
    _, made = sign_in(client, device)
    token = bearer(made["token"])
    assert client.delete(f"/v1/tokens/{made['id']}", headers=token).status_code == 204
    resp = client.get("/v1/projects/flood", headers=token)
    assert resp.status_code == 401
    with db() as session:
        stored = session.scalar(select(PersonalToken))
        assert stored.revoked_at is not None and stored.session.refresh_token is None


def test_others_tokens_are_hidden_but_admins_see_all(client, device):
    _, made = sign_in(client, device)
    bob = client.get(
        "/v1/tokens", headers={"Authorization": f"Bearer {make_token('bob')}"}
    )
    assert bob.json() == []
    assert client.delete(
        f"/v1/tokens/{made['id']}", headers={"Authorization": f"Bearer {make_token('bob')}"}
    ).status_code == 404  # fmt: skip
    assert client.get("/v1/tokens?all=true", headers=ALICE).status_code == 403
    admin = {"Authorization": f"Bearer {make_token('root', roles=('admin',))}"}
    assert [
        t["username"] for t in client.get("/v1/tokens?all=true", headers=admin).json()
    ] == ["alice"]


def test_denied_forged_and_expired_sign_ins(client, device):
    flow = client.post("/v1/tokens/device", json={"name": "x"}).json()
    device.approved = False
    assert (
        client.post("/v1/tokens/device/poll", json={"poll": flow["poll"]}).status_code
        == 400
    )
    forged = client.post("/v1/tokens/device/poll", json={"poll": "not-sealed"})
    assert forged.status_code == 400


def test_starting_sign_ins_is_rate_limited(client, device):
    for _ in range(token_routes.STARTS_PER_MINUTE):
        assert client.post("/v1/tokens/device", json={"name": "x"}).status_code == 200
    assert client.post("/v1/tokens/device", json={"name": "x"}).status_code == 429


def test_jobs_started_with_a_token_act_through_its_session(client, staging, device):  # noqa: F811
    _, made = sign_in(client, device)
    upload_id = inspected(client, staging)
    resp = ingest(client, upload_id, headers=bearer(made["token"]))
    assert resp.status_code == 202, resp.text
    assert device.exchanged == []  # no exchange: the token's session is used
    with db() as session:
        job = session.scalar(select(Job))
        token = session.scalar(select(PersonalToken))
        assert job.session_id == token.session_id
    # Ending the job leaves the token's session alone.
    resp = client.post(f"{UPLOADS}/{upload_id}/cancel", headers=bearer(made["token"]))
    assert resp.status_code == 200, resp.text
    with db() as session:
        assert session.scalar(select(Job)).status == "cancelled"
        assert session.scalar(select(PersonalToken)).session.revoked_at is None
