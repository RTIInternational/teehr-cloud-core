"""Access checks through the app (dashboard routes); run from xpublish-api/ with `python -m pytest tests`.

Token validation is replaced by a stub that reads the roles from the bearer token itself
(``Bearer roles:a,b``), and repo and layer listings are faked, so no Keycloak or S3 is needed.
A permitted request reaches the route and fails there (404 or 500) on the fake repos; a denied
one gets 403.
"""
import json
import os
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest
from botocore.exceptions import ClientError
from fastapi.testclient import TestClient

for key, value in {
    "ICECHUNK_BUCKET": "bkt",
    "ICECHUNK_PREFIX": "icechunk-ingests",
    "PMTILES_BUCKET": "bkt",
    "PMTILES_PREFIX": "vector-tiles",
    "ICECHUNK_STORAGE_MODE": "local",
    "ICECHUNK_ENDPOINT_URL": "http://local-s3",
}.items():
    os.environ.setdefault(key, value)

import src.main as main  # noqa: E402
from src.auth import AuthIdentity  # noqa: E402
from src.provider import IcechunkDatasetProvider, RepoConfig  # noqa: E402

RULES = {
    "default": ["icechunk-read-only"],
    "repos": {"isnobal_boise": ["admin"]},
    "layers": {"rfc-basins": ["admin"]},
}
REPOS = ["isnobal_boise", "ua_swann_4km"]
LAYERS = ["rfc-basins", "usgs-basins"]


class _StubValidator:
    async def validate(self, token):
        roles = token.removeprefix("roles:").split(",")
        return AuthIdentity(subject="u", auth_type="jwt", roles=[r for r in roles if r])


@pytest.fixture
def client(monkeypatch):
    with _client(monkeypatch) as test_client:
        yield test_client


@contextmanager
def _client(monkeypatch, sts_role_arn=""):
    """The app with fake repos, layers and tokens; STS is on when a role ARN is given."""
    monkeypatch.setattr(main.config, "TEEHR_ACCESS_RULES", json.dumps(RULES))
    monkeypatch.setattr(main.config, "ICECHUNK_ACCESS_ROLE_ARN", sts_role_arn)
    monkeypatch.setattr(main.config, "ICECHUNK_STS_UNAVAILABLE", not sts_role_arn)
    monkeypatch.setattr(
        IcechunkDatasetProvider,
        "_discover_repo_configs",
        lambda self: [RepoConfig(name=n, bucket="bkt", prefix=f"icechunk-ingests/{n}") for n in REPOS],
    )
    monkeypatch.setattr(main, "list_pmtiles_layers", lambda: [{"id": n, "source_layer": n} for n in LAYERS])
    monkeypatch.setattr(main, "KeycloakJWTValidator", _StubValidator)

    def no_storage(self, cfg):
        raise RuntimeError("no repo storage in tests")

    # A permitted request reaches the provider; fail there at once instead of trying S3
    monkeypatch.setattr(IcechunkDatasetProvider, "_open_repo", no_storage)
    with TestClient(main.build_app(), raise_server_exceptions=False) as test_client:
        yield test_client


def get(client, path, *roles):
    return client.get(path, headers={"Authorization": f"Bearer roles:{','.join(roles)}"})


USER = ("icechunk-read-only",)
ADMIN = ("icechunk-read-only", "admin")
NOACCESS = ("basic-user",)


def test_dataset_keys_lists_only_permitted_repos(client):
    assert get(client, "/api/dataset-keys", *ADMIN).json()["datasets"] == REPOS
    assert get(client, "/api/dataset-keys", *USER).json()["datasets"] == ["ua_swann_4km"]
    assert get(client, "/api/dataset-keys", *NOACCESS).json()["datasets"] == []


def test_vector_tiles_lists_only_permitted_layers(client):
    def ids(*roles):
        return [item["id"] for item in get(client, "/api/vector-tiles", *roles).json()["items"]]

    assert ids(*ADMIN) == LAYERS
    assert ids(*USER) == ["usgs-basins"]
    assert ids(*NOACCESS) == []


DATASET_PATHS = [
    "/api/datasets/{id}/coords/time",
    "/api/datasets/{id}/variable-attrs",
    "/api/datasets/{id}/tiles/WebMercatorQuad/3/2/1",
    "/api/datasets/{id}/tiles/legend",
    "/api/datasets/{id}/edr/position",
    "/api/dataset-variables/{id}",
]


@pytest.mark.parametrize("path", DATASET_PATHS)
@pytest.mark.parametrize("dataset_id", ["isnobal_boise", "isnobal_boise_data"])
def test_restricted_repo_is_403_on_every_route(client, path, dataset_id):
    url = path.format(id=dataset_id)
    assert get(client, url, *USER).status_code == 403
    assert get(client, url, *ADMIN).status_code != 403


@pytest.mark.parametrize("path", DATASET_PATHS)
def test_unlisted_repo_needs_the_default_role(client, path):
    url = path.format(id="ua_swann_4km")
    assert get(client, url, *USER).status_code != 403
    assert get(client, url, *NOACCESS).status_code == 403


@pytest.mark.parametrize("path", [
    "/api/datasets/ua_swann_4km/zarr/.zmetadata",   # not a route xpublish-api serves today
    "/api/datasets/ua_swann_4km/",
    "/api/datasets/ua_swann_4km",
    "/api/datasets",
])
def test_unrecognised_dataset_paths_fail_closed(client, path):
    assert get(client, path, *ADMIN).status_code == 403


def test_pmtiles_archive_checked_as_vector_tile(client):
    assert get(client, "/api/vector-tiles/rfc-basins.pmtiles", *USER).status_code == 403
    assert get(client, "/api/vector-tiles/usgs-basins.pmtiles", *NOACCESS).status_code == 403


@pytest.mark.parametrize("path", ["/api/vector-tiles/../datasets.pmtiles", "/api/vector-tiles/a/b.pmtiles"])
def test_odd_pmtiles_paths_never_read(client, path):
    assert get(client, path, *ADMIN).status_code in (400, 403, 404)


def test_no_token_is_401(client):
    assert client.get("/api/dataset-keys").status_code == 401
    assert client.get("/api/datasets/ua_swann_4km/variable-attrs").status_code == 401


def test_health_reveals_no_dataset_names(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_unmatched_layer_rule_is_warned_once(client, caplog, monkeypatch):
    monkeypatch.setattr(main, "list_pmtiles_layers", lambda: [{"id": "usgs-basins", "source_layer": "usgs-basins"}])
    with caplog.at_level("WARNING"):
        get(client, "/api/vector-tiles", *ADMIN)
        get(client, "/api/vector-tiles", *ADMIN)
    warnings = [r for r in caplog.records if "vector-tile layer(s) that don't exist" in r.getMessage()]
    assert len(warnings) == 1 and "rfc-basins" in warnings[0].getMessage()


# --- Notebook credentials route ---

CREDENTIALS = "/api/auth/icechunk-credentials"


def post(client, path, *roles):
    return client.post(path, headers={"Authorization": f"Bearer roles:{','.join(roles)}"})


@pytest.fixture
def sts(monkeypatch):
    """The app with STS on and a fake AssumeRole; yields (client, calls)."""
    calls = []

    def fake_vend(role_arn, identity, policy):
        calls.append({"role_arn": role_arn, "policy": policy})
        expiration = datetime.now(timezone.utc) + timedelta(seconds=900)
        return {"AccessKeyId": "AK", "SecretAccessKey": "SK", "SessionToken": "ST", "Expiration": expiration}

    monkeypatch.setattr(main, "vend_credentials", fake_vend)
    with _client(monkeypatch, sts_role_arn="arn:aws:iam::1:role/icechunk") as test_client:
        yield test_client, calls


def test_credentials_without_sts_list_repos_only(client):
    body = post(client, CREDENTIALS, *USER).json()
    assert body["credentials"] is None and body["repos"] == ["ua_swann_4km"]
    assert body["bucket"] == "bkt" and body["prefix"] == "icechunk-ingests" and body["trace_id"]
    assert post(client, CREDENTIALS, *ADMIN).json()["repos"] == REPOS
    assert post(client, CREDENTIALS, *NOACCESS).json()["repos"] == []


def test_credentials_need_a_token(client):
    assert client.post(CREDENTIALS).status_code == 401


def test_default_role_holder_gets_deny_mode_credentials(sts):
    client, calls = sts
    body = post(client, CREDENTIALS, *USER).json()
    assert body["repos"] == ["ua_swann_4km"]
    assert body["credentials"] == {"access_key_id": "AK", "secret_access_key": "SK", "session_token": "ST"}
    assert 0 < body["expires_in_seconds"] <= 900 and body["expires_at_epoch_seconds"]
    assert calls[0]["role_arn"] == "arn:aws:iam::1:role/icechunk"
    denied = [s for s in calls[0]["policy"]["Statement"] if s["Effect"] == "Deny" and s["Action"] == "s3:GetObject"]
    assert denied[0]["Resource"] == ["arn:aws:s3:::bkt/icechunk-ingests/isnobal_boise/*"]


def test_user_without_any_readable_repo_gets_no_credentials(sts):
    client, calls = sts
    body = post(client, CREDENTIALS, *NOACCESS).json()
    assert body["credentials"] is None and body["repos"] == []
    assert calls == []


def test_oversized_policy_is_a_clear_500(sts, monkeypatch):
    client, _ = sts

    def too_large(*args):
        raise main.PolicyTooLargeError("too many repo rules for one credential policy")

    monkeypatch.setattr(main, "vend_credentials", too_large)
    response = post(client, CREDENTIALS, *USER)
    assert response.status_code == 500 and "too many repo rules" in response.json()["detail"]


def test_sts_failure_is_a_502(sts, monkeypatch):
    client, _ = sts

    def denied(*args):
        raise ClientError({"Error": {"Code": "AccessDenied", "Message": "no"}}, "AssumeRole")

    monkeypatch.setattr(main, "vend_credentials", denied)
    response = post(client, CREDENTIALS, *USER)
    assert response.status_code == 502 and "trace_id" in response.json()["detail"]


def test_sts_without_a_role_fails_startup(monkeypatch):
    monkeypatch.setattr(main.config, "ICECHUNK_STS_UNAVAILABLE", False)
    monkeypatch.setattr(main.config, "ICECHUNK_ACCESS_ROLE_ARN", "")
    with pytest.raises(RuntimeError, match="ICECHUNK_ACCESS_ROLE_ARN"):
        main.build_app()
