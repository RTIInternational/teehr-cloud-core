"""Access checks through the app (dashboard routes); run from xpublish-api/ with `python -m pytest tests`.

Token validation is replaced by a stub that reads the roles from the bearer token itself
(``Bearer roles:a,b``), and repo and layer listings are faked, so no Keycloak or S3 is needed.
A permitted request reaches the route and fails there (404 or 500) on the fake repos; a denied
one gets 403.
"""
import json
import os

import pytest
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
    "icechunk": {"isnobal_boise": ["admin"]},
    "vector_tiles": {"rfc-basins": ["admin"]},
}
REPOS = ["isnobal_boise", "ua_swann_4km"]
LAYERS = ["rfc-basins", "usgs-basins"]


class _StubValidator:
    async def validate(self, token):
        roles = token.removeprefix("roles:").split(",")
        return AuthIdentity(subject="u", auth_type="jwt", roles=[r for r in roles if r])


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(main.config, "TEEHR_ACCESS_RULES", json.dumps(RULES))
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
@pytest.mark.parametrize("dataset_id", ["isnobal_boise", "isnobal_boise_raw_data"])
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
