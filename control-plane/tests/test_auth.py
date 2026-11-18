"""Token checks on a protected route."""

from cryptography.hazmat.primitives.asymmetric import rsa

from tests.conftest import auth


def test_no_token_is_401(client):
    resp = client.get("/v1/projects")
    assert resp.status_code == 401
    assert resp.headers["www-authenticate"] == "Bearer"


def test_valid_token_is_accepted(client):
    assert client.get("/v1/projects", headers=auth("alice")).status_code == 200


def test_azp_in_allow_list_is_accepted(client):
    headers = auth("alice", aud="account", azp="teehr-frontend")
    assert client.get("/v1/projects", headers=headers).status_code == 200


def test_audience_not_allowed_is_401(client):
    headers = auth("alice", aud="someone-else", azp="someone-else")
    assert client.get("/v1/projects", headers=headers).status_code == 401


def test_wrong_issuer_is_401(client):
    headers = auth("alice", issuer="https://evil.test/realms/teehr")
    assert client.get("/v1/projects", headers=headers).status_code == 401


def test_expired_token_is_401(client):
    headers = auth("alice", expires_in=-60)
    assert client.get("/v1/projects", headers=headers).status_code == 401


def test_token_signed_by_another_key_is_401(client):
    other = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    headers = auth("alice", key=other)
    assert client.get("/v1/projects", headers=headers).status_code == 401
