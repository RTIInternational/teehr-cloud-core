"""The API contract: strict inputs, stable operation ids, documented errors, and a
snapshot of the published spec (control-plane/openapi.json).

After a deliberate API change, regenerate the snapshot and review its diff:

    UPDATE_OPENAPI=1 .venv/bin/pytest tests/test_contract.py
"""

import json
import os
from pathlib import Path

import pytest

from src.main import app
from tests.conftest import auth

SNAPSHOT = Path(__file__).resolve().parents[1] / "openapi.json"
ALICE = auth("alice")

OPERATION_IDS = {
    "health",
    "ready",
    "start_token_sign_in",
    "finish_token_sign_in",
    "list_tokens",
    "get_me",
    "revoke_token",
    "read_run_results",
    "read_run_logs",
    "read_upload_logs",
    "list_metrics",
    "create_project",
    "list_projects",
    "get_project",
    "update_project",
    "delete_project",
    "provision_project",
    "list_members",
    "add_member",
    "set_member",
    "remove_member",
    "create_dataset",
    "list_datasets",
    "get_dataset",
    "update_dataset",
    "add_dataset_columns",
    "delete_dataset",
    "write_dataset",
    "list_dataset_writes",
    "read_dataset",
    "plan_run",
    "save_evaluation",
    "list_evaluations",
    "get_evaluation",
    "get_evaluation_version",
    "delete_evaluation",
    "start_run",
    "list_runs",
    "get_run",
    "cancel_run",
    "list_units",
    "add_unit",
    "list_periods",
    "add_period",
    "list_variables",
    "add_variable",
    "create_upload",
    "list_uploads",
    "get_upload",
    "inspect_upload",
    "delete_upload",
    "ingest_upload",
    "cancel_upload",
}


def _spec() -> dict:
    return app.openapi()


def _operations():
    for path, item in _spec()["paths"].items():
        for method, op in item.items():
            yield path, method, op


def test_unknown_field_is_rejected_not_ignored(client):
    resp = client.post(
        "/v1/projects", json={"name": "flood", "dispaly_name": "typo"}, headers=ALICE
    )
    assert resp.status_code == 422
    assert "dispaly_name" in resp.text
    # Nothing was created.
    assert client.get("/v1/projects/flood", headers=ALICE).status_code == 404


def test_unknown_field_rejected_on_member_bodies(client):
    client.post("/v1/projects", json={"name": "flood"}, headers=ALICE)
    resp = client.put(
        "/v1/projects/flood/members/bob",
        json={"role": "read", "rol": "admin"},
        headers=ALICE,
    )
    assert resp.status_code == 422


@pytest.mark.parametrize("bad", ["Flood", "flood-study", "1flood"])
def test_project_name_in_path_is_validated(client, bad):
    assert client.get(f"/v1/projects/{bad}", headers=ALICE).status_code == 422


def test_api_is_versioned():
    paths = set(_spec()["paths"])
    assert {p for p in paths if p not in ("/health", "/ready")} == {
        p for p in paths if p.startswith("/v1/")
    }


def test_operation_ids_are_stable_and_readable():
    assert {op["operationId"] for _, _, op in _operations()} == OPERATION_IDS


def test_every_api_operation_documents_its_errors():
    for path, method, op in _operations():
        if path in ("/health", "/ready"):
            continue
        responses = op["responses"]
        # Signing in to make a personal token needs no sign-in (ADR-0018).
        if not path.startswith("/v1/tokens/device"):
            assert "401" in responses, f"{method} {path} doesn't document 401"
        for code, body in responses.items():
            if code.startswith(("4", "5")):
                assert list(body["content"]) == ["application/problem+json"]
                ref = body["content"]["application/problem+json"]["schema"]["$ref"]
                want = "/ValidationErrorOut" if code == "422" else "/ErrorOut"
                assert ref.endswith(want), f"{method} {path} {code}"


def test_published_spec_matches_snapshot():
    current = json.dumps(_spec(), indent=2, sort_keys=True) + "\n"
    if os.getenv("UPDATE_OPENAPI"):
        SNAPSHOT.write_text(current)
    assert SNAPSHOT.exists(), "Run with UPDATE_OPENAPI=1 to create the snapshot"
    assert current == SNAPSHOT.read_text(), (
        "The API contract changed. If deliberate, regenerate with "
        "UPDATE_OPENAPI=1 and review the openapi.json diff."
    )


def test_errors_are_problem_details(client):
    resp = client.get("/v1/projects/nope", headers=ALICE)
    assert resp.status_code == 404
    assert resp.headers["content-type"] == "application/problem+json"
    assert resp.json() == {
        "type": "about:blank",
        "title": "Not Found",
        "status": 404,
        "detail": "Project not found",
    }


def test_validation_errors_list_every_problem(client):
    resp = client.post(
        "/v1/projects", json={"name": "Bad Name", "extra": 1}, headers=ALICE
    )
    assert resp.status_code == 422
    assert resp.headers["content-type"] == "application/problem+json"
    body = resp.json()
    assert body["title"] == "Unprocessable Content" and body["status"] == 422
    locs = {tuple(e["loc"]) for e in body["errors"]}
    assert {("body", "name"), ("body", "extra")} <= locs


def test_unauthenticated_is_a_problem_with_www_authenticate(client):
    resp = client.get("/v1/projects")
    assert resp.status_code == 401
    assert resp.headers["www-authenticate"] == "Bearer"
    assert resp.json()["title"] == "Unauthorized"


def test_titles_dont_depend_on_the_python_version():
    """Python 3.12 calls 422 "Unprocessable Entity"; the API always says Content."""
    import json

    from src.errors import problem

    for status, title in ((422, "Unprocessable Content"), (413, "Content Too Large")):
        assert json.loads(problem(status).body)["title"] == title
