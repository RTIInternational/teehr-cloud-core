"""Uploads (Phase 3): stage a file with a presigned URL, inspect it."""

import io
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.main import app
from src.staging import StagedMissing, get_staging
from src.uploads import dataset_name, detect_format
from tests.conftest import auth
from tests.test_datasets import ALICE, add_member
from tests.test_datasets import tables as tables  # noqa: F401  (fixture)

BOB = auth("bob")
UPLOADS = "/v1/projects/flood/uploads"


class FakeStaging:
    def __init__(self):
        self.objects: dict[str, bytes] = {}
        self.deleted: list[str] = []

    def upload_url(self, key, ttl_seconds):
        return f"https://staging.test/{key}?ttl={ttl_seconds}"

    def download_url(self, key, ttl_seconds):
        return f"https://staging.test/{key}?get"

    def size(self, key):
        if key not in self.objects:
            raise StagedMissing(key)
        return len(self.objects[key])

    def open(self, key):
        return pa.BufferReader(self.objects[key])

    def delete(self, key):
        self.deleted.append(key)
        self.objects.pop(key, None)

    def delete_prefix(self, prefix):
        keys = [k for k in self.objects if k.startswith(prefix)]
        for key in keys:
            self.delete(key)
        return len(keys)


@pytest.fixture
def staging(client, tables) -> FakeStaging:  # noqa: F811
    fake = FakeStaging()
    app.dependency_overrides[get_staging] = lambda: fake
    return fake


def create(client, filename="USGS obs.csv", headers=ALICE, **extra):
    return client.post(
        UPLOADS,
        json={"filename": filename, "kind": "timeseries", **extra},
        headers=headers,
    )


def staged(staging, upload, data: bytes):
    key = next(k for k in [f"flood/{upload['id']}/{upload['filename']}"])
    staging.objects[key] = data


def parquet_bytes(table: pa.Table) -> bytes:
    sink = io.BytesIO()
    pq.write_table(table, sink)
    return sink.getvalue()


CSV = b"""site_no,datetime,discharge,qualifiers
01010000,2026-01-01 00:00,12.5,P
01010000,2026-01-01 01:00,13.0,P
01010500,2026-01-01 00:00,7.25,A
"""


def test_create_returns_an_upload_url(client, staging):
    resp = create(client)
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["status"] == "staged" and body["format"] == "csv"
    assert body["upload_url"].startswith(
        f"https://staging.test/flood/{body['id']}/USGS obs.csv"
    )
    assert body["upload_url_expires_at"]
    # The URL is shown once.
    again = client.get(f"{UPLOADS}/{body['id']}", headers=ALICE).json()
    assert again["upload_url"] is None


def test_inspect_csv_proposes_a_mapping(client, staging):
    upload = create(client).json()
    staged(staging, upload, CSV)
    resp = client.post(f"{UPLOADS}/{upload['id']}/inspect", headers=ALICE)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "inspected" and body["size_bytes"] == len(CSV)
    preview = body["preview"]
    assert preview["rows"] == 3
    assert preview["mapping"] | {} == {
        "series_key": "site_no",
        "value_time": "datetime",
        "value": "discharge",
        "reference_time": None,
        "member": None,
        "quantile": None,
        "quality": "qualifiers",
        "series_key_value": None,
        "timezone": None,
    }
    assert preview["dataset"] == {"name": "usgs_obs"}
    assert "mapping.timezone" in preview["needed"]
    assert "dataset.variable" in preview["needed"]
    assert len(preview["sample"]) == 3
    # Read as text: leading zeros survive.
    assert preview["sample"][0]["site_no"] == "01010000"
    looks = {c["name"]: c["looks_like"] for c in preview["columns"]}
    assert looks == {
        "site_no": "integer", "datetime": "time", "discharge": "number",
        "qualifiers": "text",
    }  # fmt: skip
    assert [c["name"] for c in preview["columns"]] == [
        "site_no", "datetime", "discharge", "qualifiers"
    ]  # fmt: skip


def test_inspect_parquet_with_zoned_times(client, staging):
    table = pa.table(
        {
            "value_time": pa.array([0, 3_600_000_000], pa.timestamp("us", tz="UTC")),
            "flow_cms": [1.0, 2.0],
        }
    )
    upload = create(client, "one_gauge.parquet").json()
    staged(staging, upload, parquet_bytes(table))
    preview = client.post(f"{UPLOADS}/{upload['id']}/inspect", headers=ALICE).json()[
        "preview"
    ]
    assert preview["rows"] == 2
    assert preview["mapping"]["value_time"] == "value_time"
    assert preview["mapping"]["value"] == "flow_cms"  # the only numeric column
    assert preview["mapping"]["series_key"] is None
    assert "mapping.timezone" not in preview["needed"]
    assert any("series_key_value" in n for n in preview["needed"])
    assert preview["ingest"]["dataset"]["name"] == "one_gauge"
    assert preview["ingest"]["mapping"] == {
        "series_key_value": "<one series key for every row>"
    }


def test_inspect_before_the_file_is_there(client, staging):
    upload = create(client).json()
    resp = client.post(f"{UPLOADS}/{upload['id']}/inspect", headers=ALICE)
    assert resp.status_code == 409 and "PUT the file" in resp.json()["detail"]


def test_unreadable_file(client, staging):
    upload = create(client, "x.parquet").json()
    staged(staging, upload, b"not parquet")
    resp = client.post(f"{UPLOADS}/{upload['id']}/inspect", headers=ALICE)
    assert resp.status_code == 422


def test_too_large(client, staging):
    import dataclasses

    from src.config import get_settings

    settings = app.dependency_overrides[get_settings]()
    app.dependency_overrides[get_settings] = lambda: dataclasses.replace(
        settings, max_upload_bytes=10
    )
    upload = create(client).json()
    staged(staging, upload, CSV)
    resp = client.post(f"{UPLOADS}/{upload['id']}/inspect", headers=ALICE)
    assert resp.status_code == 413


def test_format_must_be_known(client, staging):
    assert create(client, "data.xlsx").status_code == 422
    assert create(client, "data.txt", format="csv").status_code == 201
    assert create(client, "a/b.csv").status_code == 422  # no paths


def test_access(client, staging):
    upload = create(client).json()
    staged(staging, upload, CSV)
    # Non-member: no such project.
    assert client.get(UPLOADS, headers=BOB).status_code == 404
    add_member(client, "bob", "read")
    assert client.get(UPLOADS, headers=BOB).status_code == 200
    assert client.get(f"{UPLOADS}/{upload['id']}", headers=BOB).status_code == 200
    assert create(client, headers=BOB).status_code == 403
    assert (
        client.post(f"{UPLOADS}/{upload['id']}/inspect", headers=BOB).status_code == 403
    )
    assert client.delete(f"{UPLOADS}/{upload['id']}", headers=BOB).status_code == 403


def test_list_and_delete(client, staging):
    first = create(client, "a.csv").json()
    second = create(client, "b.csv").json()
    staged(staging, first, CSV)
    listed = [u["id"] for u in client.get(UPLOADS, headers=ALICE).json()]
    assert listed == [second["id"], first["id"]]
    assert client.delete(f"{UPLOADS}/{first['id']}", headers=ALICE).status_code == 204
    assert staging.deleted == [f"flood/{first['id']}/a.csv"]
    assert client.get(f"{UPLOADS}/{first['id']}", headers=ALICE).status_code == 404


def test_other_projects_uploads_are_not_found(client, staging):
    upload = create(client).json()
    client.post("/v1/projects", json={"name": "other"}, headers=ALICE)
    resp = client.get(f"/v1/projects/other/uploads/{upload['id']}", headers=ALICE)
    assert resp.status_code == 404


def test_unconfigured_staging(client, tables):  # noqa: F811
    app.dependency_overrides[get_staging] = lambda: None
    assert create(client).status_code == 503


@pytest.mark.parametrize(
    ("filename", "name"),
    [
        ("USGS obs-2026.csv", "usgs_obs_2026"),
        ("2026.csv", "upload_2026"),
        ("..csv", "upload"),
    ],
)
def test_dataset_name(filename, name):
    assert dataset_name(filename) == name


def test_detect_format():
    assert detect_format("a.PARQUET") == "parquet"
    assert detect_format("a.pq") == "parquet"
    assert detect_format("a") is None


def test_deleting_a_project_removes_its_staged_files(client, staging):
    client.post("/v1/projects", json={"name": "scratch"}, headers=ALICE)
    staging.objects |= {"scratch/u1/a.csv": b"x", "scratchpad/u2/b.csv": b"y"}
    assert client.delete("/v1/projects/scratch", headers=ALICE).status_code == 204
    assert list(staging.objects) == ["scratchpad/u2/b.csv"]  # another project's


def test_a_project_with_a_loading_upload_cant_be_deleted(client, staging):
    from sqlalchemy.orm import Session

    from src.db import get_engine
    from src.models import Upload, UploadStatus

    upload = create(client).json()
    with Session(app.dependency_overrides[get_engine]()) as session:
        session.get(Upload, uuid.UUID(upload["id"])).status = UploadStatus.RUNNING
        session.commit()
    resp = client.delete("/v1/projects/flood", headers=ALICE)
    assert resp.status_code == 409 and "loading" in resp.json()["detail"]
