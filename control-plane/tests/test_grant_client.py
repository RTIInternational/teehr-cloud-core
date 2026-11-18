"""The job's client rides out a control-plane restart (#226)."""

import httpx
import pytest

from src.jobs import load
from src.jobs.load import GrantClient


@pytest.fixture(autouse=True)
def clock(monkeypatch):
    """Sleeping advances a fake clock instantly."""
    now = [0.0]
    monkeypatch.setattr(load.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(load.time, "sleep", lambda s: now.__setitem__(0, now[0] + s))
    return now


def client(failures, error):
    """A client whose first ``failures`` requests fail with ``error``."""
    sent = []

    def handle(request):
        sent.append((request.method, request.url.path, request.url.params.get("mode")))
        if len(sent) <= failures:
            raise error("down", request=request)
        if request.url.path.endswith("/finish") and len(sent) > failures + 1:
            return httpx.Response(401)
        if request.url.path.endswith("/token"):
            return httpx.Response(200, json={"access_token": "t", "expires_in": 300})
        return httpx.Response(201 if request.method == "POST" else 200, json={})

    http = httpx.Client(transport=httpx.MockTransport(handle))
    job = GrantClient("http://cp", "tjg_x", http=http)
    job._token, job._expires = "t", 1e12  # a token in hand: tests aim at one call
    return job, sent


def test_a_call_that_never_reached_the_control_plane_is_sent_again():
    job, sent = client(3, httpx.ConnectError)
    assert job.api("POST", "/projects/p/datasets", json={}).status_code == 201
    assert sent == [("POST", "/v1/projects/p/datasets", None)] * 4  # 3 refused


@pytest.mark.parametrize("mode", ["overwrite", "upsert"])
def test_a_dropped_write_safe_to_repeat_is_sent_again(mode):
    job, sent = client(1, httpx.RemoteProtocolError)
    resp = job.api("POST", "/projects/p/datasets/d/data", params={"mode": mode})
    assert resp.status_code == 201 and len(sent) == 2


def test_a_dropped_token_call_is_sent_again():
    job, sent = client(1, httpx.RemoteProtocolError)
    assert job.token(fresh=True) == "t" and len(sent) == 2


def test_a_dropped_append_is_not_sent_again():
    job, sent = client(1, httpx.RemoteProtocolError)
    with pytest.raises(httpx.RemoteProtocolError):
        job.api("POST", "/projects/p/datasets/d/data", params={"mode": "append"})
    assert len(sent) == 1


def test_it_gives_up_after_its_time():
    job, sent = client(10**6, httpx.ConnectError)
    job.retry_seconds = 10  # waits 1, 2, 4 s, then 8 more would pass 10: gives up
    with pytest.raises(httpx.ConnectError):
        job.task()
    assert len(sent) == 4


def test_a_finish_whose_first_send_landed_is_done():
    """The first send dropped after landing: the grant stopped working, so the
    repeat is refused, and that's the end of it."""
    job, sent = client(0, httpx.RemoteProtocolError)
    job.finish("succeeded")  # 201
    job.finish("succeeded")  # 401: already finished
    assert [s[1] for s in sent] == ["/v1/job-grants/finish"] * 2
