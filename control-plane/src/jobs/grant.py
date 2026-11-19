"""A job's side of its grant (ADR-0014): the control plane's calls for the job --
its task, the caller's tokens, progress, outcome -- and the API as the caller."""

import time
from dataclasses import dataclass

import httpx

# How long a job keeps trying to reach the control plane (a restart takes ~30 s).
RETRY_SECONDS = 120.0
# The request never reached the control plane: any call can be sent again.
_NOT_SENT = (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout)
# The connection dropped mid-call: it may have landed, so only calls that are safe
# to repeat are sent again.
_DROPPED = (httpx.RemoteProtocolError, httpx.ReadError, httpx.WriteError)
_WRITE_MODES_SAFE_TO_REPEAT = ("overwrite", "upsert")


class LoadFailed(Exception):
    """The load can't go ahead; ``problems`` says why."""

    def __init__(self, problems: list[str]):
        super().__init__("; ".join(problems))
        self.problems = problems


@dataclass
class GrantClient:
    """The job's side of its grant: tokens, task, progress, outcome."""

    base_url: str
    grant: str
    http: httpx.Client | None = None
    retry_seconds: float = RETRY_SECONDS

    def __post_init__(self):
        self.http = self.http or httpx.Client(timeout=120)
        self._token, self._expires = "", 0.0

    def _send(
        self, method: str, url: str, repeatable: bool, **kwargs
    ) -> httpx.Response:
        """One request, sent again while the control plane can't be reached (it
        may be restarting): always when the request never reached it, and when the
        connection dropped mid-call only if ``repeatable``."""
        deadline, wait = time.monotonic() + self.retry_seconds, 1.0
        while True:
            try:
                return self.http.request(method, url, **kwargs)
            except (*_NOT_SENT, *_DROPPED) as exc:
                if isinstance(exc, _DROPPED) and not repeatable:
                    raise
                if time.monotonic() + wait > deadline:
                    raise
                time.sleep(wait)
                wait = min(wait * 2, 15.0)

    def _grant(self, method: str, path: str, **kwargs) -> httpx.Response:
        # Every grant call is safe to repeat: a finish that landed before the
        # connection dropped makes the grant stop working, so its repeat is refused
        # (and the outcome is already recorded).
        resp = self._send(
            method,
            f"{self.base_url}/v1/job-grants/{path}",
            True,
            headers={"Authorization": f"Bearer {self.grant}"},
            **kwargs,
        )
        resp.raise_for_status()
        return resp

    def download(self, url: str, file) -> None:
        """Copy the staged file into ``file``."""
        with self.http.stream("GET", url) as resp:
            if resp.status_code != 200:
                raise LoadFailed(
                    [f"couldn't read the staged file (HTTP {resp.status_code})"]
                )
            for part in resp.iter_bytes(1024 * 1024):
                file.write(part)

    def put(self, url: str, data: bytes) -> None:
        """PUT a work file to its (presigned) URL; repeated if the send drops."""
        resp = self._send("PUT", url, True, content=data)
        if resp.status_code not in (200, 201, 204):
            raise LoadFailed([f"couldn't store a work file (HTTP {resp.status_code})"])

    def get(self, url: str) -> bytes:
        """GET a work file from its (presigned) URL."""
        resp = self._send("GET", url, True)
        if resp.status_code != 200:
            raise LoadFailed([f"couldn't read a work file (HTTP {resp.status_code})"])
        return resp.content

    def task(self) -> dict:
        return self._grant("GET", "task").json()

    def token(self, fresh: bool = False) -> str:
        if fresh or time.time() > self._expires - 60:
            body = self._grant("POST", "token").json()
            self._token, self._expires = (
                body["access_token"],
                time.time() + body["expires_in"],
            )
        return self._token

    def progress(self, rows_done: int) -> None:
        self._grant("POST", "progress", json={"rows_done": rows_done})

    def finish(self, status: str, **outcome) -> None:
        try:
            self._grant("POST", "finish", json={"status": status, **outcome})
        except httpx.HTTPStatusError as exc:
            # Refused: the job has already ended (an earlier send of this landed
            # before the connection dropped, or it was cancelled). Nothing to record.
            if exc.response.status_code != 401:
                raise

    def api(self, method: str, path: str, **kwargs) -> httpx.Response:
        """Call the control plane's API as the caller (one retry with a fresh
        token if it's refused). Reads, overwrites and upserts are sent again if the
        connection drops mid-call; appends and creates aren't (they may have
        landed)."""
        extra = kwargs.pop("headers", {})
        mode = (kwargs.get("params") or {}).get("mode")
        repeatable = method == "GET" or mode in _WRITE_MODES_SAFE_TO_REPEAT
        for fresh in (False, True):
            headers = {**extra, "Authorization": f"Bearer {self.token(fresh)}"}
            resp = self._send(
                method, f"{self.base_url}/v1{path}", repeatable,
                headers=headers, **kwargs,
            )  # fmt: skip
            if resp.status_code != 401:
                return resp
        return resp


def _problem(resp: httpx.Response) -> str:
    try:
        body = resp.json()
    except ValueError:
        return f"HTTP {resp.status_code}"
    errors = body.get("errors") or []
    if errors:
        return "; ".join(f"{'.'.join(e['loc'][1:])}: {e['msg']}" for e in errors)
    return body.get("detail") or f"HTTP {resp.status_code}"
