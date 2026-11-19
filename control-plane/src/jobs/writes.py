"""A job's writes, as its caller (ADR-0016): the job commits with the write
gateway's own code, then reports the commit through its grant; the control plane
checks the snapshot is on the table and records the dataset's lineage."""

import time

import httpx

from src.jobs.grant import GrantClient, LoadFailed, _problem
from src.jobs.reads import CatalogAccess
from src.tables import (
    PolarisTables,
    TableError,
    TableForbidden,
    Tables,
    WriteConflict,
    catalog_properties,
)

CONFLICT_TRIES = 4


def open_tables(task: dict) -> Tables:
    """Table writes as the caller, at the task's catalog (tests replace this)."""
    if not task.get("catalog"):
        raise LoadFailed(["the control plane didn't say where the warehouse is"])
    c = CatalogAccess.from_task(task["catalog"])
    return PolarisTables.at(
        catalog_properties(c.uri, c.warehouse, c.realm, c.region, c.endpoint,
                           c.path_style)
    )  # fmt: skip


def as_caller(client: GrantClient, name: str, call):
    """``call(token)`` as the caller: once more with a fresh token if it's refused
    (the token may have gone stale), again after a short wait while another write
    commits first; table errors as LoadFailed."""

    def attempt(token):
        for tries in range(CONFLICT_TRIES):
            try:
                return call(token)
            except WriteConflict:
                if tries == CONFLICT_TRIES - 1:
                    raise
                time.sleep(2**tries)

    try:
        try:
            return attempt(client.token())
        except TableForbidden:
            return attempt(client.token(fresh=True))
    except TableForbidden:
        raise LoadFailed([f"writing {name}: the caller may not write it"]) from None
    except TableError as exc:
        raise LoadFailed([f"writing {name}: {exc}"]) from None


def report(
    client: GrantClient,
    name: str,
    mode: str,
    where: str | None,
    rows: int,
    snap: int,
    **extra,
) -> None:
    """Report a commit to the control plane, which records it (ADR-0016).
    ``extra``: what else the write's record says (months, unit, series_added)."""
    body = {"dataset": name, "mode": mode, "filter": where, "rows": rows,
            "snapshot_id": snap, **{k: v for k, v in extra.items() if v is not None}}  # fmt: skip
    try:
        client._grant("POST", "writes", json=body)
    except httpx.HTTPStatusError as exc:
        raise LoadFailed(
            [f"recording the write to {name}: {_problem(exc.response)}"]
        ) from None
