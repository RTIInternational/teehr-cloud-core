"""Page cursors for dataset reads (as in VeriRace).

A cursor is opaque to clients: it pins the snapshot the first page read, so every
page of a read sees the same data even while writes land, and records the next
row offset. It also carries a short fingerprint of the query (filter and
columns), so a cursor can't be replayed against a different query.
"""

import base64
import hashlib
import json


class BadCursor(ValueError):
    """A cursor that isn't one of ours, or belongs to a different query."""


def fingerprint(row_filter: str | None, columns: tuple[str, ...] | None) -> str:
    raw = json.dumps([row_filter, list(columns or ())]).encode()
    return hashlib.sha256(raw).hexdigest()[:12]


def encode_cursor(snapshot_id: int, offset: int, query: str) -> str:
    raw = json.dumps({"s": snapshot_id, "o": offset, "q": query}, separators=(",", ":"))
    return base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")


def decode_cursor(cursor: str, query: str) -> tuple[int, int]:
    """(snapshot_id, offset) from a cursor made for ``query``."""
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        data = json.loads(base64.urlsafe_b64decode(padded))
        snapshot_id, offset, made_for = int(data["s"]), int(data["o"]), data["q"]
    except (ValueError, KeyError, TypeError) as exc:
        raise BadCursor("not a valid cursor") from exc
    if offset < 0:
        raise BadCursor("not a valid cursor")
    if made_for != query:
        raise BadCursor("this cursor belongs to a different filter or columns")
    return snapshot_id, offset
