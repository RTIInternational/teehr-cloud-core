"""Notes a write returns about what it refers to (shared by ``/data`` and jobs)."""


def missing_note(missing: list[str], total: int, what: str, where: str) -> str:
    """E.g. "3 of 40 location keys aren't in gauges: a, b, c"."""
    examples = ", ".join(missing[:5]) + (", ..." if len(missing) > 5 else "")
    return f"{len(missing)} of {total} {what} aren't in {where}: {examples}"
