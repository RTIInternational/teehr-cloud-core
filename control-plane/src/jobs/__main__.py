"""A job's pod: ``python -m src.jobs run <job id>`` (ADR-0014).

Reads its grant from ``JOB_GRANT`` and the control plane's address from
``CONTROL_PLANE_INTERNAL_URL``; does the job as its caller; reports the outcome.
Exits 0 once the outcome is reported (a failed load included), 1 if it can't be.
"""

import logging
import os
import sys
import traceback

from src.jobs.load import GrantClient, LoadFailed, load_upload

log = logging.getLogger("src.jobs")


def run(job_id: str) -> int:
    client = GrantClient(
        os.environ["CONTROL_PLANE_INTERNAL_URL"], os.environ["JOB_GRANT"]
    )
    task = client.task()
    log.info("job %s: %s in %s", job_id, task["kind"], task["project"])
    try:
        if task["kind"] != "upload":
            raise LoadFailed([f"unknown job kind '{task['kind']}'"])
        result = load_upload(client, task)
    except LoadFailed as exc:
        log.warning("job %s failed: %s", job_id, exc)
        client.finish("failed", problems=exc.problems, error=str(exc)[:10_000])
        return 0
    except Exception as exc:  # report anything unexpected, then show it in the log
        traceback.print_exc()
        client.finish("failed", error=f"{type(exc).__name__}: {exc}"[:10_000])
        return 0
    log.info("job %s succeeded: %s", job_id, result)
    client.finish("succeeded", result=result)
    return 0


def main(argv: list[str]) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    # httpx logs request URLs, and the staged file's URL carries a signature.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    if len(argv) != 3 or argv[1] != "run":
        print("usage: python -m src.jobs run <job id>", file=sys.stderr)
        return 2
    try:
        return run(argv[2])
    except Exception:
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
