"""``python -m src.access_reconcile``: make Keycloak and Polaris match the
membership table (run by the control-plane-access-reconcile CronJob, and at
startup). Exits 1 if any project couldn't be reconciled."""

import logging
import sys

from src.db import get_engine
from src.provisioning import get_keycloak, get_polaris, reconcile_access


def main() -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    counts = reconcile_access(get_engine(), get_polaris(), get_keycloak())
    logging.info(
        "Access reconcile: %(projects)d project(s), %(removed)d stale role(s) "
        "removed, %(failed)d failed",
        counts,
    )
    return 1 if counts["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
