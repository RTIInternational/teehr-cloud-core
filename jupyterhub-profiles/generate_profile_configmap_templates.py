#!/usr/bin/env python3
"""Render JupyterHub profile ConfigMap templates from source JSON payloads.

This keeps profile-list JSON as the source of truth and prevents drift between
`profile-list.*.json` files and manifest template copies.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


HEADER = """apiVersion: v1
kind: ConfigMap
metadata:
  name: jupyterhub-profile-list
  namespace: ${environment.namespace}
  labels:
    app: jupyterhub
    component: hub
data:
  profile-list.json: |
"""


def render_template(source_json: Path, output_tpl: Path) -> None:
    payload = json.loads(source_json.read_text())
    pretty_json = json.dumps(payload, indent=2)
    indented_json = "\n".join(f"    {line}" for line in pretty_json.splitlines())
    output_tpl.write_text(f"{HEADER}{indented_json}\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--spec",
        type=Path,
        required=True,
        help="Path to input profile-list JSON",
    )
    parser.add_argument(
        "--out",
        type=Path,
        required=True,
        help="Path to output ConfigMap template",
    )
    args = parser.parse_args()

    render_template(source_json=args.spec, output_tpl=args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())