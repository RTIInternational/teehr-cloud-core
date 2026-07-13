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
    mode = parser.add_mutually_exclusive_group(required=False)
    mode.add_argument(
        "--local",
        action="store_true",
        help="Render local profile ConfigMap template using repo defaults",
    )
    mode.add_argument(
        "--remote",
        action="store_true",
        help="Render remote profile ConfigMap template using deployment defaults",
    )
    parser.add_argument(
        "--spec",
        type=Path,
        help="Path to input profile-list JSON",
    )
    parser.add_argument(
        "--out",
        type=Path,
        help="Path to output ConfigMap template",
    )
    args = parser.parse_args()

    using_explicit_paths = args.spec is not None or args.out is not None
    using_mode = args.local or args.remote

    if using_explicit_paths and using_mode:
        parser.error("Use either --local/--remote or --spec/--out, not both.")

    if using_explicit_paths:
        if args.spec is None or args.out is None:
            parser.error("--spec and --out must be provided together.")
        spec = args.spec
        out = args.out
    elif args.local:
        spec = Path("jupyterhub-profiles/profile-list.local.json")
        out = Path("jupyterhub-profiles/manifests/jupyterhub-profile-list-local.configmap.yaml.tpl")
    elif args.remote:
        spec = Path("jupyterhub-profiles/profile-list.remote.json")
        out = Path("jupyterhub-profiles/manifests/jupyterhub-profile-list-remote.configmap.yaml.tpl")
    else:
        parser.error("Provide --local, --remote, or both --spec and --out.")

    render_template(source_json=spec, output_tpl=out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())