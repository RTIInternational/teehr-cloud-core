#!/usr/bin/env python3
"""Generate a profile-list JSON payload from project specs.

This keeps profile boilerplate in one place while allowing per-project
customization for project id, nodegroup suffix, and optional extra choices.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


STABLE_IMAGE = "${actions.build.teehr-jupyter-driver-image-stable.outputs.deploymentImageId}"
PREVIOUS_IMAGE = "${actions.build.teehr-jupyter-driver-image-previous.outputs.deploymentImageId}"
EDGE_IMAGE = "${actions.build.teehr-jupyter-driver-image-edge.outputs.deploymentImageId}"
HEFS_V031_IMAGE = "${var.remoteCluster.ecrRegistry}/hefs-fews-hub:v0.3.1"
HEFS_DEV_IMAGE = "${var.remoteCluster.ecrRegistry}/hefs-fews-hub:dev"
FRACTIONAL_4XL_MEM_LIMIT = "127G"
FRACTIONAL_4XL_CPU_LIMIT = 15.5


def _nodegroup(size: str, suffix: str) -> str:
    return f"nb-r5-{size}{suffix}"


def _choice(
    display_name: str,
    image: str,
    nodegroup_name: str,
    mem_limit: str,
    cpu_limit: float,
    *,
    default: bool = False,
    image_pull_policy: str | None = None,
) -> dict[str, Any]:
    override: dict[str, Any] = {
        "image": image,
        "mem_limit": mem_limit,
        "mem_guarantee": "19G" if "xlarge" in nodegroup_name and "4xlarge" not in nodegroup_name else "76G",
        "cpu_limit": cpu_limit,
        "cpu_guarantee": 3 if "xlarge" in nodegroup_name and "4xlarge" not in nodegroup_name else 12,
        "node_selector": {
            "teehr-hub/nodegroup-name": nodegroup_name,
        },
    }
    if image_pull_policy:
        override["image_pull_policy"] = image_pull_policy

    result: dict[str, Any] = {
        "display_name": display_name,
        "kubespawner_override": override,
    }
    if default:
        result["default"] = True
    return result


def _standard_choices(
    *,
    suffix: str,
    stable_version: str,
    previous_version: str,
    dev_version: str,
    current_default: bool,
) -> dict[str, Any]:
    xlarge = _nodegroup("xlarge", suffix)
    xl4 = _nodegroup("4xlarge", suffix)

    return {
        "current-tag-xlarge": _choice(
            f"Version {stable_version} - XL (4 cores, 16 GB memory)",
            STABLE_IMAGE,
            xlarge,
            "32G",
            4,
            default=current_default,
        ),
        "current-tag-4xlarge": _choice(
            f"Version {stable_version} - 4XL (16 cores, 128 GB memory)",
            STABLE_IMAGE,
            xl4,
            FRACTIONAL_4XL_MEM_LIMIT,
            FRACTIONAL_4XL_CPU_LIMIT,
        ),
        "previous-tag-xlarge": _choice(
            f"Version {previous_version} - XL (4 cores, 32 GB memory)",
            PREVIOUS_IMAGE,
            xlarge,
            "32G",
            4,
        ),
        "previous-tag-4xlarge": _choice(
            f"Version {previous_version} - 4XL (16 cores, 128 GB memory)",
            PREVIOUS_IMAGE,
            xl4,
            "128G",
            16,
        ),
        "dev-xlarge": _choice(
            f"Bleeding Edge {dev_version} - XL (4 cores, 32 GB memory)",
            EDGE_IMAGE,
            xlarge,
            "32G",
            4,
        ),
        "dev-4xlarge": _choice(
            f"Bleeding Edge {dev_version} - 4XL (16 cores, 128 GB memory)",
            EDGE_IMAGE,
            xl4,
            FRACTIONAL_4XL_MEM_LIMIT,
            FRACTIONAL_4XL_CPU_LIMIT,
        ),
    }


def _hefs_choices(*, suffix: str) -> dict[str, Any]:
    xlarge = _nodegroup("xlarge", suffix)
    return {
        "hefs-v0-3-0": _choice(
            "HEFS-FEWS v0.3.x - XL (4 cores, 16 GB memory)",
            HEFS_V031_IMAGE,
            xlarge,
            "32G",
            4,
            default=True,
        ),
        "hefs-latest": _choice(
            "HEFS-FEWS Bleeding Edge - XL (4 cores, 16 GB memory)",
            HEFS_DEV_IMAGE,
            xlarge,
            "32G",
            4,
            image_pull_policy="Always",
        ),
    }


def _build_profile(spec: dict[str, Any]) -> dict[str, Any]:
    suffix = spec["nodegroup_suffix"]
    standard = _standard_choices(
        suffix=suffix,
        stable_version="${var.stableTeehrVersion}",
        previous_version="${var.previousTeehrVersion}",
        dev_version="${slice(var.devTeehrVersion, 0, 6)}",
        current_default=not spec["include_hefs_choices"],
    )

    choices: dict[str, Any] = {}
    if spec["include_hefs_choices"]:
        choices.update(_hefs_choices(suffix=suffix))
    choices.update(standard)

    return {
        "display_name": spec["display_name"],
        "default": spec["default"],
        "description": spec["description"],
        "profile_options": {
            "image": {
                "display_name": "Image",
                "choices": choices,
            }
        },
        "kubespawner_override": {
            "environment": {
                "TEEHR_PROJECT_ID": spec["project_id"],
            }
        },
    }


def generate(specs_path: Path, out_path: Path) -> None:
    specs = json.loads(specs_path.read_text())
    payload = {
        "version": 1,
        "profiles": [_build_profile(spec) for spec in specs],
    }
    out_path.write_text(json.dumps(payload, indent=2) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=False)
    mode.add_argument(
        "--local",
        action="store_true",
        help="Generate local profile list using repo defaults",
    )
    mode.add_argument(
        "--remote",
        action="store_true",
        help="Generate remote profile list using deployment defaults",
    )
    parser.add_argument(
        "--spec",
        type=Path,
        help="Path to project specs JSON",
    )
    parser.add_argument(
        "--out",
        type=Path,
        help="Output profile list path",
    )
    args = parser.parse_args()

    using_explicit_paths = args.spec is not None or args.out is not None
    using_mode = args.local or args.remote

    if using_explicit_paths and using_mode:
        parser.error("Use either --local/--remote or --spec/--out, not both.")

    if using_explicit_paths:
        if args.spec is None or args.out is None:
            parser.error("--spec and --out must be provided together.")
        specs = args.spec
        out = args.out
    elif args.local:
        specs = Path("jupyterhub-profiles/profile-list.example.projects.json")
        out = Path("jupyterhub-profiles/profile-list.local.json")
    elif args.remote:
        specs = Path("jupyterhub-profiles/profile-list.remote.projects.json")
        out = Path("jupyterhub-profiles/profile-list.remote.json")
    else:
        parser.error("Provide --local, --remote, or both --spec and --out.")

    generate(specs, out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())