"""Kubernetes Jobs the control plane starts (ADR-0014; as VeriRace's runs/jobs.py).

A job runs in its own pod, ``python -m src.jobs run <job id>``, from the control
plane's image. The control plane creates it through the cluster's REST API with its
own service account, which may only manage Jobs (and read their pods) in its
namespace (manifests/rbac.yaml). Job pods get no Kubernetes token, and no
credentials but their job grant.

A job's state lives in the control-plane database; the Kubernetes Job is only where
it executes. :func:`job_state` says whether it's still running and, if its pod
died, why ("killed for running out of memory"), so a crashed job is reported, not
left running for good.

Routes depend on :func:`get_kube`, which tests replace.
"""

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal, Protocol

import httpx

from src.config import Settings, get_settings

_SERVICE_ACCOUNT = Path("/var/run/secrets/kubernetes.io/serviceaccount")


class KubeError(Exception):
    """The Kubernetes API refused or failed."""


class Kube(Protocol):
    def create_job(self, manifest: dict[str, Any]) -> str:
        """Create a Job; returns its name."""

    def get_job(self, name: str) -> dict[str, Any] | None:
        """The Job, or None if it's gone."""

    def job_pods(self, name: str) -> list[dict[str, Any]]:
        """The Job's pods."""

    def delete_job(self, name: str) -> None:
        """Delete the Job and its pods; no error if it's gone."""


#: The control plane's settings ConfigMap, and the job kinds that get it.
CONFIG_MAP = "teehr-control-plane-config"
CATALOG_JOBS = frozenset({"legacy_copy"})


def job_manifest(
    settings: Settings, kind: str, job_id: str, grant: str
) -> dict[str, Any]:
    """The Job for one control-plane job: non-root, no privilege escalation, no
    Kubernetes token, memory-bounded, never restarted (a crashed job fails), kept
    a day for its log."""
    labels = {"app": "teehr-job", "teehr.io/job": job_id, "teehr.io/kind": kind}
    pod: dict[str, Any] = {
        "restartPolicy": "Never",
        "automountServiceAccountToken": False,
        "enableServiceLinks": False,
        "securityContext": {
            "runAsNonRoot": True,
            "runAsUser": 10001,
            "runAsGroup": 10001,
            "seccompProfile": {"type": "RuntimeDefault"},
        },
        "containers": [
            {
                "name": "job",
                "image": settings.job_image,
                "imagePullPolicy": "IfNotPresent",
                "command": ["python", "-m", "src.jobs", "run", job_id],
                "env": [
                    {"name": "JOB_GRANT", "value": grant},
                    {
                        "name": "CONTROL_PLANE_INTERNAL_URL",
                        "value": settings.internal_url,
                    },
                ],
                # Jobs that read the catalog themselves (as their caller) get
                # the control plane's settings (no secrets in it).
                **(
                    {"envFrom": [{"configMapRef": {"name": CONFIG_MAP}}]}
                    if kind in CATALOG_JOBS
                    else {}
                ),
                "resources": {
                    "requests": {"memory": settings.job_memory_request},
                    "limits": {"memory": settings.job_memory_limit},
                },
                "securityContext": {
                    "allowPrivilegeEscalation": False,
                    "capabilities": {"drop": ["ALL"]},
                },
            }
        ],
    }
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            # Names are DNS labels: no underscores (legacy_copy -> legacy-copy).
            "generateName": f"{kind.replace('_', '-')}-{job_id[:8]}-",
            "namespace": settings.job_namespace,
            "labels": labels,
        },
        "spec": {
            "backoffLimit": 0,
            "ttlSecondsAfterFinished": settings.job_ttl_seconds,
            "template": {"metadata": {"labels": labels}, "spec": pod},
        },
    }


class InClusterKube:
    """:class:`Kube` over the cluster's REST API with the pod's service account."""

    def __init__(self, namespace: str):
        import os

        host = os.environ["KUBERNETES_SERVICE_HOST"]
        port = os.environ.get("KUBERNETES_SERVICE_PORT", "443")
        token = (_SERVICE_ACCOUNT / "token").read_text().strip()
        self.namespace = namespace
        self._client = httpx.Client(
            base_url=f"https://{host}:{port}",
            headers={"Authorization": f"Bearer {token}"},
            verify=str(_SERVICE_ACCOUNT / "ca.crt"),
            timeout=30,
        )

    def _jobs(self) -> str:
        return f"/apis/batch/v1/namespaces/{self.namespace}/jobs"

    def _check(self, resp: httpx.Response) -> None:
        if resp.status_code >= 400:
            raise KubeError(f"{resp.status_code}: {resp.text[:200]}")

    def create_job(self, manifest: dict[str, Any]) -> str:
        resp = self._client.post(self._jobs(), json=manifest)
        self._check(resp)
        return str(resp.json()["metadata"]["name"])

    def get_job(self, name: str) -> dict[str, Any] | None:
        resp = self._client.get(f"{self._jobs()}/{name}")
        if resp.status_code == 404:
            return None
        self._check(resp)
        return resp.json()

    def job_pods(self, name: str) -> list[dict[str, Any]]:
        resp = self._client.get(
            f"/api/v1/namespaces/{self.namespace}/pods",
            params={"labelSelector": f"job-name={name}"},
        )
        self._check(resp)
        return resp.json().get("items", [])

    def delete_job(self, name: str) -> None:
        resp = self._client.delete(
            f"{self._jobs()}/{name}", params={"propagationPolicy": "Background"}
        )
        if resp.status_code != 404:
            self._check(resp)


@dataclass(frozen=True)
class JobState:
    """Where a Kubernetes Job is: active, succeeded, failed or missing."""

    phase: Literal["active", "succeeded", "failed", "missing"]
    reason: str = ""

    @property
    def ended(self) -> bool:
        return self.phase != "active"


def job_state(kube: Kube, name: str) -> JobState:
    """A Job's state, and why its pod stopped if it failed."""
    job = kube.get_job(name)
    if job is None:
        return JobState("missing", "its job no longer exists")
    status = job.get("status") or {}
    conditions = {
        c.get("type"): c
        for c in status.get("conditions") or []
        if c.get("status") == "True"
    }
    if "Complete" in conditions or status.get("succeeded"):
        return JobState("succeeded")
    failed = conditions.get("Failed") or conditions.get("FailureTarget")
    if failed is not None or status.get("failed"):
        return JobState("failed", _why(kube, name, failed or {}))
    return JobState("active")


def _why(kube: Kube, name: str, condition: dict[str, Any]) -> str:
    pods = kube.job_pods(name)
    for pod in pods:
        for c in (pod.get("status") or {}).get("containerStatuses") or []:
            ended = (c.get("state") or {}).get("terminated") or {}
            if ended.get("reason") == "OOMKilled":
                return "its pod was killed for running out of memory"
            if ended:
                code, reason = ended.get("exitCode"), ended.get("reason") or "error"
                return f"its pod exited with code {code} ({reason})"
        meta, status = pod.get("metadata") or {}, pod.get("status") or {}
        if meta.get("deletionTimestamp") or status.get("reason") == "Evicted":
            return "its pod was deleted or evicted"
    if not pods:
        return "its pod is gone (deleted or evicted)"
    return f"its job failed ({condition.get('reason') or 'failed'})"


@lru_cache
def get_kube() -> Kube | None:
    """Kubernetes, or None outside a cluster / when jobs aren't configured."""
    settings = get_settings()
    if not (settings.job_image and settings.job_namespace):
        return None
    return InClusterKube(settings.job_namespace)
