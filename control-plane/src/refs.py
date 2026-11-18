"""References from one dataset to another (layers-and-crosswalks.md).

``<name>`` is a dataset in the current project; ``public.<name>`` is one in the
``public`` system project, so every project can use the shared ones without
copying. (``<project>.<name>`` for other projects may come later.)
"""

from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models import Dataset, Project

PUBLIC = "public"


@dataclass(frozen=True)
class Resolved:
    project: Project
    dataset: Dataset

    @property
    def namespace(self) -> str:
        return self.project.name


def split(ref: str) -> tuple[str | None, str]:
    """(project or None for the current one, dataset name)."""
    if ref.startswith(f"{PUBLIC}."):
        return PUBLIC, ref[len(PUBLIC) + 1 :]
    return None, ref


def resolve(session: Session, project: Project, ref: str) -> Resolved | None:
    """The dataset a reference names, seen from ``project``; None if there's none."""
    owner_name, name = split(ref)
    owner = (
        project
        if owner_name is None or owner_name == project.name
        else session.scalar(select(Project).where(Project.name == owner_name))
    )
    if owner is None:
        return None
    dataset = session.scalar(
        select(Dataset).where(Dataset.project_id == owner.id, Dataset.name == name)
    )
    return None if dataset is None else Resolved(owner, dataset)


def qualify(owner: Project, current: Project, name: str) -> str:
    """How ``current`` refers to dataset ``name`` of ``owner``."""
    return name if owner.id == current.id else f"{owner.name}.{name}"


def _refs(dataset: Dataset) -> list[str]:
    """The references a dataset's metadata makes."""
    properties = dataset.properties or {}
    return [
        properties[k] for k in ("location_layer", "left", "right") if properties.get(k)
    ]


def referrers(session: Session, project: Project, name: str) -> list[str]:
    """Datasets whose metadata refers to ``project``'s dataset ``name`` (as
    ``<project>.<dataset>``): what deleting it would break."""
    found = []
    query = select(Dataset, Project).join(Project, Project.id == Dataset.project_id)
    if project.name != PUBLIC:  # only public datasets are referred to from elsewhere
        query = query.where(Dataset.project_id == project.id)
    for dataset, owner in session.execute(query):
        if dataset.name == name and owner.id == project.id:
            continue
        for ref in _refs(dataset):
            target_project, target = split(ref)
            target_project = target_project or owner.name
            if target_project == project.name and target == name:
                found.append(f"{owner.name}.{dataset.name}")
    return sorted(set(found))
