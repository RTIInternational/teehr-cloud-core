"""Domain lookups: units, periods and variables visible to a project.

A project sees the public rows plus its own. A name is defined once across both,
so a lookup in a project's scope is never ambiguous.
"""

import uuid

import pyarrow.compute as pc
from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from src.models import Period, Unit, Variable

DomainModel = type[Unit] | type[Period] | type[Variable]


def key_column(model: DomainModel):
    return model.symbol if model is Unit else model.name


def visible(model: DomainModel, project_id: uuid.UUID | None):
    """Rows visible in a scope: public, plus the project's own."""
    scope = model.project_id.is_(None)
    if project_id is not None:
        scope = or_(scope, model.project_id == project_id)
    return select(model).where(scope)


def lookup(session: Session, model, name: str, project_id: uuid.UUID | None):
    return session.scalar(visible(model, project_id).where(key_column(model) == name))


def taken(session: Session, model, name: str, project_id: uuid.UUID | None) -> bool:
    """Whether ``name`` is already defined where a new row would be visible: for
    a project row, in public or that project; for a public row, anywhere."""
    query = select(model.id).where(key_column(model) == name)
    if project_id is not None:
        query = query.where(
            or_(model.project_id.is_(None), model.project_id == project_id)
        )
    return session.scalar(query.limit(1)) is not None


def convert(values, source: Unit, target: Unit):
    """Values in ``source`` as ``target`` (same dimension), through SI:
    ``si = v * to_si + offset`` (ADR-0005)."""
    if source.symbol == target.symbol:
        return values
    si = pc.add(pc.multiply(values, source.to_si), source.offset)
    return pc.divide(pc.subtract(si, target.offset), target.to_si)
