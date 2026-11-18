"""Row filters in the shared expression language (teehr-shared-eval-spec
``spec/expressions.md``): a restricted SQL subset, checked against teehr-core's
allow-list (``teehr_core.spec.expressions``, the one VeriRace's specs use too).

A filter is split into what PyIceberg can push down to the scan (comparisons of a
column with literals, ``IN``, ``BETWEEN``, ``IS [NOT] NULL``, and ``AND`` / ``OR`` /
``NOT`` of those), so Iceberg still skips files, and the rest (functions,
arithmetic, ``CASE``, comparisons between columns), evaluated with Arrow on the rows
the scan returns. A number is read in the column's stored unit (a timeseries'
canonical unit).
"""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import pyarrow as pa
import pyarrow.compute as pc
import sqlglot
from pyiceberg import expressions as ice
from pyiceberg.expressions import BooleanExpression
from pyiceberg.expressions.visitors import bind
from sqlglot import exp
from teehr_core.spec.expressions import ExpressionError, check_expression

from src.tables import DataMismatch, iceberg_schema

# Casts the Arrow evaluation supports (every type the allow-list names).
_CASTS = {
    exp.DataType.Type.DOUBLE: pa.float64(),
    exp.DataType.Type.FLOAT: pa.float64(),
    exp.DataType.Type.BIGINT: pa.int64(),
    exp.DataType.Type.INT: pa.int64(),
    exp.DataType.Type.VARCHAR: pa.string(),
    exp.DataType.Type.TEXT: pa.string(),
    exp.DataType.Type.BOOLEAN: pa.bool_(),
    exp.DataType.Type.DATE: pa.date32(),
    exp.DataType.Type.TIMESTAMP: pa.timestamp("us"),
    exp.DataType.Type.TIMESTAMPTZ: pa.timestamp("us", tz="UTC"),
}
_TRUNC = {"YEAR": "year", "QUARTER": "quarter", "MONTH": "month", "WEEK": "week",
          "DAY": "day", "HOUR": "hour", "MINUTE": "minute", "SECOND": "second"}  # fmt: skip


@dataclass(frozen=True)
class Filter:
    """A parsed filter: the part pushed down to the scan, and the rest."""

    text: str
    pushdown: BooleanExpression | None
    residual: Callable[[pa.Table], pa.ChunkedArray] | None
    # The columns the residual reads (a read must fetch them too).
    residual_columns: tuple[str, ...] = ()

    def apply(self, table: pa.Table) -> pa.Table:
        """The rows of ``table`` (already filtered by ``pushdown``) the rest keeps."""
        if self.residual is None or table.num_rows == 0:
            return table
        mask = self.residual(table)
        return table.filter(pc.fill_null(mask, False))


def compile_filter(text: str, columns: list[dict]) -> Filter:
    """Parse ``text``, check it against the allow-list and ``columns``, and split it.
    Raises DataMismatch with what's wrong."""
    try:
        check_expression(text)
    except ExpressionError as exc:
        raise DataMismatch(f"invalid filter: {exc}") from exc
    tree = sqlglot.parse_one(text)
    known = {c["name"]: c for c in columns}
    _check(tree, known)
    schema = iceberg_schema(columns)
    pushed, rest = [], []
    for part in _conjuncts(tree):
        expression = None
        for iso in (False, True):  # times with Z bind as +00:00
            candidate = _to_iceberg(part, iso)
            if candidate is None:
                break
            try:
                bind(schema, candidate, case_sensitive=True)
            except Exception:  # a literal that doesn't fit the column
                continue
            expression = candidate
            break
        (pushed if expression is not None else rest).append(
            expression if expression is not None else part
        )
    pushdown = None
    for expression in pushed:
        pushdown = expression if pushdown is None else ice.And(pushdown, expression)
    residual = None
    if rest:
        parts = list(rest)

        def residual(table: pa.Table) -> pa.ChunkedArray:
            mask = None
            for part in parts:
                value = _as_bool(_eval(part, table), part)
                mask = value if mask is None else pc.and_kleene(mask, value)
            return mask

    used = tuple(dict.fromkeys(c.name for p in rest for c in p.find_all(exp.Column)))
    return Filter(text, pushdown, residual, used)


# --- checks -------------------------------------------------------------------


def _check(tree: exp.Expression, known: dict) -> None:
    """Columns must be the dataset's, unqualified (the allow-list is checked first)."""
    for node in tree.walk():
        if isinstance(node, exp.Column):
            if node.table or node.name not in known:
                raise DataMismatch(f"invalid filter: unknown column '{node.sql()}'")
        elif isinstance(node, exp.Cast) and node.to.this not in _CASTS:
            raise DataMismatch(f"invalid filter: can't cast to {node.to.sql()}")


def _conjuncts(tree: exp.Expression) -> list[exp.Expression]:
    while isinstance(tree, exp.Paren):
        tree = tree.this
    if isinstance(tree, exp.And):
        return _conjuncts(tree.this) + _conjuncts(tree.expression)
    return [tree]


# --- pushdown -----------------------------------------------------------------


def _iso(text: str) -> str:
    """An ISO 8601 time with a ``Z`` as ``+00:00`` (what PyIceberg binds to a
    timestamptz column); other strings as they are."""
    if text.endswith(("Z", "z")):
        try:
            datetime.fromisoformat(text)
        except ValueError:
            return text
        return text[:-1] + "+00:00"
    return text


def _literal(node: exp.Expression):
    """A Python value for a literal node, or a marker that it isn't one."""
    if isinstance(node, exp.Paren):
        return _literal(node.this)
    if isinstance(node, exp.Literal):
        if node.is_string:
            return node.this
        number = node.this
        return float(number) if any(c in number for c in ".eE") else int(number)
    if isinstance(node, exp.Neg):
        value = _literal(node.this)
        return -value if isinstance(value, (int, float)) else _NOT
    if isinstance(node, exp.Boolean):
        return node.this
    return _NOT


_NOT = object()
_COMPARE = {exp.EQ: ice.EqualTo, exp.NEQ: ice.NotEqualTo, exp.LT: ice.LessThan,
            exp.LTE: ice.LessThanOrEqual, exp.GT: ice.GreaterThan,
            exp.GTE: ice.GreaterThanOrEqual}  # fmt: skip
_FLIP = {exp.LT: exp.GT, exp.LTE: exp.GTE, exp.GT: exp.LT, exp.GTE: exp.LTE,
         exp.EQ: exp.EQ, exp.NEQ: exp.NEQ}  # fmt: skip


def _to_iceberg(node: exp.Expression, iso: bool = False) -> BooleanExpression | None:
    """The PyIceberg expression for ``node``, or None if it can't be pushed down.
    With ``iso``, string literals that are ISO times with ``Z`` get ``+00:00``."""

    def lit(n):
        value = _literal(n)
        return _iso(value) if iso and isinstance(value, str) else value

    if isinstance(node, exp.Paren):
        return _to_iceberg(node.this, iso)
    if isinstance(node, (exp.And, exp.Or)):
        left, right = _to_iceberg(node.this, iso), _to_iceberg(node.expression, iso)
        if left is None or right is None:
            return None
        return (ice.And if isinstance(node, exp.And) else ice.Or)(left, right)
    if isinstance(node, exp.Not):
        inner = _to_iceberg(node.this, iso)
        return None if inner is None else ice.Not(inner)
    if type(node) in _COMPARE:
        left, right = node.this, node.expression
        if isinstance(left, exp.Column) and lit(right) is not _NOT:
            return _COMPARE[type(node)](left.name, lit(right))
        if isinstance(right, exp.Column) and lit(left) is not _NOT:
            return _COMPARE[_FLIP[type(node)]](right.name, lit(left))
        return None
    if isinstance(node, exp.In) and isinstance(node.this, exp.Column):
        values = [lit(v) for v in node.expressions]
        if not values or any(v is _NOT for v in values):
            return None
        return ice.In(node.this.name, set(values))
    if isinstance(node, exp.Between) and isinstance(node.this, exp.Column):
        low, high = lit(node.args["low"]), lit(node.args["high"])
        if _NOT in (low, high):
            return None
        name = node.this.name
        return ice.And(
            ice.GreaterThanOrEqual(name, low), ice.LessThanOrEqual(name, high)
        )
    if isinstance(node, exp.Is) and isinstance(node.this, exp.Column):
        if isinstance(node.expression, exp.Null):
            return ice.IsNull(node.this.name)
    return None


# --- evaluation ---------------------------------------------------------------


def _as_bool(value, node: exp.Expression):
    if isinstance(value, (pa.ChunkedArray, pa.Array)) and pa.types.is_boolean(
        value.type
    ):
        return value
    raise DataMismatch(f"invalid filter: '{node.sql()}' isn't true or false")


def _coerce(value, other):
    """A string literal compared with a time column, as a time."""
    if not isinstance(value, str) or not isinstance(other, (pa.ChunkedArray, pa.Array)):
        return value
    t = other.type
    try:
        if pa.types.is_timestamp(t):
            parsed = datetime.fromisoformat(value)
            return pa.scalar(parsed, pa.timestamp(t.unit, tz=t.tz))
        if pa.types.is_date(t):
            return pa.scalar(date.fromisoformat(value), t)
    except (ValueError, pa.ArrowInvalid) as exc:
        raise DataMismatch(f"invalid filter: '{value}' isn't a time ({exc})") from exc
    return value


def _pair(node, table):
    left, right = _eval(node.this, table), _eval(node.expression, table)
    return _coerce(left, right), _coerce(right, left)


def _eval(node: exp.Expression, table: pa.Table):
    """``node``'s value over ``table``: an Arrow array, or a Python scalar."""
    try:
        return _evaluate(node, table)
    except DataMismatch:
        raise
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError, TypeError) as exc:
        raise DataMismatch(
            f"invalid filter: can't evaluate '{node.sql()}' ({exc})"
        ) from exc


_BINARY = {exp.EQ: pc.equal, exp.NEQ: pc.not_equal, exp.LT: pc.less,
           exp.LTE: pc.less_equal, exp.GT: pc.greater, exp.GTE: pc.greater_equal,
           exp.Add: pc.add_checked, exp.Sub: pc.subtract_checked,
           exp.Mul: pc.multiply_checked}  # fmt: skip
_UNARY = {exp.Abs: pc.abs, exp.Ln: pc.ln, exp.Exp: pc.exp, exp.Sqrt: pc.sqrt,
          exp.Ceil: pc.ceil, exp.Floor: pc.floor, exp.Hour: pc.hour, exp.Day: pc.day,
          exp.Month: pc.month, exp.Year: pc.year, exp.Quarter: pc.quarter,
          exp.Minute: pc.minute, exp.Lower: pc.utf8_lower, exp.Upper: pc.utf8_upper,
          exp.Length: pc.utf8_length}  # fmt: skip
_EXTRACT = {"YEAR": pc.year, "QUARTER": pc.quarter, "MONTH": pc.month, "DAY": pc.day,
            "HOUR": pc.hour, "MINUTE": pc.minute}  # fmt: skip


def _evaluate(node: exp.Expression, table: pa.Table):
    kind = type(node)
    if kind is exp.Paren:
        return _eval(node.this, table)
    if kind is exp.Column:
        return table[node.name]
    if kind in (exp.Literal, exp.Boolean):
        return _literal(node)
    if kind is exp.Null:
        return None
    if kind is exp.Neg:
        return pc.negate_checked(_eval(node.this, table))
    if kind is exp.And:
        return pc.and_kleene(_eval(node.this, table), _eval(node.expression, table))
    if kind is exp.Or:
        return pc.or_kleene(_eval(node.this, table), _eval(node.expression, table))
    if kind is exp.Not:
        return pc.invert(_eval(node.this, table))
    if kind in _BINARY:
        return _BINARY[kind](*_pair(node, table))
    if kind is exp.Div:
        left, right = _pair(node, table)
        return pc.divide(pc.cast(left, pa.float64()) if not isinstance(left, (int, float))
                         else float(left), right)  # fmt: skip
    if kind is exp.Mod:
        left, right = _pair(node, table)
        return pc.subtract(left, pc.multiply(pc.trunc(pc.divide(
            pc.cast(left, pa.float64()), right)), right))  # fmt: skip
    if kind in _UNARY:
        return _UNARY[kind](_eval(node.this, table))
    if kind is exp.Log:  # log10, or log(b, x)
        base, value = _literal(node.this), _eval(node.expression, table)
        if base == 10:
            return pc.log10(value)
        return pc.divide(pc.ln(value), pc.ln(pa.scalar(float(base))))
    if kind is exp.Pow:
        return pc.power(_eval(node.this, table), _eval(node.expression, table))
    if kind is exp.Coalesce:
        values = [_eval(node.this, table), *(_eval(e, table) for e in node.expressions)]
        return pc.coalesce(*values)
    if kind in (exp.Least, exp.Greatest):
        values = [_eval(node.this, table), *(_eval(e, table) for e in node.expressions)]
        f = pc.min_element_wise if kind is exp.Least else pc.max_element_wise
        return f(*values)
    if kind is exp.Nullif:
        value, other = _pair(node, table)
        return pc.if_else(pc.equal(value, other), pa.scalar(None, value.type), value)
    if kind is exp.Substring:
        value = _eval(node.this, table)
        start = _literal(node.args["start"]) - 1
        length = node.args.get("length")
        stop = None if length is None else start + _literal(length)
        return pc.utf8_slice_codeunits(value, start, stop)
    if kind in (exp.Concat, exp.DPipe):
        parts = (node.expressions if kind is exp.Concat
                 else [node.this, node.expression])  # fmt: skip
        values = [_eval(p, table) for p in parts]
        return pc.binary_join_element_wise(*values, "")
    if kind is exp.Like:
        pattern = _literal(node.expression)
        return pc.match_like(_eval(node.this, table), pattern)
    if kind is exp.In:
        values = [_literal(v) for v in node.expressions]
        column = _eval(node.this, table)
        values = [_coerce(v, column) for v in values]
        return pc.is_in(column, value_set=pa.array(values, column.type))
    if kind is exp.Between:
        value = _eval(node.this, table)
        low = _coerce(_eval(node.args["low"], table), value)
        high = _coerce(_eval(node.args["high"], table), value)
        return pc.and_kleene(pc.greater_equal(value, low), pc.less_equal(value, high))
    if kind is exp.Is:
        value = _eval(node.this, table)
        if isinstance(node.expression, exp.Null):
            return pc.is_null(value)
        return pc.equal(value, _eval(node.expression, table))
    if kind is exp.Extract:
        part = node.this.name.upper()
        if part not in _EXTRACT:
            raise DataMismatch(f"invalid filter: extract({part}) isn't allowed")
        return _EXTRACT[part](_eval(node.expression, table))
    if kind is exp.DateTrunc:
        unit = _TRUNC.get(str(node.args["unit"].this).upper())
        if unit is None:
            raise DataMismatch("invalid filter: unknown date_trunc unit")
        return pc.floor_temporal(_eval(node.this, table), unit=unit)
    if kind is exp.Cast:
        return pc.cast(_eval(node.this, table), _CASTS[node.to.this])
    if kind in (exp.Case, exp.If):
        return _case(node, table)
    if kind is exp.Interval:  # INTERVAL '6' HOUR: a duration to add to a time
        unit = node.args["unit"].name.lower() + "s"
        return pa.scalar(timedelta(**{unit: float(node.this.name)}), pa.duration("us"))
    raise DataMismatch(f"invalid filter: '{node.sql()}' isn't allowed")


def _case(node: exp.Expression, table: pa.Table):
    if isinstance(node, exp.If):
        branches = [(node.this, node.args.get("true"))]
        default = node.args.get("false")
    else:
        branches = [(i.this, i.args.get("true")) for i in node.args.get("ifs", [])]
        default = node.args.get("default")
    result = _eval(default, table) if default is not None else None
    for condition, value in reversed(branches):
        result = pc.if_else(
            pc.fill_null(_eval(condition, table), False), _eval(value, table), result
        )
    return result


def overwrite_expression(
    text: str | None, columns: list[dict]
) -> BooleanExpression | None:
    """An overwrite's filter as the table applies it (None: the whole table). It
    must compile wholly to the table's terms (raises DataMismatch otherwise)."""
    if not text:
        return None
    compiled = compile_filter(text, columns)
    if compiled.residual is not None:
        raise DataMismatch(
            "an overwrite filter must compare columns with values (=, <, IN, "
            "BETWEEN, IS NULL, AND/OR/NOT), so the table can apply it"
        )
    return compiled.pushdown
