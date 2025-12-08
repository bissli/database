"""SQL parameter processing with clean tokenization.

Public API:
- prepare_query(sql, args, dialect) - Main entry point for query processing
- quote_identifier(name, dialect) - Quote table/column names
- has_placeholders(sql) - Check for parameter placeholders
- standardize_placeholders(sql, dialect) - Convert %s <-> ?
"""
import re
from collections import namedtuple
from typing import Any

from database.types import TypeConverter

from libb import issequence

# Regex patterns
_PH_RE = re.compile(r'%\((\w+)\)s|%s|\?')  # Placeholders (group 1 = named param name)
_STR_RE = re.compile(r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"")  # String literals
_REGEXP_RE = re.compile(r'regexp_replace\s*\([^)]*(?:\([^)]*\)[^)]*)*\)', re.I)
_UNESCAPE_PCT = re.compile(r'(?<!%)%(?![%s(])')  # Unescaped % not followed by % or s or (

# Placeholder info: position, end, name (for named params), context, already in parens
PH = namedtuple('PH', 'pos end name ctx in_parens')


def prepare_query(sql: str, args: tuple | list | dict | None, dialect: str = 'postgresql') -> tuple[str, Any]:
    """Process SQL query with parameters for the given dialect.

    Handles:
    - IN clause expansion: `IN %s` with `(1,2,3)` → `IN (%s,%s,%s)`
    - IS NULL handling: `IS %s` with `None` → `IS NULL`
    - Placeholder conversion: `%s` ↔ `?` based on dialect
    - Percent escaping in string literals for PostgreSQL
    """

    # Fast path: no placeholders
    if not sql or not _PH_RE.search(sql):
        converted = TypeConverter.convert_params(args) if args else args
        return sql, converted

    # Find placeholders with context
    phs = _find_contexts(sql)

    # Normalize args to canonical form
    args = _normalize(args, phs)

    # Transform SQL and args
    sql, args = _transform(sql, phs, args, dialect)

    return sql, TypeConverter.convert_params(args)


def quote_identifier(identifier: str, dialect: str = 'postgresql') -> str:
    """Quote a table or column name safely."""
    if dialect not in {'postgresql', 'sqlite'}:
        raise ValueError(f'Unknown dialect: {dialect}')
    return f'"{identifier.replace(chr(34), chr(34)+chr(34))}"'


def make_placeholders(count: int, dialect: str = 'postgresql') -> str:
    """Generate SQL placeholders for the given dialect.

    Args:
        count: Number of placeholders to generate
        dialect: Database dialect ('postgresql' or 'sqlite')

    Returns
        Comma-separated placeholder string (e.g., '%s, %s, %s' or '?, ?, ?')
    """
    marker = '?' if dialect == 'sqlite' else '%s'
    return ', '.join([marker] * count)


def build_select_sql(table: str, dialect: str, columns: list[str] | None = None,
                     where: str | None = None, order_by: str | None = None,
                     limit: int | None = None) -> str:
    """Generate a SELECT statement for the specified database type.
    """
    quoted_table = quote_identifier(table, dialect)

    if columns:
        quoted_cols = ', '.join(quote_identifier(col, dialect) for col in columns)
        select_clause = f'SELECT {quoted_cols}'
    else:
        select_clause = 'SELECT *'

    sql = f'{select_clause} FROM {quoted_table}'

    if where:
        sql += f' WHERE {where}'

    if order_by:
        sql += f' ORDER BY {order_by}'

    if limit is not None:
        sql += f' LIMIT {limit}'

    return sql


def build_insert_sql(dialect: str, table: str, columns: list[str]) -> str:
    """Generate an INSERT statement.
    """
    quoted_table = quote_identifier(table, dialect)
    quoted_columns = ', '.join(quote_identifier(col, dialect) for col in columns)
    placeholders = make_placeholders(len(columns), dialect)

    return f'INSERT INTO {quoted_table} ({quoted_columns}) VALUES ({placeholders})'


def has_placeholders(sql: str | None) -> bool:
    """Check if SQL contains parameter placeholders."""
    return bool(sql and _PH_RE.search(sql))


def standardize_placeholders(sql: str, dialect: str = 'postgresql') -> str:
    """Convert placeholders between database dialects."""
    if not sql:
        return sql

    # Quick check: nothing to convert
    if dialect == 'sqlite' and '%s' not in sql:
        return sql
    if dialect == 'postgresql' and '?' not in sql:
        return sql

    # Protect regexp_replace and string literals
    protected = set()
    for m in _REGEXP_RE.finditer(sql):
        protected.update(range(m.start(), m.end()))
    for m in _STR_RE.finditer(sql):
        protected.update(range(m.start(), m.end()))

    target = '?' if dialect == 'sqlite' else '%s'

    def replace(m):
        if m.start() in protected:
            return m.group(0)  # Preserve protected content
        return m.group(0) if m.group(1) else target  # Keep named params

    return _PH_RE.sub(replace, sql)


# ---------------------------------------------------------------------------
# Internal functions
# ---------------------------------------------------------------------------

def _find_contexts(sql: str) -> list[PH]:
    """Find placeholders with their contexts in one pass."""
    # Build protected ranges (strings + regexp calls)
    protected = set()
    for m in _STR_RE.finditer(sql):
        protected.update(range(m.start(), m.end()))
    for m in _REGEXP_RE.finditer(sql):
        protected.update(range(m.start(), m.end()))

    phs = []
    for m in _PH_RE.finditer(sql):
        if m.start() in protected:
            continue

        # Determine context from SQL prefix
        prefix = sql[:m.start()].upper().rstrip()
        ctx, in_parens = _parse_ctx(prefix)

        phs.append(PH(m.start(), m.end(), m.group(1), ctx, in_parens))

    return phs


def _parse_ctx(prefix: str) -> tuple[str, bool]:
    """Parse context from SQL prefix. Returns (context, in_parens)."""
    if prefix.endswith('('):
        inner = prefix[:-1].rstrip()
        if inner.endswith('IN'):
            return 'in', True
    if prefix.endswith('IN'):
        return 'in', False
    if prefix.endswith('IS NOT'):
        return 'is_not', False
    if prefix.endswith('IS'):
        return 'is', False
    return 'val', False


def _normalize(args: tuple | list | dict | None, phs: list[PH]) -> tuple | dict | None:
    """Normalize args."""
    if not args:
        return args

    # Rule 1: Dict passthrough
    if isinstance(args, dict):
        return args
    if len(args) == 1 and isinstance(args[0], dict):
        return args[0]

    in_count = sum(1 for p in phs if p.ctx == 'in')

    # Rule 2: Single IN with flat values -> wrap
    if in_count == 1 and len(phs) == 1 and _is_flat(args):
        return (tuple(args),)

    # Rule 3: Nested list/tuple handling
    if len(args) == 1 and _isseq(args[0]):
        inner = args[0]
        # [(a,b,c)] -> (a,b,c) if count matches
        if len(inner) == len(phs):
            return tuple(inner)
        # [[1,2,3]] -> ((1,2,3),) for single nested IN
        if len(inner) == 1 and _isseq(inner[0]):
            return (tuple(inner[0]),)

    # Rule 4: Multiple IN with lists -> convert inner lists to tuples
    if isinstance(args, list) and in_count > 1:
        result = []
        for i, arg in enumerate(args):
            ph = phs[i] if i < len(phs) else None
            if ph and ph.ctx == 'in' and isinstance(arg, list):
                result.append(tuple(arg))
            else:
                result.append(arg)
        return tuple(result)

    return tuple(args) if isinstance(args, list) else args


def _transform(sql: str, phs: list[PH], args: tuple | dict | None, dialect: str) -> tuple[str, Any]:
    """Transform SQL and args in single pass."""
    parts = []
    new_args = {} if isinstance(args, dict) else []
    pos = 0
    is_pg = dialect == 'postgresql'
    marker = '?' if dialect == 'sqlite' else '%s'

    for i, ph in enumerate(phs):
        # Text segment before placeholder
        seg = sql[pos:ph.pos]
        if is_pg:
            seg = _escape_percents(seg)
        parts.append(seg)

        # Process placeholder
        if isinstance(args, dict):
            sql_part, arg_upd = _proc_named(ph, args)
            parts.append(sql_part)
            new_args.update(arg_upd)
        else:
            val = args[i] if args and i < len(args) else None
            sql_part, arg_list = _proc_pos(ph, val, marker)
            parts.append(sql_part)
            new_args.extend(arg_list)

        pos = ph.end

    # Final segment
    seg = sql[pos:]
    if is_pg:
        seg = _escape_percents(seg)
    parts.append(seg)

    final_args = new_args if isinstance(args, dict) else tuple(new_args)
    return ''.join(parts), final_args


def _proc_pos(ph: PH, val: Any, marker: str) -> tuple[str, list]:
    """Process positional placeholder. Returns (sql, args)."""
    # IS NULL / IS NOT NULL
    if ph.ctx in {'is', 'is_not'} and val is None:
        return 'NULL', []

    # IN clause
    if ph.ctx == 'in':
        return _expand_in(val, marker, ph.in_parens)

    return marker, [val]


def _expand_in(val: Any, marker: str, in_parens: bool) -> tuple[str, list]:
    """Expand IN clause. Returns (sql, args)."""
    # Unwrap single nested sequence
    if _isseq(val) and len(val) == 1 and _isseq(val[0]):
        val = val[0]

    if _isseq(val):
        if not val:  # Empty -> NULL
            return 'NULL' if in_parens else '(NULL)', []

        phs = ', '.join([marker] * len(val))
        return phs if in_parens else f'({phs})', list(val)

    # Single value
    return marker if in_parens else f'({marker})', [val]


def _proc_named(ph: PH, args: dict) -> tuple[str, dict]:
    """Process named placeholder."""
    name = ph.name
    if name not in args:
        return f'%({name})s', {}

    val = args[name]

    # IS NULL / IS NOT NULL
    if ph.ctx in {'is', 'is_not'} and val is None:
        return 'NULL', {}

    # IN clause
    if ph.ctx == 'in':
        return _expand_named_in(name, val, ph.in_parens)

    return f'%({name})s', {name: val}


def _expand_named_in(name: str, val: Any, in_parens: bool) -> tuple[str, dict]:
    """Expand named IN clause."""
    # Unwrap single nested sequence
    if _isseq(val) and len(val) == 1 and _isseq(val[0]):
        val = val[0]

    if _isseq(val):
        if not val:
            return ('NULL', {}) if in_parens else ('(NULL)', {})

        new_args = {}
        phs = []
        for i, v in enumerate(val):
            key = f'{name}_{i}'
            phs.append(f'%({key})s')
            new_args[key] = v

        sql = ', '.join(phs)
        return (sql, new_args) if in_parens else (f'({sql})', new_args)

    # Single value
    key = f'{name}_0'
    sql = f'%({key})s'
    return (sql, {key: val}) if in_parens else (f'({sql})', {key: val})


def _escape_percents(segment: str) -> str:
    """Escape unescaped % in string literals, preserving regexp_replace."""
    # Protect regexp_replace calls
    regexps = []

    def save_regexp(m):
        regexps.append(m.group(0))
        return f'\x00R{len(regexps)-1}\x00'

    segment = _REGEXP_RE.sub(save_regexp, segment)

    # Escape % in string literals
    def esc_str(m):
        return _UNESCAPE_PCT.sub('%%', m.group(0))

    segment = _STR_RE.sub(esc_str, segment)

    # Restore regexp calls
    for i, r in enumerate(regexps):
        segment = segment.replace(f'\x00R{i}\x00', r)

    return segment


# Helpers
def _isseq(v: Any) -> bool:
    """Check if sequence (not string/dict)."""
    return issequence(v) and not isinstance(v, (str, dict))


def _is_flat(args: Any) -> bool:
    """Check if args is flat (no nested sequences)."""
    return _isseq(args) and all(not _isseq(a) for a in args)
