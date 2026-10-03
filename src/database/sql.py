"""SQL text processing: placeholders, identifier quoting, and scanning.
"""
import re
from collections import namedtuple
from typing import Any

from database.exceptions import DatabaseError, QueryError, ReadOnlyError
from database.exceptions import ValidationError

from libb import issequence

_SUPPORTED_DIALECTS = {'postgresql', 'sqlite'}

_PH_RE = re.compile(r'%\((\w+)\)s|%s|\?')
_HAS_PH_RE = re.compile(r'%\((\w+)\)s|%s|\?|(?<!:):\w+')
_STR_RE = re.compile(r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"")
_REGEXP_RE = re.compile(r'regexp_replace\s*\([^)]*(?:\([^)]*\)[^)]*)*\)', re.I)
_UNESCAPED_PCT_RE = re.compile(r'(?<!%)%(?![%s(])')
_DOLLAR_OPEN_RE = re.compile(r'\$(\w*)\$')
_NAMED_PYFORMAT_RE = re.compile(r'%\(\w+\)s')
_NAMED_SQLITE_RE = re.compile(r'(?<!:):\w+|(?<![\w$@])[$@]\w+')

_IDENT_CHARS = frozenset('_$')
_MASKABLE_RE = re.compile(r'[\'"$]|--|/\*')

_DISARM_WORDS = ('query_only', 'default_transaction_read_only', 'reset')
_DISARM_RE = re.compile(
    r'\bset\s+(?:session\s+|local\s+)?default_transaction_read_only\b'
    r'|\breset\s+(?:all\b|default_transaction_read_only\b)'
    r'|\bpragma\s+(?:\w+\s*\.\s*)?query_only\s*=', re.I)

PH = namedtuple('PH', 'pos end name ctx in_parens')


def prepare_query(
        sql: str,
        args: tuple | list | dict | None,
        dialect: str = 'postgresql') -> tuple[str, Any]:
    """SQL and args rewritten for the dialect's driver.

    Parameters
    ----------
    sql : str
        Statement text with '%s', '?' or '%(name)s' placeholders.
    args : tuple | list | dict | None
        Positional values, or a dict that binds by name when sql names a
        placeholder. A lone sequence fills several placeholders in order.
        A sequence at 'in %s' expands to one marker per item, and an empty
        one to '(null)'. None at 'is %s' or 'is not %s' inlines null.
    dialect : str, default 'postgresql'
        'sqlite' emits '?' and ':name'. Any other value emits '%s' and
        '%(name)s'.

    Returns
    -------
    tuple[str, Any]
        The rewritten SQL and its args, a tuple or a dict. Args come back
        unchanged when sql holds no '%s', '?' or '%(name)s' anywhere.

    Raises
    ------
    QueryError
        When the positional arg count differs from the placeholder count.
    """
    if not sql or not _PH_RE.search(sql):
        return sql, args
    phs = _find_contexts(sql, dialect)
    return _transform(sql, phs, _normalize(args, phs), dialect)


def quote_identifier(identifier: str, dialect: str = 'postgresql') -> str:
    """Identifier double-quoted per segment, safe to splice into SQL.

    Parameters
    ----------
    identifier : str
        Table or column name, optionally schema-qualified. Each unquoted
        dot separates a segment. A segment that opens with '"' is already
        quoted, and a dot inside it stays part of the name.
    dialect : str, default 'postgresql'
        'postgresql' or 'sqlite'.

    Returns
    -------
    str
        Each segment wrapped in '"', with any '"' inside it doubled.

    Raises
    ------
    DatabaseError
        When dialect is not supported.
    ValidationError
        When identifier contains a null byte.
    """
    if dialect not in _SUPPORTED_DIALECTS:
        raise DatabaseError(
            f'Unknown dialect: {dialect}. Supported: {_SUPPORTED_DIALECTS}')
    if '\x00' in identifier:
        raise ValidationError(f'Identifier contains null byte: {identifier!r}')
    return '.'.join(
        '"' + part.replace('"', '""') + '"'
        for part in _split_qualified_identifier(identifier))


def _split_qualified_identifier(identifier: str) -> list[str]:
    """Segments of a possibly qualified identifier, split on unquoted dots.

    Parameters
    ----------
    identifier : str
        A segment counts as quoted only when '"' is its first character.
        Inside a quoted segment '""' stands for one '"'. A '"' anywhere
        else is data.

    Returns
    -------
    list[str]
        Segment text without its enclosing quotes, in order. Never empty:
        '' gives [''].
    """
    parts: list[str] = []
    buf: list[str] = []
    in_quote = False
    i = 0
    n = len(identifier)
    while i < n:
        c = identifier[i]
        if in_quote:
            if c == '"':
                if i + 1 < n and identifier[i + 1] == '"':
                    buf.append('"')
                    i += 2
                    continue
                in_quote = False
                i += 1
                continue
            buf.append(c)
            i += 1
            continue
        if c == '"' and not buf:
            in_quote = True
            i += 1
            continue
        if c == '.':
            parts.append(''.join(buf))
            buf = []
            i += 1
            continue
        buf.append(c)
        i += 1
    parts.append(''.join(buf))
    return parts


def make_placeholders(count: int, dialect: str = 'postgresql') -> str:
    """Comma-joined run of count markers, such as '%s, %s' or '?, ?'.

    Parameters
    ----------
    count : int
        Number of markers. Zero gives ''.
    dialect : str, default 'postgresql'
        'sqlite' takes '?'. Any other value takes '%s'.

    Returns
    -------
    str
        The markers joined by ', '.
    """
    marker = '?' if dialect == 'sqlite' else '%s'
    return ', '.join([marker] * count)


def build_select_sql(
        table: str,
        dialect: str,
        columns: list[str] | None = None,
        where: str | None = None,
        order_by: str | None = None,
        limit: int | None = None) -> str:
    """Select statement over a quoted table.

    Parameters
    ----------
    table : str
        Table name, quoted per segment by quote_identifier.
    dialect : str
        'postgresql' or 'sqlite'.
    columns : list[str] | None, default None
        Columns to select, each quoted. None or empty selects '*'.
    where : str | None, default None
        Condition text, spliced in as written.
    order_by : str | None, default None
        Ordering text, spliced in as written.
    limit : int | None, default None
        Row limit. None emits no limit clause, and 0 emits 'limit 0'.

    Returns
    -------
    str
        The statement, with lower-case keywords.

    Raises
    ------
    DatabaseError
        When dialect is not supported.
    ValidationError
        When the table or a column name contains a null byte.
    """
    quoted_table = quote_identifier(table, dialect)

    if columns:
        quoted_cols = ', '.join(quote_identifier(col, dialect) for col in columns)
        select_clause = f'select {quoted_cols}'
    else:
        select_clause = 'select *'

    sql = f'{select_clause} from {quoted_table}'

    if where:
        sql += f' where {where}'

    if order_by:
        sql += f' order by {order_by}'

    if limit is not None:
        sql += f' limit {limit}'

    return sql


def build_insert_sql(dialect: str, table: str, columns: list[str]) -> str:
    """Insert statement with one placeholder per column.

    Parameters
    ----------
    dialect : str
        'postgresql' takes '%s' markers, 'sqlite' takes '?'.
    table : str
        Table name, quoted per segment by quote_identifier.
    columns : list[str]
        Column names, each quoted, in the order the values bind.

    Returns
    -------
    str
        The statement, with lower-case keywords.

    Raises
    ------
    DatabaseError
        When dialect is not supported.
    ValidationError
        When the table or a column name contains a null byte.
    """
    quoted_table = quote_identifier(table, dialect)
    quoted_columns = ', '.join(quote_identifier(col, dialect) for col in columns)
    placeholders = make_placeholders(len(columns), dialect)

    return f'insert into {quoted_table} ({quoted_columns}) values ({placeholders})'


def has_placeholders(sql: str | None) -> bool:
    """True when sql holds a placeholder in any style a driver takes.

    Parameters
    ----------
    sql : str | None
        Statement text. Empty or None gives False.

    Returns
    -------
    bool
        True for '%s', '?', '%(name)s' or ':name'. A PostgreSQL '::' cast
        does not count. Unlike has_named_placeholders, a placeholder inside
        a literal or comment counts.
    """
    return bool(sql and _HAS_PH_RE.search(sql))


def has_named_placeholders(sql: str | None, dialect: str = 'postgresql') -> bool:
    """True when sql holds a named placeholder outside protected text.

    Parameters
    ----------
    sql : str | None
        Statement text, already standardized for the dialect.
    dialect : str, default 'postgresql'
        Which named syntaxes count. PostgreSQL recognizes pyformat
        '%(name)s' only; sqlite also recognizes ':name', '$name' and
        '@name'.

    Returns
    -------
    bool
        True when a named placeholder sits outside a string literal,
        comment, or dollar-quoted body.
    """
    if not sql:
        return False
    patterns = [_NAMED_PYFORMAT_RE]
    if dialect == 'sqlite':
        patterns.append(_NAMED_SQLITE_RE)
    protected = _protected_ranges(sql, dialect)
    return any(m.start() not in protected
               for pattern in patterns
               for m in pattern.finditer(sql))


def standardize_placeholders(sql: str, dialect: str = 'postgresql') -> str:
    """SQL with its placeholders rewritten into the dialect's style.

    Parameters
    ----------
    sql : str
        Statement text. An empty string or None comes back as given.
    dialect : str, default 'postgresql'
        'sqlite' turns '%s' into '?' and '%(name)s' into ':name'. Any
        other value turns '?' into '%s' and keeps '%(name)s'.

    Returns
    -------
    str
        The rewritten statement. A placeholder inside a string literal, a
        comment, or a dollar-quoted body stays as written, and so does a
        PostgreSQL JSONB '?' operator.
    """
    if not sql:
        return sql
    if dialect == 'sqlite' and '%' not in sql:
        return sql
    if dialect == 'postgresql' and '?' not in sql:
        return sql

    protected = _protected_ranges(sql, dialect)
    target = '?' if dialect == 'sqlite' else '%s'

    def replace(m: re.Match[str]) -> str:
        if m.start() in protected:
            return m.group(0)
        if (dialect == 'postgresql' and m.group(0) == '?'
                and _is_jsonb_op(sql, m.start())):
            return m.group(0)
        if m.group(1):
            return _named_ph(m.group(1), dialect)
        return target

    return _PH_RE.sub(replace, sql)


def mask_protected_text(sql: str, dialect: str = 'postgresql') -> str | None:
    """Blank out every literal and comment, or report the text unscannable.

    Parameters
    ----------
    sql : str
        Statement text.
    dialect : str, default 'postgresql'
        PostgreSQL nests block comments, honors a backslash escape inside
        an E'' string, and has dollar quoting. SQLite has none of the
        three.

    Returns
    -------
    str | None
        The text with each literal and comment replaced by spaces,
        preserving length and offsets. None when a literal, comment, or
        dollar-quoted body never closes, so no offset after it can be
        trusted.
    """
    if not sql:
        return sql
    if not _MASKABLE_RE.search(sql):
        return sql

    out = list(sql)
    n = len(sql)
    i = 0

    while i < n:
        char = sql[i]

        if char in {"'", '"'}:
            is_escape_string = (
                dialect == 'postgresql' and char == "'"
                and i and sql[i - 1] in 'Ee'
                and not (i > 1 and (sql[i - 2].isalnum()
                                    or sql[i - 2] in _IDENT_CHARS)))
            j = i + 1
            closed = False
            while j < n:
                if is_escape_string and sql[j] == '\\':
                    j += 2
                    continue
                if sql[j] == char:
                    if j + 1 < n and sql[j + 1] == char:
                        j += 2
                        continue
                    j += 1
                    closed = True
                    break
                j += 1
            if not closed:
                return None
            out[i:j] = ' ' * (j - i)
            i = j
            continue

        if char == '-' and sql.startswith('--', i):
            j = sql.find('\n', i + 2)
            j = n if j == -1 else j
            out[i:j] = ' ' * (j - i)
            i = j
            continue

        if char == '/' and sql.startswith('/*', i):
            depth = 1
            j = i + 2
            while j < n and depth:
                if dialect == 'postgresql' and sql.startswith('/*', j):
                    depth += 1
                    j += 2
                    continue
                if sql.startswith('*/', j):
                    depth -= 1
                    j += 2
                    continue
                j += 1
            if depth:
                return None
            out[i:j] = ' ' * (j - i)
            i = j
            continue

        if dialect == 'postgresql' and char == '$':
            match = _DOLLAR_OPEN_RE.match(sql, i)
            if match:
                tag = match.group(0)
                close = sql.find(tag, match.end())
                if close == -1:
                    return None
                j = close + len(tag)
                out[i:j] = ' ' * (j - i)
                i = j
                continue

        i += 1

    return ''.join(out)


def split_statements(sql: str, dialect: str = 'postgresql') -> list[str]:
    """Statements of sql, split on semicolons outside a literal or comment.

    Parameters
    ----------
    sql : str
        One or more statements.
    dialect : str, default 'postgresql'
        Dialect whose literal and comment rules apply.

    Returns
    -------
    list[str]
        The statements, stripped, with blank ones dropped. The whole text
        as one element when mask_protected_text cannot scan it.
    """
    masked = mask_protected_text(sql, dialect) if ';' in sql else None
    if masked is None or ';' not in masked:
        stripped = sql.strip()
        return [stripped] if stripped else []

    statements = []
    start = 0
    for pos, char in enumerate(masked):
        if char != ';':
            continue
        piece = sql[start:pos].strip()
        if piece:
            statements.append(piece)
        start = pos + 1
    tail = sql[start:].strip()
    if tail:
        statements.append(tail)
    return statements


def raise_on_readonly_disarm(cn: Any, sql: str) -> None:
    """Refuse a statement that would unlock a read-only connection.

    Parameters
    ----------
    cn : Any
        Connection-like object, read-only when its 'readonly' attribute
        is true. A missing attribute counts as a writer. Its 'dialect'
        attribute, default 'postgresql', picks the scan rules.
    sql : str
        Statement text about to be executed.

    Raises
    ------
    ReadOnlyError
        When cn is read-only and sql sets or resets the dialect's
        read-only session setting. Also when sql mentions 'query_only',
        'default_transaction_read_only' or 'reset' and holds a literal,
        comment or dollar body that never closes.
    """
    if not getattr(cn, 'readonly', False) or not sql:
        return
    lowered = sql.lower()
    if not any(word in lowered for word in _DISARM_WORDS):
        return
    dialect = getattr(cn, 'dialect', 'postgresql')
    masked = mask_protected_text(sql, dialect)
    if masked is not None and not _DISARM_RE.search(masked):
        return
    statement = ' '.join(sql.split())[:120]
    raise ReadOnlyError(
        f'a read-only connection may not change its read-only '
        f'setting: {statement}')


def _find_contexts(sql: str, dialect: str) -> list[PH]:
    """Placeholders the args must fill, in text order, with their context.

    Parameters
    ----------
    sql : str
        Statement text.
    dialect : str
        Picks the protected-text rules. On 'postgresql' a JSONB '?'
        operator is not a placeholder.

    Returns
    -------
    list[PH]
        One entry per placeholder outside protected text.
    """
    protected = _protected_ranges(sql, dialect)

    phs = []
    for m in _PH_RE.finditer(sql):
        if m.start() in protected:
            continue
        if (dialect == 'postgresql' and m.group(0) == '?'
                and _is_jsonb_op(sql, m.start())):
            continue
        prefix = sql[:m.start()].upper().rstrip()
        ctx, in_parens = _parse_ctx(prefix)
        phs.append(PH(m.start(), m.end(), m.group(1), ctx, in_parens))

    return phs


def _protected_ranges(sql: str, dialect: str) -> set[int]:
    """Offsets inside a literal, comment, dollar body or regexp_replace call.

    Parameters
    ----------
    sql : str
        Statement text.
    dialect : str
        'postgresql' adds dollar-quoted bodies. 'sqlite' adds backtick
        and bracket identifiers.

    Returns
    -------
    set[int]
        Protected offsets.
    """
    protected: set[int] = set()
    n = len(sql)
    i = 0
    while i < n:
        c = sql[i]
        if c in {"'", '"'} or (dialect == 'sqlite' and c == '`'):
            j = i + 1
            while j < n:
                if sql[j] == c:
                    if j + 1 < n and sql[j + 1] == c:
                        j += 2
                        continue
                    j += 1
                    break
                j += 1
            else:
                j = n
            protected.update(range(i, j))
            i = j
            continue
        if dialect == 'sqlite' and c == '[':
            j = sql.find(']', i + 1)
            j = n if j == -1 else j + 1
            protected.update(range(i, j))
            i = j
            continue
        if c == '-' and sql.startswith('--', i):
            j = sql.find('\n', i + 2)
            j = n if j == -1 else j
            protected.update(range(i, j))
            i = j
            continue
        if c == '/' and sql.startswith('/*', i):
            j = sql.find('*/', i + 2)
            j = n if j == -1 else j + 2
            protected.update(range(i, j))
            i = j
            continue
        if dialect == 'postgresql' and c == '$':
            m = _DOLLAR_OPEN_RE.match(sql, i)
            if m:
                tag = m.group(0)
                close = sql.find(tag, m.end())
                j = n if close == -1 else close + len(tag)
                protected.update(range(i, j))
                i = j
                continue
        i += 1
    for m in _REGEXP_RE.finditer(sql):
        if m.start() not in protected:
            protected.update(range(m.start(), m.end()))
    return protected


def _is_jsonb_op(sql: str, pos: int) -> bool:
    """True when the '?' at pos is a PostgreSQL JSONB key-exists operator.

    Parameters
    ----------
    sql : str
        PostgreSQL statement text.
    pos : int
        Offset of a '?'.

    Returns
    -------
    bool
        True for '?|', '?&', or a '?' whose next non-space character is a
        quote.
    """
    n = len(sql)
    if pos + 1 < n and sql[pos + 1] in '|&':
        return True
    j = pos + 1
    while j < n and sql[j].isspace():
        j += 1
    return j < n and sql[j] in "'\""


def _parse_ctx(prefix: str) -> tuple[str, bool]:
    """Context of the placeholder that follows prefix.

    Parameters
    ----------
    prefix : str
        The SQL before the placeholder, upper-cased and right-stripped.

    Returns
    -------
    tuple[str, bool]
        The PH ctx, and whether the IN list's '(' is already written.
    """
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
    """Args reshaped to bind against phs, by name or in order.

    Parameters
    ----------
    args : tuple | list | dict | None
        Args as the caller passed them.
    phs : list[PH]
        Placeholders the args must fill.

    Returns
    -------
    tuple | dict | None
        A dict to bind by name, or a tuple to bind in order. Empty args
        come back as given, except against a lone IN placeholder, which
        gets ((),).
    """
    if not args:
        if len(phs) == 1 and phs[0].ctx == 'in':
            return ((),)
        return args

    has_named = any(p.name for p in phs)

    if isinstance(args, dict):
        return args if has_named else (args,)
    if len(args) == 1 and isinstance(args[0], dict) and has_named:
        return args[0]

    in_count = sum(1 for p in phs if p.ctx == 'in')

    if (in_count == 1 and len(phs) == 1
            and _isseq(args) and not any(_isseq(arg) for arg in args)):
        return (tuple(args),)

    if len(args) == 1 and _isseq(args[0]):
        inner = args[0]
        if len(inner) == len(phs) and len(phs) > 1:
            return tuple(inner)
        if len(inner) == 1 and _isseq(inner[0]):
            return (tuple(inner[0]),)

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


def _transform(
        sql: str,
        phs: list[PH],
        args: tuple | dict | None,
        dialect: str) -> tuple[str, Any]:
    """Rewrite each placeholder for the dialect and rebuild the args to match.

    Parameters
    ----------
    sql : str
        Statement text.
    phs : list[PH]
        Placeholders found in sql, in text order.
    args : tuple | dict | None
        Normalized args. A dict binds by name.
    dialect : str
        'postgresql' or 'sqlite'.

    Returns
    -------
    tuple[str, Any]
        The rewritten SQL, and a tuple of positional args or a dict of
        named args.

    Raises
    ------
    QueryError
        When the positional arg count differs from the placeholder count.
    """
    parts = []
    if isinstance(args, dict):
        pyformat_names = {ph.name for ph in phs}
        new_args = {
            name: val for name, val in args.items()
            if name not in pyformat_names
            }
    else:
        if len(args or ()) != len(phs):
            raise QueryError(
                f'Parameter count mismatch: SQL needs {len(phs)} '
                f'but {len(args or ())} were provided')
        new_args = []
    pos = 0
    is_pg = dialect == 'postgresql'
    marker = '?' if dialect == 'sqlite' else '%s'

    for i, ph in enumerate(phs):
        seg = sql[pos:ph.pos]
        if is_pg:
            seg = _escape_percents(seg)
        parts.append(seg)

        if isinstance(args, dict):
            sql_part, arg_upd = _proc_named(ph, args, dialect)
            parts.append(sql_part)
            new_args.update(arg_upd)
        else:
            sql_part, arg_list = _proc_pos(ph, args[i], marker)
            parts.append(sql_part)
            new_args.extend(arg_list)

        pos = ph.end

    seg = sql[pos:]
    if is_pg:
        seg = _escape_percents(seg)
    parts.append(seg)

    final_args = new_args if isinstance(args, dict) else tuple(new_args)
    return ''.join(parts), final_args


def _proc_pos(ph: PH, val: Any, marker: str) -> tuple[str, list]:
    """SQL and args for one placeholder when the args bind in order.

    Parameters
    ----------
    ph : PH
        The placeholder.
    val : Any
        The arg for ph.
    marker : str
        '%s' or '?'.

    Returns
    -------
    tuple[str, list]
        The replacement SQL, and the args it binds in order.
    """
    if ph.ctx in {'is', 'is_not'} and val is None:
        return 'null', []
    if ph.ctx == 'in':
        return _expand_in(val, marker, ph.in_parens)
    return marker, [val]


def _expand_in(val: Any, marker: str, in_parens: bool) -> tuple[str, list]:
    """IN-list SQL and args for one positional value.

    Parameters
    ----------
    val : Any
        A sequence spreads to one marker per item, after a sequence
        holding one sequence unwraps. Any other value is one item.
    marker : str
        '%s' or '?'.
    in_parens : bool
        True when the SQL already holds the list's parentheses.

    Returns
    -------
    tuple[str, list]
        The list SQL and its args. An empty sequence gives null and no
        args, so the list matches no row.
    """
    if _isseq(val) and len(val) == 1 and _isseq(val[0]):
        val = val[0]

    if _isseq(val):
        if not val:
            return 'null' if in_parens else '(null)', []
        phs = ', '.join([marker] * len(val))
        return phs if in_parens else f'({phs})', list(val)

    return marker if in_parens else f'({marker})', [val]


def _named_ph(name: str, dialect: str) -> str:
    """Named placeholder for the dialect: ':name' for sqlite, else '%(name)s'.
    """
    return f':{name}' if dialect == 'sqlite' else f'%({name})s'


def _proc_named(ph: PH, args: dict, dialect: str) -> tuple[str, dict]:
    """SQL and args for one placeholder when the args bind by name.

    Parameters
    ----------
    ph : PH
        The placeholder. An unnamed one ('?' or '%s') takes no arg.
    args : dict
        Named args.
    dialect : str
        'postgresql' or 'sqlite'.

    Returns
    -------
    tuple[str, dict]
        The replacement SQL, and the args it binds.
    """
    name = ph.name
    if name is None:
        return '?' if dialect == 'sqlite' else '%s', {}
    if name not in args:
        return _named_ph(name, dialect), {}

    val = args[name]
    if ph.ctx in {'is', 'is_not'} and val is None:
        return 'null', {}
    if ph.ctx == 'in':
        return _expand_named_in(name, val, ph.in_parens, dialect)
    return _named_ph(name, dialect), {name: val}


def _expand_named_in(
        name: str,
        val: Any,
        in_parens: bool,
        dialect: str) -> tuple[str, dict]:
    """IN-list SQL and args for one named value.

    Parameters
    ----------
    name : str
        Placeholder name. Item i binds under the key f'{name}_{i}'.
    val : Any
        A sequence spreads to one placeholder per item, after a sequence
        holding one sequence unwraps. Any other value is item 0.
    in_parens : bool
        True when the SQL already holds the list's parentheses.
    dialect : str
        'postgresql' or 'sqlite'.

    Returns
    -------
    tuple[str, dict]
        The list SQL and its args. An empty sequence gives null and no
        args, so the list matches no row.
    """
    if _isseq(val) and len(val) == 1 and _isseq(val[0]):
        val = val[0]

    if _isseq(val):
        if not val:
            return ('null', {}) if in_parens else ('(null)', {})

        new_args = {}
        phs = []
        for i, v in enumerate(val):
            key = f'{name}_{i}'
            phs.append(_named_ph(key, dialect))
            new_args[key] = v

        sql = ', '.join(phs)
        return (sql, new_args) if in_parens else (f'({sql})', new_args)

    key = f'{name}_0'
    sql = _named_ph(key, dialect)
    return (sql, {key: val}) if in_parens else (f'({sql})', {key: val})


def _escape_percents(segment: str) -> str:
    """Segment with lone '%' in literals doubled, except in regexp_replace.
    """
    regexps = []

    def save_regexp(m: re.Match[str]) -> str:
        regexps.append(m.group(0))
        return f'\x00R{len(regexps) - 1}\x00'

    def double_percents(m: re.Match[str]) -> str:
        return _UNESCAPED_PCT_RE.sub('%%', m.group(0))

    segment = _REGEXP_RE.sub(save_regexp, segment)
    segment = _STR_RE.sub(double_percents, segment)
    for i, regexp in enumerate(regexps):
        segment = segment.replace(f'\x00R{i}\x00', regexp)
    return segment


def _isseq(v: Any) -> bool:
    """True for a Sequence other than a str or dict.
    """
    return issequence(v) and not isinstance(v, (str, dict))
