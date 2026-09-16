"""SQL parameter processing with clean tokenization.

This is a low-level utility module. For higher-level dialect-aware operations,
use strategy methods instead (e.g., strategy.standardize_sql(), strategy.get_placeholder_style()).

Public API:
- prepare_query(sql, args, dialect) - Main entry point for query processing
- quote_identifier(name, dialect) - Quote table/column names
- has_placeholders(sql) - Check for parameter placeholders
- has_named_placeholders(sql, dialect) - Check for named placeholders only
- standardize_placeholders(sql, dialect) - Convert %s <-> ?
- is_write_sql(sql, dialect) - Classify SQL as writing or reading
- raise_on_readonly_write(cn, sql) - Guard a read-only connection
- split_statements(sql, dialect) - Split on unprotected semicolons
- mask_protected_text(sql, dialect) - Blank literals and comments
"""
import re
from collections import namedtuple
from typing import Any

from database.exceptions import DatabaseError, ReadOnlyError, ValidationError

from libb import issequence

_SUPPORTED_DIALECTS = {'postgresql', 'sqlite'}

# Regex patterns
_PH_RE = re.compile(r'%\((\w+)\)s|%s|\?')  # Input placeholders (group 1 = named param name)
_HAS_PH_RE = re.compile(r'%\((\w+)\)s|%s|\?|(?<!:):\w+')  # Detection regex; also covers ':name' for sqlite
_STR_RE = re.compile(r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"")  # String literals
_REGEXP_RE = re.compile(r'regexp_replace\s*\([^)]*(?:\([^)]*\)[^)]*)*\)', re.I)
_UNESCAPE_PCT = re.compile(r'(?<!%)%(?![%s(])')  # Unescaped % not followed by % or s or (
_DOLLAR_OPEN_RE = re.compile(r'\$(\w*)\$')  # PG dollar-quoted-string opening tag
_NAMED_PYFORMAT_RE = re.compile(r'%\(\w+\)s')  # Named pyformat placeholder
_NAMED_COLON_RE = re.compile(r'(?<!:):\w+')  # sqlite named placeholder, ':' cast excluded

# Leading keyword of a statement that only reads
_READ_LEAD_WORDS = frozenset({'select', 'with', 'values', 'table', 'show'})
_LEAD_WORD_RE = re.compile(r'[A-Za-z_][A-Za-z_0-9]*')
_EXPLAIN_PREFIX_RE = re.compile(
    r'^explain\b(?:\s*\([^)]*\))?(?:\s+(?:analyze|verbose|query\s+plan))*', re.I)
_NESTED_WRITE_RE = re.compile(
    r'\b(?:insert|update|delete|merge|into|truncate)\b', re.I)
_ROW_LOCK_RE = re.compile(
    r'\bfor\s+(?:no\s+key\s+)?update\b|\bfor\s+(?:key\s+)?share\b', re.I)

# Every word _NESTED_WRITE_RE or _ROW_LOCK_RE can match, as a plain
# substring. A body holding none of them cannot match either pattern,
# and the substring scan runs in C where the alternations do not.
_WRITE_HINTS = ('insert', 'update', 'delete', 'merge', 'into', 'truncate',
                'for', 'key', 'share')
_PRAGMA_NAME_RE = re.compile(r'^pragma\s+(?:\w+\s*\.\s*)?(\w+)', re.I)
_ANALYZE_RE = re.compile(r'\banalyze\b', re.I)
_IDENT_CHARS = frozenset('_$')
_MASKABLE_RE = re.compile(r'[\'"$]|--|/\*')

# SQLite pragmas that only report. Every other pragma counts as a write,
# because the assignment forms include 'query_only', which turns the
# server-side half of the read-only guard back off.
_READ_PRAGMAS = frozenset({
    'collation_list',
    'database_list',
    'foreign_key_check',
    'foreign_key_list',
    'function_list',
    'index_info',
    'index_list',
    'index_xinfo',
    'integrity_check',
    'module_list',
    'pragma_list',
    'quick_check',
    'table_info',
    'table_xinfo',
    })

# Placeholder info: position, end, name (for named params), context, already in parens
PH = namedtuple('PH', 'pos end name ctx in_parens')


def prepare_query(sql: str, args: tuple | list | dict | None, dialect: str = 'postgresql') -> tuple[str, Any]:
    """Process SQL query with parameters for the given dialect.

    Handles:
    - IN clause expansion: `IN %s` with `(1,2,3)` -> `IN (%s,%s,%s)`
    - IS NULL handling: `IS %s` with `None` -> `IS NULL`
    - Placeholder conversion: `%s` <-> `?` based on dialect
    - Percent escaping in string literals for PostgreSQL
    """

    # Fast path: no placeholders
    if not sql or not _PH_RE.search(sql):
        return sql, args

    # Find placeholders with context
    phs = _find_contexts(sql, dialect)

    # Normalize args to canonical form
    args = _normalize(args, phs)

    # Transform SQL and args
    sql, args = _transform(sql, phs, args, dialect)

    return sql, args


def quote_identifier(identifier: str, dialect: str = 'postgresql') -> str:
    """Quote a table or column name safely.

    Dotted identifiers (e.g. 'public.foo') are split on the dot and each
    segment is quoted independently - so 'public.foo' becomes
    '"public"."foo"', not '"public.foo"'. A dot inside an already-quoted
    segment ('"weird.name"') is preserved as part of that segment.

    Standard SQL double-quote escaping is applied identically for all
    supported dialects.
    """
    if dialect not in _SUPPORTED_DIALECTS:
        raise DatabaseError(
            f'Unknown dialect: {dialect}. Supported: {_SUPPORTED_DIALECTS}'
        )
    if '\x00' in identifier:
        raise ValidationError(f'Identifier contains null byte: {identifier!r}')
    parts = _split_qualified_identifier(identifier)
    return '.'.join(f'"{p.replace(chr(34), chr(34) + chr(34))}"' for p in parts)


def _split_qualified_identifier(identifier: str) -> list[str]:
    """Split a possibly-qualified identifier on unquoted dots.

    A segment is "quoted" only when '"' appears at the start of the
    segment; '"' characters in the middle of an otherwise-unquoted
    segment are treated as literal data (and the caller will double-quote
    them). Inside a quoted segment, '""' is an escape for a literal '"'.
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
    """Check if SQL contains parameter placeholders.

    Covers pyformat ('%(name)s'), classic positional ('%s'), qmark ('?'),
    and sqlite named (':name') - i.e. any placeholder that might appear
    in SQL on its way to the DBAPI cursor. '::' (PG type cast) is
    explicitly excluded.
    """
    return bool(sql and _HAS_PH_RE.search(sql))


def has_named_placeholders(sql: str | None, dialect: str = 'postgresql') -> bool:
    """Report whether SQL carries named placeholders the caller must bind.

    Parameters
    ----------
    sql : str | None
        SQL to inspect, already standardized for the dialect.
    dialect : str, default 'postgresql'
        Which named syntaxes count. PostgreSQL recognizes pyformat
        '%(name)s' only; sqlite also recognizes ':name'.

    Returns
    -------
    bool
        True when at least one named placeholder sits outside a string
        literal, comment, or dollar-quoted body.

    Notes
    -----
    - This is what separates a dict argument that names parameters from
      a dict argument that IS a value (a JSON column, say). Without the
      distinction every dict is bound by name.
    - The ':name' form is left out for PostgreSQL on purpose: '::' casts
      and array slices ('arr[1:3]') would otherwise read as named
      placeholders.
    """
    if not sql:
        return False
    patterns = [_NAMED_PYFORMAT_RE]
    if dialect == 'sqlite':
        patterns.append(_NAMED_COLON_RE)
    protected = _protected_ranges(sql, dialect)
    return any(m.start() not in protected
               for pattern in patterns
               for m in pattern.finditer(sql))


def standardize_placeholders(sql: str, dialect: str = 'postgresql') -> str:
    """Convert placeholders between database dialects."""
    if not sql:
        return sql

    # Quick check: nothing to convert
    if dialect == 'sqlite' and '%s' not in sql:
        return sql
    if dialect == 'postgresql' and '?' not in sql:
        return sql

    protected = _protected_ranges(sql, dialect)
    target = '?' if dialect == 'sqlite' else '%s'

    def replace(m):
        if m.start() in protected:
            return m.group(0)  # Preserve protected content
        if (dialect == 'postgresql' and m.group(0) == '?'
                and _is_jsonb_op(sql, m.start())):
            return m.group(0)
        return m.group(0) if m.group(1) else target  # Keep named params

    return _PH_RE.sub(replace, sql)


def mask_protected_text(sql: str, dialect: str = 'postgresql') -> str | None:
    """Blank out every literal and comment, or report the text unscannable.

    Parameters
    ----------
    sql : str
        Statement text.
    dialect : str, default 'postgresql'
        Governs the two dialect-specific rules: PostgreSQL nests block
        comments, honors a backslash escape inside an E'' string, and
        has dollar quoting; SQLite has none of the three.

    Returns
    -------
    str | None
        The text with each literal and comment replaced by spaces,
        preserving length and offsets. None when a literal, comment, or
        dollar-quoted body never closes, so no offset after it can be
        trusted.

    Notes
    -----
    - Separate from _protected_ranges, which serves placeholder
      processing and must keep going on text this rejects. This one
      fails closed instead, so a caller reading it as a permission
      check cannot be fooled by an unterminated quote.
    - Leaves regexp_replace() calls alone. _protected_ranges masks the
      whole call, which would hide a write sitting after it.
    - Text carrying no quote, comment opener, or dollar sign is
      returned unchanged, which keeps the scan off a large generated
      statement that has nothing to mask.
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
            escaped = (dialect == 'postgresql' and char == "'"
                       and i and sql[i - 1] in 'Ee'
                       and not (i > 1 and (sql[i - 2].isalnum()
                                           or sql[i - 2] in _IDENT_CHARS)))
            j = i + 1
            closed = False
            while j < n:
                if escaped and sql[j] == '\\':
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
    """Split SQL on the semicolons that are not inside a literal.

    Parameters
    ----------
    sql : str
        One or more statements.
    dialect : str, default 'postgresql'
        Dialect whose literal and comment rules apply.

    Returns
    -------
    list[str]
        The statements, stripped, with blank ones dropped. A single
        element when there is nothing to split, and the whole text as
        one element when it cannot be scanned.

    Notes
    -----
    - A semicolon inside a string or a comment does not split.
      Splitting on the raw text executes a statement hidden behind
      '--' and breaks a query holding a semicolon in a literal.
    - Text with no semicolon at all skips the scan, which keeps the
      cost off the single-statement path every query takes.
    """
    if ';' not in sql:
        stripped = sql.strip()
        return [stripped] if stripped else []

    masked = mask_protected_text(sql, dialect)
    if masked is None:
        return [sql.strip()]
    return _split_on_semicolons(masked, sql)


def _split_on_semicolons(masked: str, original: str) -> list[str]:
    """Split original where masked carries an unprotected semicolon.

    Parameters
    ----------
    masked : str
        mask_protected_text output for original, so every semicolon
        left in it sits outside a literal and a comment.
    original : str
        Text to slice, offset for offset with masked. Callers that
        only need the masked form pass it for both, which is what
        keeps is_write_sql from masking twice.

    Returns
    -------
    list[str]
        Stripped statements, with blank ones dropped.
    """
    if ';' not in masked:
        stripped = original.strip()
        return [stripped] if stripped else []

    statements = []
    start = 0
    for pos, char in enumerate(masked):
        if char != ';':
            continue
        piece = original[start:pos].strip()
        if piece:
            statements.append(piece)
        start = pos + 1
    tail = original[start:].strip()
    if tail:
        statements.append(tail)
    return statements


def is_write_sql(sql: str, dialect: str = 'postgresql') -> bool:
    """Classify one or more statements as writing or reading.

    Parameters
    ----------
    sql : str
        Statement text, semicolon separated for a multi-statement call.
    dialect : str, default 'postgresql'
        Dialect whose literal, comment, and pragma rules apply.

    Returns
    -------
    bool
        True when any statement in sql can write. False only where
        every statement leads with a reading keyword and carries no
        data-modifying clause and no row lock.

    Notes
    -----
    - Fails closed. An unrecognized leading keyword, a body the mask
      cannot scan, and a body whose first token is not a word all read
      as writes, so a statement form this function has never seen is
      blocked rather than waved through.
    - A row-locking select ('for update', 'for share') counts as a
      write, because a standby refuses the lock.
    - 'set', 'reset', and every assigning pragma count as writes, which
      is what stops a caller clearing the server-side read-only
      setting. A reporting pragma such as 'pragma table_info(t)' reads.
    - 'explain' without 'analyze' reads: the server plans the inner
      statement without running it.
    - Blind to a write reached through a function, as in
      'select setval(...)'. The session read-only setting refuses most
      of those, though not every one, so this is a guard and not a
      proof.
    - Runs only for a read-only connection, and short-circuits on a
      cheap substring scan before either alternation, so a large
      generated statement does not pay for the regexes.
    """
    if not sql:
        return False

    masked = mask_protected_text(sql, dialect)
    if masked is None:
        return True

    for statement in _split_on_semicolons(masked, masked):
        body = statement.strip()
        explain = _EXPLAIN_PREFIX_RE.match(body)
        if explain is not None:
            if not _ANALYZE_RE.search(explain.group(0)):
                continue
            body = body[explain.end():]
        body = body.lstrip('( \t\r\n')
        if not body:
            continue
        lead = _LEAD_WORD_RE.match(body)
        if lead is None:
            return True
        keyword = lead.group(0).lower()
        if keyword == 'pragma':
            if _pragma_reads(body):
                continue
            return True
        if keyword not in _READ_LEAD_WORDS:
            return True
        lowered = body.lower()
        if not any(hint in lowered for hint in _WRITE_HINTS):
            continue
        if _NESTED_WRITE_RE.search(body) or _ROW_LOCK_RE.search(body):
            return True
    return False


def _pragma_reads(body: str) -> bool:
    """Return True for a pragma that only reports.

    Parameters
    ----------
    body : str
        A statement already known to lead with 'pragma', masked so no
        literal or comment remains.

    Returns
    -------
    bool
        True only for a whitelisted pragma name carrying no assignment.
    """
    if '=' in body:
        return False
    name = _PRAGMA_NAME_RE.match(body)
    return name is not None and name.group(1).lower() in _READ_PRAGMAS


def raise_on_readonly_write(cn: Any, sql: str) -> None:
    """Reject a write before it travels to a read-only connection.

    Parameters
    ----------
    cn : Any
        Connection-like object; read-only when its 'readonly' attribute
        is true, and its 'dialect' names the dialect to classify under.
    sql : str
        Statement text about to be executed.

    Raises
    ------
    ReadOnlyError
        When cn is read-only and sql can write.

    Notes
    -----
    - Classification runs only for a read-only connection, so a writer
      pays nothing for the guard.
    - An object carrying no 'readonly' attribute is treated as a
      writer, which keeps a raw DBAPI connection working unchanged.
    """
    if not getattr(cn, 'readonly', False):
        return
    if not is_write_sql(sql, getattr(cn, 'dialect', 'postgresql')):
        return
    statement = ' '.join(sql.split())[:120]
    raise ReadOnlyError(
        f'write rejected on a read-only connection: {statement}')


def _find_contexts(sql: str, dialect: str = 'postgresql') -> list[PH]:
    """Find placeholders with their contexts in one pass."""
    protected = _protected_ranges(sql, dialect)

    phs = []
    for m in _PH_RE.finditer(sql):
        if m.start() in protected:
            continue

        if (dialect == 'postgresql' and m.group(0) == '?'
                and _is_jsonb_op(sql, m.start())):
            continue

        # Determine context from SQL prefix
        prefix = sql[:m.start()].upper().rstrip()
        ctx, in_parens = _parse_ctx(prefix)

        phs.append(PH(m.start(), m.end(), m.group(1), ctx, in_parens))

    return phs


def _protected_ranges(sql: str, dialect: str = 'postgresql') -> set[int]:
    """Return character positions that are inside protected SQL contexts.

    Protected contexts:
    - String literals ('...' and "...") - handled in lexical order with
      precedence over comments and dollar quotes.
    - Single-line comments (--) and block comments (/* */).
    - Dollar-quoted bodies ($$...$$ and $tag$...$tag$) - PostgreSQL only.
    - regexp_replace(...) calls.

    Notes
    -----
    - A block comment closes at the first '*/' here, so a nested
      PostgreSQL comment leaves its tail unprotected. Placeholder
      processing wants that: it keeps going on text it cannot parse
      rather than refusing the query. mask_protected_text nests
      instead, because a permission check has to fail closed. Do not
      make one match the other.
    """
    protected: set[int] = set()
    n = len(sql)
    i = 0
    while i < n:
        c = sql[i]
        if c in {"'", '"'}:
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
        if c == '-' and i + 1 < n and sql[i + 1] == '-':
            j = sql.find('\n', i + 2)
            if j == -1:
                j = n
            protected.update(range(i, j))
            i = j
            continue
        if c == '/' and i + 1 < n and sql[i + 1] == '*':
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
    """Detect PostgreSQL JSONB '?' operator at pos.

    Returns True for the JSONB key-exists family - '?' followed (after
    optional whitespace) by a quoted literal, or paired into multi-char
    operators '?|' / '?&'. The caller must already know the dialect is
    'postgresql'.
    """
    n = len(sql)
    if pos + 1 < n and sql[pos + 1] in '|&':
        return True
    j = pos + 1
    while j < n and sql[j].isspace():
        j += 1
    return j < n and sql[j] in "'\""


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

    # Notes:
    # - A dict is a name->value binding map only when the SQL actually
    #   names a placeholder. Against '?' or '%s' it is an ordinary
    #   value (a JSON column, say), and binding it by name would emit
    #   ':None' from _proc_named, since a positional PH carries no name.
    has_named = any(p.name for p in phs)

    # Rule 1: Dict passthrough
    if isinstance(args, dict):
        return args if has_named else (args,)
    if len(args) == 1 and isinstance(args[0], dict) and has_named:
        return args[0]

    in_count = sum(1 for p in phs if p.ctx == 'in')

    # Rule 2: Single IN with flat values -> wrap
    if in_count == 1 and len(phs) == 1 and _is_flat(args):
        return (tuple(args),)

    # Rule 3: Nested list/tuple handling
    if len(args) == 1 and _isseq(args[0]):
        inner = args[0]
        # [(a,b,c)] -> (a,b,c) when there are multiple placeholders to fill.
        # With a single placeholder, never unpack: the inner sequence is the
        # value (e.g. ANY(%s) or = %s with an array). Unpacking a one-element
        # list under a single placeholder silently turns array params into
        # scalar binds and breaks `ANY(%s)`.
        if len(inner) == len(phs) and len(phs) > 1:
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
            sql_part, arg_upd = _proc_named(ph, args, dialect)
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


def _named_ph(name: str, dialect: str) -> str:
    """Return the named-placeholder syntax for the dialect.

    psycopg accepts pyformat '%(name)s'; sqlite3 only accepts the named
    style ':name'. Emitting the right shape here keeps the args dict
    untouched - both drivers look up by `name` as the dict key.
    """
    return f':{name}' if dialect == 'sqlite' else f'%({name})s'


def _proc_named(ph: PH, args: dict, dialect: str) -> tuple[str, dict]:
    """Process named placeholder."""
    name = ph.name
    if name not in args:
        return _named_ph(name, dialect), {}

    val = args[name]

    # IS NULL / IS NOT NULL
    if ph.ctx in {'is', 'is_not'} and val is None:
        return 'NULL', {}

    # IN clause
    if ph.ctx == 'in':
        return _expand_named_in(name, val, ph.in_parens, dialect)

    return _named_ph(name, dialect), {name: val}


def _expand_named_in(name: str, val: Any, in_parens: bool, dialect: str) -> tuple[str, dict]:
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
            phs.append(_named_ph(key, dialect))
            new_args[key] = v

        sql = ', '.join(phs)
        return (sql, new_args) if in_parens else (f'({sql})', new_args)

    # Single value
    key = f'{name}_0'
    sql = _named_ph(key, dialect)
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
