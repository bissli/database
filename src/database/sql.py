"""SQL parameter processing with clean tokenization.

Public API:
- prepare_query(cn, sql, args) - Main entry point for query processing
- quote_identifier(name, dialect) - Quote table/column names
- has_placeholders(sql) - Check for parameter placeholders
- standardize_placeholders(sql, dialect) - Convert %s <-> ?
"""
import re
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any

from database.types import TypeConverter
from database.utils.connection_utils import get_dialect_name

from libb import issequence


class TokenType(Enum):
    """Token types for SQL parsing."""
    TEXT = auto()        # Raw SQL text
    STRING = auto()      # 'quoted' or "quoted" literals
    PLACEHOLDER = auto()  # %s, ?, %(name)s
    REGEXP = auto()      # regexp_replace(...) calls


@dataclass(slots=True)
class Token:
    """A token from SQL parsing."""
    type: TokenType
    text: str
    name: str | None = None  # For named placeholders %(name)s


@dataclass(slots=True)
class Placeholder:
    """Information about a placeholder and its context."""
    token: Token
    index: int              # Position in token list
    name: str | None        # For %(name)s
    context: str = 'value'  # 'value', 'in_clause', 'is_null', 'is_not_null'
    in_parens: bool = False  # Already in parens: IN (%s)


# Master tokenization pattern
_RE_TOKEN = re.compile(r"""
    (?P<string>'(?:[^']|'')*'|"(?:[^"]|"")*")
    |(?P<regexp>regexp_replace\s*\((?:[^()'"]*|'(?:[^']|'')*'|"(?:[^"]|"")*"|\([^()]*\))*\))
    |(?P<named>%\((?P<pname>[^)]+)\)s)
    |(?P<positional>%s|\?)
""", re.VERBOSE | re.IGNORECASE)

# Quick placeholder check (no tokenization needed)
_RE_HAS_PH = re.compile(r'%s|\?|%\([^)]+\)s')

# Unescaped percent in string content
_RE_UNESCAPED_PCT = re.compile(r'(?<!%)%(?![%s(])')


def prepare_query(
    cn: Any,
    sql: str,
    args: tuple | list | dict | None
) -> tuple[str, tuple | dict]:
    """Process SQL query with parameters for the connection's dialect.

    This is the main entry point. It handles:
    - IN clause expansion: `IN %s` with `(1,2,3)` → `IN (%s,%s,%s)`
    - IS NULL handling: `IS %s` with `None` → `IS NULL`
    - Placeholder conversion: `%s` ↔ `?` based on dialect
    - Percent escaping in string literals for PostgreSQL

    Args:
        cn: Database connection or transaction
        sql: SQL query with placeholders
        args: Query parameters (tuple, list, or dict)

    Returns
        Tuple of (processed_sql, processed_args)
    """
    dialect = get_dialect_name(cn)

    if not sql or not _RE_HAS_PH.search(sql):
        converted_args = TypeConverter.convert_params(args) if args else args
        return sql, converted_args

    # Tokenize and analyze
    tokens = _tokenize(sql)
    placeholders = _analyze(tokens)

    # Normalize arguments
    normalized = _normalize_args(args, placeholders)

    # Transform SQL and args
    result_sql, result_args = _transform(tokens, placeholders, normalized, dialect)

    # Convert types for the database driver
    converted_args = TypeConverter.convert_params(result_args)

    return result_sql, converted_args


def quote_identifier(identifier: str, dialect: str = 'postgresql') -> str:
    """Quote a table or column name safely.

    Args:
        identifier: The name to quote
        dialect: 'postgresql' or 'sqlite'

    Returns
        Safely quoted identifier

    Raises
        ValueError: For unknown dialects
    """
    if dialect not in {'postgresql', 'sqlite'}:
        raise ValueError(f'Unknown dialect: {dialect}')

    # Both PostgreSQL and SQLite use double quotes, escape by doubling
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def has_placeholders(sql: str | None) -> bool:
    """Check if SQL contains parameter placeholders.

    Args:
        sql: SQL string to check

    Returns
        True if sql contains %s, ?, or %(name)s placeholders
    """
    if not sql:
        return False
    return bool(_RE_HAS_PH.search(sql))


def standardize_placeholders(sql: str, dialect: str = 'postgresql') -> str:
    """Convert placeholders between database dialects.

    Args:
        sql: SQL string with placeholders
        dialect: Target dialect ('postgresql' uses %s, 'sqlite' uses ?)

    Returns
        SQL with converted placeholders
    """
    if not sql:
        return sql

    if dialect == 'sqlite' and '%s' not in sql:
        return sql
    if dialect == 'postgresql' and '?' not in sql:
        return sql

    tokens = _tokenize(sql)
    result = []

    for token in tokens:
        if token.type == TokenType.PLACEHOLDER and token.name is None:
            # Positional placeholder - convert based on dialect
            if dialect == 'sqlite':
                result.append('?')
            else:
                result.append('%s')
        else:
            result.append(token.text)

    return ''.join(result)


def _tokenize(sql: str) -> list[Token]:
    """Parse SQL into tokens."""
    tokens = []
    pos = 0

    for match in _RE_TOKEN.finditer(sql):
        # Add any text before this match
        if match.start() > pos:
            tokens.append(Token(TokenType.TEXT, sql[pos:match.start()]))

        # Determine token type
        if match.group('string'):
            tokens.append(Token(TokenType.STRING, match.group('string')))
        elif match.group('regexp'):
            tokens.append(Token(TokenType.REGEXP, match.group('regexp')))
        elif match.group('named'):
            name = match.group('pname')
            tokens.append(Token(TokenType.PLACEHOLDER, match.group('named'), name))
        elif match.group('positional'):
            tokens.append(Token(TokenType.PLACEHOLDER, match.group('positional')))

        pos = match.end()

    # Add any remaining text
    if pos < len(sql):
        tokens.append(Token(TokenType.TEXT, sql[pos:]))

    return tokens


def _analyze(tokens: list[Token]) -> list[Placeholder]:
    """Analyze placeholder contexts (IN clause, IS NULL, etc.)."""
    placeholders = []
    ph_idx = 0

    for i, token in enumerate(tokens):
        if token.type != TokenType.PLACEHOLDER:
            continue

        context = 'value'
        in_parens = False

        # Look back for context keywords
        prev_text = _get_prev_text(tokens, i).upper()

        if _ends_with_in(prev_text):
            context = 'in_clause'
            # Check if already in parentheses
            stripped = prev_text.rstrip()
            if stripped.endswith('('):
                in_parens = True
        elif prev_text.rstrip().endswith('IS NOT'):
            context = 'is_not_null'
        elif prev_text.rstrip().endswith('IS'):
            context = 'is_null'

        placeholders.append(Placeholder(
            token=token,
            index=i,
            name=token.name,
            context=context,
            in_parens=in_parens
        ))
        ph_idx += 1

    return placeholders


def _get_prev_text(tokens: list[Token], idx: int) -> str:
    """Get concatenated text of previous TEXT tokens."""
    parts = []
    for i in range(idx - 1, -1, -1):
        if tokens[i].type == TokenType.TEXT:
            parts.insert(0, tokens[i].text)
            # Stop at significant SQL (not just whitespace)
            if tokens[i].text.strip():
                break
        else:
            break
    return ''.join(parts)


def _ends_with_in(text: str) -> bool:
    """Check if text ends with IN keyword."""
    stripped = text.rstrip()
    # Handle "IN", "IN ", "IN  (", etc.
    if stripped.endswith('('):
        stripped = stripped[:-1].rstrip()
    return stripped.endswith('IN')


def _normalize_args(
    args: tuple | list | dict | None,
    placeholders: list[Placeholder]
) -> tuple | dict:
    """Normalize arguments to canonical form.

    Rules (in order):
    1. None/empty → pass through
    2. dict or [dict] → return dict
    3. [1,2,3] for single IN → ((1,2,3),)
    4. [(a,b,c)] matching count → (a,b,c)
    5. list → tuple
    """
    if not args:
        return args

    if isinstance(args, dict):
        return args

    if isinstance(args, (list, tuple)):
        # [dict] → dict
        if len(args) == 1 and isinstance(args[0], dict):
            return args[0]

        in_count = sum(1 for p in placeholders if p.context == 'in_clause')
        total = len(placeholders)

        # Single IN with flat values: [1,2,3] → ((1,2,3),)
        if in_count == 1 and total == 1 and _is_flat_sequence(args):
            return (tuple(args),)

        # Single item for single IN: [101] → ((101,),)
        if (in_count == 1 and total == 1 and isinstance(args, list)
                and len(args) == 1 and not _is_sequence_not_str(args[0])):
            return ((args[0],),)

        # Nested once: [(a,b,c)] → (a,b,c) if count matches
        if (len(args) == 1 and _is_sequence_not_str(args[0])
                and len(args[0]) == total):
            return tuple(args[0])

        # Double nested: [[(1,2,3)]] → ((1,2,3),)
        if (len(args) == 1 and _is_sequence_not_str(args[0])
                and len(args[0]) == 1 and _is_sequence_not_str(args[0][0])):
            return (tuple(args[0][0]),)

        # Multiple IN clauses with lists: convert inner lists to tuples
        if isinstance(args, list) and in_count > 1:
            normalized = []
            for i, arg in enumerate(args):
                ph = placeholders[i] if i < len(placeholders) else None
                if ph and ph.context == 'in_clause' and isinstance(arg, list):
                    normalized.append(tuple(arg))
                else:
                    normalized.append(arg)
            return tuple(normalized)

    return tuple(args) if isinstance(args, list) else args


def _is_flat_sequence(args: Any) -> bool:
    """Check if args is a flat sequence (no nested sequences except strings)."""
    if not _is_sequence_not_str(args):
        return False
    return all(not _is_sequence_not_str(a) for a in args)


def _is_sequence_not_str(val: Any) -> bool:
    """Check if value is a sequence but not a string."""
    return issequence(val) and not isinstance(val, str)


def _transform(
    tokens: list[Token],
    placeholders: list[Placeholder],
    args: tuple | dict,
    dialect: str
) -> tuple[str, tuple | dict]:
    """Transform SQL and args based on placeholder contexts."""
    result_parts = []
    is_dict = isinstance(args, dict)
    result_args = {} if is_dict else []

    ph_idx = 0
    args_idx = 0
    skip_next_close = False

    for token in tokens:
        if token.type == TokenType.STRING:
            # Escape percent signs in literals for PostgreSQL
            if dialect == 'postgresql':
                result_parts.append(_escape_percent(token.text))
            else:
                result_parts.append(token.text)

        elif token.type == TokenType.REGEXP:
            # Preserve regexp_replace unchanged
            result_parts.append(token.text)

        elif token.type == TokenType.PLACEHOLDER:
            if ph_idx < len(placeholders):
                ph = placeholders[ph_idx]

                if is_dict:
                    sql_part, arg_updates = _process_named_ph(ph, args, dialect)
                    result_parts.append(sql_part)
                    result_args.update(arg_updates)
                else:
                    sql_part, new_args, skip = _process_positional_ph(
                        ph, args, args_idx, dialect
                    )
                    result_parts.append(sql_part)
                    result_args.extend(new_args)
                    skip_next_close = skip
                    args_idx += 1

                ph_idx += 1
            else:
                result_parts.append(_dialect_ph(dialect))

        elif token.type == TokenType.TEXT:
            text = token.text
            # Skip close paren if IN clause expansion added parens
            if skip_next_close and text.lstrip().startswith(')'):
                text = text.lstrip()[1:]  # Remove the )
                skip_next_close = False
            result_parts.append(text)

        else:
            result_parts.append(token.text)

    final_sql = ''.join(result_parts)
    final_args = result_args if is_dict else tuple(result_args)

    return final_sql, final_args


def _process_positional_ph(
    ph: Placeholder,
    args: tuple,
    args_idx: int,
    dialect: str
) -> tuple[str, list, bool]:
    """Process a positional placeholder. Returns (sql, args, skip_close_paren)."""
    placeholder = _dialect_ph(dialect)

    if args_idx >= len(args):
        return placeholder, [], False

    value = args[args_idx]

    # IS NULL / IS NOT NULL
    if ph.context in {'is_null', 'is_not_null'} and value is None:
        return 'NULL', [], False

    # IN clause
    if ph.context == 'in_clause':
        return _expand_in(value, dialect, ph.in_parens)

    return placeholder, [value], False


def _process_named_ph(
    ph: Placeholder,
    args: dict,
    dialect: str
) -> tuple[str, dict]:
    """Process a named placeholder."""
    name = ph.name

    if name not in args:
        return ph.token.text, {}

    value = args[name]

    # IS NULL / IS NOT NULL
    if ph.context in {'is_null', 'is_not_null'} and value is None:
        return 'NULL', {}

    # IN clause
    if ph.context == 'in_clause':
        return _expand_named_in(name, value, ph.in_parens, args)

    return ph.token.text, {name: value}


def _expand_in(
    value: Any,
    dialect: str,
    in_parens: bool
) -> tuple[str, list, bool]:
    """Expand IN clause value. Returns (sql, args, skip_close_paren)."""
    placeholder = _dialect_ph(dialect)

    if _is_sequence_not_str(value):
        # Unwrap single nested sequence
        if len(value) == 1 and _is_sequence_not_str(value[0]):
            value = value[0]

        # Empty sequence → NULL
        if not value:
            if in_parens:
                return 'NULL', [], False
            return '(NULL)', [], False

        # Expand to multiple placeholders
        placeholders = ', '.join([placeholder] * len(value))
        if in_parens:
            return placeholders, list(value), True  # Skip the existing )
        return f'({placeholders})', list(value), False

    # Single value
    if in_parens:
        return placeholder, [value], True
    return f'({placeholder})', [value], False


def _expand_named_in(
    name: str,
    value: Any,
    in_parens: bool,
    existing_args: dict
) -> tuple[str, dict]:
    """Expand named IN clause value."""
    if _is_sequence_not_str(value):
        # Unwrap single nested sequence
        if len(value) == 1 and _is_sequence_not_str(value[0]):
            value = value[0]

        # Empty sequence → NULL
        if not value:
            if in_parens:
                return 'NULL', {}
            return '(NULL)', {}

        # Generate numbered placeholders
        new_args = {}
        placeholders = []

        # Find next available index
        base_idx = 0
        while f'{name}_{base_idx}' in existing_args:
            base_idx += 1

        for i, v in enumerate(value):
            key = f'{name}_{base_idx + i}'
            placeholders.append(f'%({key})s')
            new_args[key] = v

        sql = ', '.join(placeholders)
        if in_parens:
            return sql, new_args
        return f'({sql})', new_args

    # Single value
    key = f'{name}_0'
    if in_parens:
        return f'%({key})s', {key: value}
    return f'(%({key})s)', {key: value}


def _dialect_ph(dialect: str) -> str:
    """Get placeholder for dialect."""
    return '?' if dialect == 'sqlite' else '%s'


def _escape_percent(text: str) -> str:
    """Escape unescaped percent signs in a string literal."""
    return _RE_UNESCAPED_PCT.sub('%%', text)
