"""
SQL parameter processing with single-pass architecture.

This module provides SQL parameter handling through a single-pass tokenization
and transformation pipeline:

    SQL + Args → Tokenize → Analyze Context → Normalize Args → Build Output
                  (once)      (one pass)        (unified)      (single pass)

Main entry points:
- `prepare_query(cn, sql, args)` - Full processing with connection dialect
- `process_sql_params(sql, args, dialect)` - Core processing without connection

Individual functions exported for backwards compatibility:
- `standardize_placeholders()` - Convert %s ↔ ? for dialect
- `handle_in_clause_params()` - Expand IN clause parameters
- `handle_null_is_operators()` - Convert IS %s with None to IS NULL
- `escape_percent_signs_in_literals()` - Escape % in string literals
- `has_placeholders()` - Check if SQL has placeholders
- `quote_identifier()` - Quote table/column names
"""
import re
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any

from libb import isiterable, issequence

# =============================================================================
# Data Structures
# =============================================================================


class TokenType(Enum):
    """Token types identified during SQL parsing."""
    SQL_TEXT = auto()
    STRING_LITERAL = auto()
    POSITIONAL_PH = auto()      # %s or ?
    NAMED_PH = auto()           # %(name)s
    IN_KEYWORD = auto()
    IS_KEYWORD = auto()
    IS_NOT_KEYWORD = auto()
    REGEXP_FUNC = auto()
    OPEN_PAREN = auto()
    CLOSE_PAREN = auto()


@dataclass(slots=True)
class Token:
    """Token from SQL parsing."""
    type: TokenType
    text: str
    start: int
    end: int


@dataclass(slots=True)
class PlaceholderInfo:
    """Information about a placeholder and its context."""
    token: Token
    index: int                      # Position in args (-1 for named)
    name: str | None = None         # For %(name)s
    context: str = 'value'          # 'value', 'in_clause', 'is_null', 'is_not_null'
    in_parentheses: bool = False    # Already in parens: IN (%s)


# =============================================================================
# Consolidated Regex Patterns (4 patterns)
# =============================================================================

# Master tokenization pattern - captures all token types in one scan
# Note: regexp pattern handles string literals inside function to avoid matching
# parentheses within regex patterns like '(CN|US)'
_TOKENIZE = re.compile(r"""
    (?P<string>'(?:[^']|'')*'|"(?:[^"]|"")*")
    |(?P<regexp>regexp_replace\s*\((?:[^()'"]|'(?:[^']|'')*'|"(?:[^"]|"")*"|\((?:[^()'"]|'(?:[^']|'')*'|"(?:[^"]|"")*")*\))*\))
    |(?P<named>%\((?P<pname>[^)]+)\)s)
    |(?P<percent_s>%s)
    |(?P<qmark>\?)
    |(?P<is_not>\bIS\s+NOT\b)
    |(?P<is_kw>\bIS\b)
    |(?P<in_kw>\bIN\b)
    |(?P<open_paren>\()
    |(?P<close_paren>\))
""", re.IGNORECASE | re.VERBOSE)

# Extract parameter name from %(name)s
_PARAM_NAME = re.compile(r'%\(([^)]+)\)s')

# Find unescaped percent signs in string content
_UNESCAPED_PERCENT = re.compile(r'(?<!%)%(?![%s(])')

# Quick placeholder check
_HAS_PLACEHOLDER = re.compile(r'%s|\?|%\([^)]+\)s')


# =============================================================================
# Core Functions
# =============================================================================

def tokenize_sql(sql: str) -> list[Token]:
    """Parse SQL into tokens in a single pass.

    Parameters
        sql: SQL query string

    Returns
        List of tokens preserving all SQL text
    """
    tokens = []
    last_end = 0

    for match in _TOKENIZE.finditer(sql):
        start, end = match.span()

        # Capture SQL text between tokens
        if start > last_end:
            tokens.append(Token(
                type=TokenType.SQL_TEXT,
                text=sql[last_end:start],
                start=last_end,
                end=start
            ))

        # Determine token type from matched group
        if match.group('string'):
            ttype = TokenType.STRING_LITERAL
        elif match.group('regexp'):
            ttype = TokenType.REGEXP_FUNC
        elif match.group('named'):
            ttype = TokenType.NAMED_PH
        elif match.group('percent_s'):
            ttype = TokenType.POSITIONAL_PH
        elif match.group('qmark'):
            ttype = TokenType.POSITIONAL_PH
        elif match.group('is_not'):
            ttype = TokenType.IS_NOT_KEYWORD
        elif match.group('is_kw'):
            ttype = TokenType.IS_KEYWORD
        elif match.group('in_kw'):
            ttype = TokenType.IN_KEYWORD
        elif match.group('open_paren'):
            ttype = TokenType.OPEN_PAREN
        elif match.group('close_paren'):
            ttype = TokenType.CLOSE_PAREN
        else:
            continue

        tokens.append(Token(type=ttype, text=match.group(0), start=start, end=end))
        last_end = end

    # Capture trailing SQL text
    if last_end < len(sql):
        tokens.append(Token(
            type=TokenType.SQL_TEXT,
            text=sql[last_end:],
            start=last_end,
            end=len(sql)
        ))

    return tokens


def analyze_placeholders(tokens: list[Token]) -> list[PlaceholderInfo]:
    """Analyze placeholder context in single forward pass.

    Determines whether each placeholder is:
    - Regular value placeholder
    - IN clause placeholder (needs expansion)
    - IS NULL placeholder (value may be None)

    Parameters
        tokens: List of tokens from tokenize_sql()

    Returns
        List of PlaceholderInfo for each placeholder found
    """
    placeholders = []
    positional_index = 0

    prev_keyword: TokenType | None = None
    in_paren_after_in = False
    i = 0

    while i < len(tokens):
        token = tokens[i]

        if token.type == TokenType.IN_KEYWORD:
            prev_keyword = TokenType.IN_KEYWORD
            # Check if next significant token is open paren
            j = i + 1
            while j < len(tokens) and tokens[j].type == TokenType.SQL_TEXT and not tokens[j].text.strip():
                j += 1
            if j < len(tokens) and tokens[j].type == TokenType.OPEN_PAREN:
                in_paren_after_in = True

        elif token.type in {TokenType.IS_KEYWORD, TokenType.IS_NOT_KEYWORD}:
            prev_keyword = token.type

        elif token.type == TokenType.OPEN_PAREN:
            pass  # Already handled in IN_KEYWORD check

        elif token.type == TokenType.CLOSE_PAREN:
            in_paren_after_in = False

        elif token.type == TokenType.POSITIONAL_PH:
            context = _determine_context(prev_keyword)
            placeholders.append(PlaceholderInfo(
                token=token,
                index=positional_index,
                name=None,
                context=context,
                in_parentheses=in_paren_after_in
            ))
            positional_index += 1
            prev_keyword = None
            in_paren_after_in = False

        elif token.type == TokenType.NAMED_PH:
            param_name = _PARAM_NAME.search(token.text).group(1)
            context = _determine_context(prev_keyword)
            placeholders.append(PlaceholderInfo(
                token=token,
                index=-1,
                name=param_name,
                context=context,
                in_parentheses=in_paren_after_in
            ))
            prev_keyword = None
            in_paren_after_in = False

        elif token.type == TokenType.SQL_TEXT:
            # Reset keyword state on significant SQL text
            if token.text.strip() and prev_keyword not in {TokenType.IN_KEYWORD, TokenType.IS_KEYWORD, TokenType.IS_NOT_KEYWORD}:
                prev_keyword = None

        i += 1

    return placeholders


def _determine_context(prev_keyword: TokenType | None) -> str:
    """Determine placeholder context from preceding keyword."""
    if prev_keyword == TokenType.IN_KEYWORD:
        return 'in_clause'
    if prev_keyword == TokenType.IS_KEYWORD:
        return 'is_null'
    if prev_keyword == TokenType.IS_NOT_KEYWORD:
        return 'is_not_null'
    return 'value'


def normalize_args(
    sql: str,
    args: tuple | list | dict | Any,
    placeholders: list[PlaceholderInfo]
) -> tuple | dict:
    """Normalize all input formats to canonical form.

    Handles:
    - Direct list: [1, 2, 3] for single IN clause
    - Nested tuple: [(1, 2, 3)]
    - Double-nested: [[(1, 2, 3)]]
    - Single item: [101]
    - Named dict: {'ids': [1,2,3]}
    - Mixed: (date1, date2, (1,2,3))

    Parameters
        sql: Original SQL string
        args: Original arguments
        placeholders: List of placeholder info

    Returns
        Normalized args matching placeholder count
    """
    if args is None or not args:
        return args

    # Dict args - return as-is
    if isinstance(args, dict):
        return args

    # Unwrap single-element containing dict
    if isinstance(args, (list, tuple)) and len(args) == 1 and isinstance(args[0], dict):
        return args[0]

    in_clause_count = sum(1 for p in placeholders if p.context == 'in_clause')
    total_placeholders = len(placeholders)

    # Case 1: Unwrap nested parameters [(a, b, c)] -> (a, b, c)
    if (isinstance(args, (list, tuple)) and len(args) == 1
            and issequence(args[0]) and not isinstance(args[0], str)):
        inner = args[0]
        if len(inner) == total_placeholders:
            return tuple(inner)

    # Case 2: Direct list [1, 2, 3] for single IN clause
    if (in_clause_count == 1 and total_placeholders == 1
            and isiterable(args) and not isinstance(args, str)):
        if all(not isiterable(a) or isinstance(a, str) for a in args):
            return (tuple(args),)

    # Case 3: Single item [101] -> wrap for IN clause
    if (isinstance(args, list) and len(args) == 1
            and in_clause_count == 1 and total_placeholders == 1):
        item = args[0]
        if not isiterable(item) or isinstance(item, str):
            return ((item,),)

    # Case 4: Double-nested [[(1, 2, 3)]] -> flatten once
    if (isinstance(args, (list, tuple)) and len(args) == 1
        and issequence(args[0]) and len(args[0]) == 1
            and issequence(args[0][0])):
        return (tuple(args[0][0]),)

    # Case 5: Multiple IN clauses with direct lists
    if isinstance(args, list) and in_clause_count > 1:
        normalized = []
        for i, arg in enumerate(args):
            ph_info = placeholders[i] if i < len(placeholders) else None
            if ph_info and ph_info.context == 'in_clause' and isinstance(arg, list):
                normalized.append(tuple(arg))
            else:
                normalized.append(arg)
        return tuple(normalized)

    return tuple(args) if isinstance(args, list) else args


def build_sql(
    tokens: list[Token],
    placeholders: list[PlaceholderInfo],
    args: tuple | dict,
    dialect: str
) -> tuple[str, tuple | dict]:
    """Build final SQL and args in single forward pass.

    Parameters
        tokens: Tokenized SQL
        placeholders: Placeholder info list
        args: Normalized arguments
        dialect: Database dialect ('postgresql' or 'sqlite')

    Returns
        Tuple of (processed_sql, processed_args)
    """
    result_parts = []
    is_dict_args = isinstance(args, dict)
    result_args = {} if is_dict_args else []
    placeholder_idx = 0
    args_idx = 0

    for token in tokens:
        if token.type == TokenType.STRING_LITERAL:
            # Escape percent signs in literals for PostgreSQL
            if dialect == 'postgresql':
                result_parts.append(_escape_percent_in_literal(token.text))
            else:
                result_parts.append(token.text)

        elif token.type == TokenType.REGEXP_FUNC:
            # Preserve regexp_replace unchanged
            result_parts.append(token.text)

        elif token.type == TokenType.POSITIONAL_PH:
            if placeholder_idx < len(placeholders):
                ph = placeholders[placeholder_idx]
                sql_part, new_args = _process_positional_placeholder(
                    ph, args, args_idx, dialect
                )
                result_parts.append(sql_part)
                result_args.extend(new_args)
                args_idx += 1
                placeholder_idx += 1
            else:
                result_parts.append(_dialect_placeholder(dialect))

        elif token.type == TokenType.NAMED_PH:
            if not is_dict_args:
                # Named placeholders unchanged when args is not a dict
                result_parts.append(token.text)
                placeholder_idx += 1
            elif placeholder_idx < len(placeholders):
                ph = placeholders[placeholder_idx]
                sql_part, arg_updates = _process_named_placeholder(ph, args, dialect)
                result_parts.append(sql_part)
                result_args.update(arg_updates)
                placeholder_idx += 1
            else:
                result_parts.append(token.text)

        elif token.type in {TokenType.IS_KEYWORD, TokenType.IS_NOT_KEYWORD}:
            # Check if next placeholder has None value
            next_ph_idx = placeholder_idx
            if next_ph_idx < len(placeholders):
                ph = placeholders[next_ph_idx]
                value = _get_placeholder_value(ph, args, args_idx)
                if value is None and ph.context in {'is_null', 'is_not_null'}:
                    result_parts.append(token.text)
                else:
                    result_parts.append(token.text)
            else:
                result_parts.append(token.text)

        elif token.type == TokenType.IN_KEYWORD:
            # Just append the IN keyword as-is
            result_parts.append(token.text)

        elif token.type == TokenType.OPEN_PAREN:
            # Always keep open parens - expansion handles not duplicating
            result_parts.append(token.text)

        elif token.type == TokenType.CLOSE_PAREN:
            # Skip close paren if placeholder added it
            result_parts.append(token.text)

        else:
            result_parts.append(token.text)

    final_sql = ''.join(result_parts)
    final_args = result_args if is_dict_args else tuple(result_args)

    return final_sql, final_args


def _get_placeholder_value(ph: PlaceholderInfo, args: tuple | dict, args_idx: int) -> Any:
    """Get value for a placeholder from args."""
    if isinstance(args, dict):
        return args.get(ph.name)
    if args_idx < len(args):
        return args[args_idx]
    return None


def _process_positional_placeholder(
    ph: PlaceholderInfo,
    args: tuple,
    args_idx: int,
    dialect: str
) -> tuple[str, list]:
    """Process a positional placeholder."""
    placeholder = _dialect_placeholder(dialect)

    if args_idx >= len(args):
        return placeholder, []

    value = args[args_idx]

    # Handle IS NULL / IS NOT NULL
    if ph.context in {'is_null', 'is_not_null'} and value is None:
        return 'NULL', []

    # Handle IN clause
    if ph.context == 'in_clause':
        return _expand_in_clause(value, dialect, ph.in_parentheses)

    return placeholder, [value]


def _expand_in_clause(
    value: Any,
    dialect: str,
    already_in_parens: bool
) -> tuple[str, list]:
    """Expand IN clause value to multiple placeholders."""
    placeholder = _dialect_placeholder(dialect)

    if issequence(value) and not isinstance(value, str):
        # Unwrap single-nested sequences
        if len(value) == 1 and issequence(value[0]) and not isinstance(value[0], str):
            value = value[0]

        # Empty sequence -> NULL
        if not value:
            return 'NULL' if already_in_parens else '(NULL)', []

        # Expand to multiple placeholders
        placeholders = ', '.join([placeholder] * len(value))
        if already_in_parens:
            return placeholders, list(value)
        return f'({placeholders})', list(value)

    # Single value
    if already_in_parens:
        return placeholder, [value]
    return f'({placeholder})', [value]


def _process_named_placeholder(
    ph: PlaceholderInfo,
    args: dict,
    dialect: str
) -> tuple[str, dict]:
    """Process a named placeholder."""
    name = ph.name

    if name not in args:
        return ph.token.text, {}

    value = args[name]

    # Handle IS NULL / IS NOT NULL
    if ph.context in {'is_null', 'is_not_null'} and value is None:
        return 'NULL', {}

    # Handle IN clause
    if ph.context == 'in_clause':
        return _expand_named_in_clause(name, value, ph.in_parentheses)

    return ph.token.text, {name: value}


def _expand_named_in_clause(
    name: str,
    value: Any,
    already_in_parens: bool
) -> tuple[str, dict]:
    """Expand named IN clause to multiple named placeholders."""
    if not issequence(value) or isinstance(value, str):
        return f'%({name})s', {name: value}

    # Unwrap nested
    if len(value) == 1 and issequence(value[0]) and not isinstance(value[0], str):
        value = list(value[0])

    # Empty -> NULL
    if not value:
        return 'NULL' if already_in_parens else '(NULL)', {}

    # Generate indexed placeholders
    placeholders = []
    new_args = {}
    for i, v in enumerate(value):
        key = f'{name}_{i}'
        placeholders.append(f'%({key})s')
        new_args[key] = v

    result = ', '.join(placeholders)
    if already_in_parens:
        return result, new_args
    return f'({result})', new_args


def _dialect_placeholder(dialect: str) -> str:
    """Return placeholder for dialect."""
    return '?' if dialect == 'sqlite' else '%s'


def _escape_percent_in_literal(literal: str) -> str:
    """Escape unescaped percent signs in string literal."""
    quote = literal[0]
    content = literal[1:-1]
    escaped = _UNESCAPED_PERCENT.sub('%%', content)
    return f'{quote}{escaped}{quote}'


# =============================================================================
# Main Entry Points
# =============================================================================

def process_sql_params(
    sql: str,
    args: tuple | list | dict | Any,
    dialect: str = 'postgresql'
) -> tuple[str, tuple | dict]:
    """Process SQL and parameters in a single pass.

    Parameters
        sql: SQL query string with placeholders
        args: Query parameters (tuple, list, dict, or scalar)
        dialect: Database dialect ('postgresql' or 'sqlite')

    Returns
        Tuple of (processed_sql, processed_args)
    """
    if not sql:
        return sql, args

    if not args:
        return sql, args

    if '%' not in sql and '?' not in sql:
        return sql, ()

    if not _HAS_PLACEHOLDER.search(sql):
        return sql, ()

    tokens = tokenize_sql(sql)
    placeholders = analyze_placeholders(tokens)

    if not placeholders:
        return sql, ()

    normalized_args = normalize_args(sql, args, placeholders)
    return build_sql(tokens, placeholders, normalized_args, dialect)


def prepare_query(cn: Any, sql: str, args: tuple | list | dict | Any) -> tuple[str, Any]:
    """Process SQL and parameters using connection's dialect.

    Parameters
        cn: Database connection
        sql: SQL query string
        args: Query parameters

    Returns
        Tuple of (processed_sql, processed_args)
    """
    if not sql or not args:
        return sql, args

    if not has_placeholders(sql):
        return sql, ()

    from database.utils.connection_utils import get_dialect_name
    dialect = get_dialect_name(cn)

    sql, args = process_sql_params(sql, args, dialect)

    # Type conversion for special types
    from database.types import TypeConverter
    args = TypeConverter.convert_params(args)

    return sql, args


# =============================================================================
# Backwards Compatible Public API
# =============================================================================

def process_query_parameters(cn: Any, sql: str, args: Any) -> tuple[str, Any]:
    """Process SQL query parameters (backwards compatible).

    This function wraps process_sql_params for backwards compatibility.
    """
    return prepare_query(cn, sql, args)


def has_placeholders(sql: str | None) -> bool:
    """Check if SQL has any parameter placeholders.

    Parameters
        sql: SQL query string

    Returns
        True if SQL contains placeholders
    """
    if not sql:
        return False

    if '%' not in sql and '?' not in sql:
        return False

    return bool(_HAS_PLACEHOLDER.search(sql))


def standardize_placeholders(sql: str, dialect: str = 'postgresql') -> str:
    """Convert placeholders between %s and ? based on dialect.

    Parameters
        sql: SQL query string
        dialect: Database dialect

    Returns
        SQL with standardized placeholders
    """
    if not sql:
        return sql

    if dialect == 'sqlite':
        if '%s' not in sql:
            return sql
        # Protect string literals
        tokens = tokenize_sql(sql)
        result = []
        for token in tokens:
            if token.type == TokenType.STRING_LITERAL:
                result.append(token.text)
            elif token.type == TokenType.POSITIONAL_PH and token.text == '%s':
                result.append('?')
            else:
                result.append(token.text)
        return ''.join(result)

    elif dialect == 'postgresql':
        if '?' not in sql:
            return sql
        tokens = tokenize_sql(sql)
        result = []
        for token in tokens:
            if token.type == TokenType.REGEXP_FUNC:
                result.append(token.text)
            elif token.type == TokenType.POSITIONAL_PH and token.text == '?':
                result.append('%s')
            else:
                result.append(token.text)
        return ''.join(result)

    return sql


def handle_in_clause_params(
    sql: str,
    args: Any,
    dialect: str = 'postgresql'
) -> tuple[str, Any]:
    """Expand list/tuple parameters for IN clauses.

    Parameters
        sql: SQL query string
        args: Query parameters
        dialect: Database dialect

    Returns
        Tuple of (processed_sql, processed_args)
    """
    if not args or not sql or ' in ' not in sql.lower():
        return sql, args

    # Use the full pipeline
    return process_sql_params(sql, args, dialect)


def handle_null_is_operators(sql: str, args: Any) -> tuple[str, Any]:
    """Handle NULL values with IS and IS NOT operators.

    Parameters
        sql: SQL query string
        args: Query parameters

    Returns
        Tuple of (processed_sql, processed_args)
    """
    if not args or not sql:
        return sql, args

    # Use full pipeline but filter for IS NULL handling only
    tokens = tokenize_sql(sql)
    placeholders = analyze_placeholders(tokens)

    if not placeholders:
        return sql, args

    # Check if any IS NULL placeholders exist
    has_is_null = any(p.context in {'is_null', 'is_not_null'} for p in placeholders)
    if not has_is_null:
        return sql, args

    # Process only IS NULL transformations
    if isinstance(args, dict):
        return _handle_null_named(sql, args, placeholders)
    else:
        return _handle_null_positional(sql, args, placeholders)


def _handle_null_positional(
    sql: str,
    args: list | tuple,
    placeholders: list[PlaceholderInfo]
) -> tuple[str, list | tuple]:
    """Handle IS NULL for positional parameters."""
    args_was_tuple = isinstance(args, tuple)
    args = list(args)
    result_sql = sql

    # Process in reverse to maintain positions
    for ph in reversed(placeholders):
        if ph.context in {'is_null', 'is_not_null'} and ph.index < len(args):
            if args[ph.index] is None:
                # Replace placeholder with NULL
                result_sql = result_sql[:ph.token.start] + 'NULL' + result_sql[ph.token.end:]
                args.pop(ph.index)

    return result_sql, tuple(args) if args_was_tuple else args


def _handle_null_named(
    sql: str,
    args: dict,
    placeholders: list[PlaceholderInfo]
) -> tuple[str, dict]:
    """Handle IS NULL for named parameters."""
    args = args.copy()
    result_sql = sql

    for ph in reversed(placeholders):
        if ph.context in {'is_null', 'is_not_null'} and ph.name in args:
            if args[ph.name] is None:
                result_sql = result_sql[:ph.token.start] + 'NULL' + result_sql[ph.token.end:]
                args.pop(ph.name)

    return result_sql, args


def escape_percent_signs_in_literals(sql: str) -> str:
    """Escape percent signs in string literals.

    Parameters
        sql: SQL query string

    Returns
        SQL with escaped percent signs in literals
    """
    if not sql or '%' not in sql:
        return sql

    # Skip if SELECT with literal % but no placeholders
    if (sql.strip().upper().startswith('SELECT')
            and "'%" in sql and '%s' not in sql and '?' not in sql):
        return sql

    tokens = tokenize_sql(sql)
    result = []
    for token in tokens:
        if token.type == TokenType.STRING_LITERAL:
            result.append(_escape_percent_in_literal(token.text))
        else:
            result.append(token.text)
    return ''.join(result)


def quote_identifier(identifier: str, dialect: str = 'postgresql') -> str:
    """Safely quote database identifiers.

    Parameters
        identifier: Table or column name
        dialect: Database dialect

    Returns
        Quoted identifier

    Raises
        ValueError: If dialect is unsupported
    """
    if dialect in {'postgresql', 'sqlite'}:
        return '"' + identifier.replace('"', '""') + '"'

    raise ValueError(f'Unknown dialect: {dialect}')


# =============================================================================
# Helper Functions for Tests
# =============================================================================

def _unwrap_nested_parameters(sql: str, args: Any) -> Any:
    """Unwrap nested parameters (for backwards compatibility with tests)."""
    if args is None:
        return None

    if not isinstance(args, (list, tuple)) or not args:
        return args

    if len(args) != 1:
        return args

    first = args[0]
    if not issequence(first) or isinstance(first, str):
        return args

    placeholder_count = sql.count('%s') + sql.count('?')
    if len(first) == placeholder_count:
        return first

    return args


def _generate_named_placeholders(base_name: str, values: Any, args_dict: dict) -> list:
    """Generate named placeholders for IN clause expansion (for tests)."""
    placeholders = []
    next_idx = 0

    for key in args_dict:
        if key.startswith(f'{base_name}_') and key[len(base_name)+1:].isdigit():
            idx = int(key[len(base_name)+1:])
            next_idx = max(next_idx, idx + 1)

    for i, val in enumerate(values):
        key = f'{base_name}_{next_idx + i}'
        placeholders.append(f'%({key})s')
        args_dict[key] = val

    return placeholders


def chunk_sql_parameters(sql: str, args: list | tuple, param_limit: int) -> list:
    """Split parameters into chunks for batch operations."""
    if not args or len(args) <= param_limit:
        return [args]
    return [args[i:i + param_limit] for i in range(0, len(args), param_limit)]


def prepare_sql_params_for_execution(
    sql: str,
    args: Any,
    dialect: str = 'postgresql'
) -> tuple[str, Any]:
    """Prepare SQL and parameters for execution (backwards compatible)."""
    result_sql, result_args = process_sql_params(sql, args, dialect)

    from database.types import TypeConverter
    result_args = TypeConverter.convert_params(result_args)

    return result_sql, result_args


def handle_query_params(func: Any) -> Any:
    """Decorator to handle query parameters (backwards compatible)."""
    from functools import wraps

    @wraps(func)
    def wrapper(cn, sql, *args, **kwargs):
        sql = escape_percent_signs_in_literals(sql)

        if not args:
            return func(cn, sql, *args, **kwargs)

        if not has_placeholders(sql):
            return func(cn, sql, *(), **kwargs)

        processed_sql, processed_args = process_query_parameters(cn, sql, args)
        return func(cn, processed_sql, *processed_args, **kwargs)

    return wrapper
