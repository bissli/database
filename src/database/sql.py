"""
SQL formatting and parameter handling utilities.

This module provides a single entry point for all SQL parameter processing:
- `prepare_query(cn, sql, args)` - Main function, handles everything

Individual functions are also exported for backwards compatibility and testing:
- `standardize_placeholders()` - Convert %s to ? for SQLite
- `handle_in_clause_params()` - Expand IN clause parameters
- `handle_null_is_operators()` - Convert IS %s with None to IS NULL
- `escape_percent_signs_in_literals()` - Escape % in string literals
- `has_placeholders()` - Check if SQL has placeholders
- `quote_identifier()` - Quote table/column names
"""
import logging
import re
from functools import wraps
from typing import Any

from more_itertools import collapse

from libb import isiterable, issequence

logger = logging.getLogger(__name__)

# Pre-compiled regex patterns
_PATTERNS = {
    'positional': re.compile(r'%s|\?'),
    'named': re.compile(r'%\([^)]+\)s'),
    'in_clause': re.compile(r'\bIN\s+(%s|\(%s\)|\?|\(\?\))', re.IGNORECASE),
    'in_clause_named': re.compile(r'(?i)\b(in)\s+(?:\()?(%\([^)]+\)s)(?:\))?'),
    'null_is_positional': re.compile(r'\b(IS\s+NOT|IS)\s+(%s|\?)', re.IGNORECASE),
    'null_is_named': re.compile(r'\b(IS\s+NOT|IS)\s+(%\([^)]+\)s)\b', re.IGNORECASE),
    'string_literal': re.compile(r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\""),
    'param_name': re.compile(r'%\(([^)]+)\)s'),
    'regexp_replace': re.compile(r'(regexp_replace\s*\(\s*[^,]+\s*,\s*[\'"])(.*?)([\'"].*?\))', re.IGNORECASE),
}


def prepare_query(cn: Any, sql: str, args: tuple | list | dict | Any) -> tuple[str, Any]:
    """Single entry point for all SQL/parameter processing.

    This function handles:
    1. Empty/placeholder-less queries (early exit)
    2. Parameter unwrapping for nested tuples
    3. IN clause expansion
    4. IS NULL operator handling
    5. Placeholder standardization (SQLite: %s -> ?)
    6. Type conversion (NumPy, Pandas, PyArrow)

    Parameters
        cn: Database connection (used to detect dialect)
        sql: SQL query string
        args: Parameters (tuple, list, dict, or scalar)

    Returns
        Tuple of (processed_sql, processed_args)
    """
    if not sql or not args:
        return sql, args

    if not has_placeholders(sql):
        return sql, ()

    from database.utils.connection_utils import get_dialect_name
    dialect = get_dialect_name(cn)

    # Step 1: Unwrap nested parameters
    args = _unwrap_args(sql, args)

    # Step 2: Escape percent signs in string literals (PostgreSQL)
    sql = escape_percent_signs_in_literals(sql)

    # Step 3: Handle IS NULL operators
    sql, args = handle_null_is_operators(sql, args)

    # Step 4: Handle IN clause expansion
    sql, args = handle_in_clause_params(sql, args, dialect=dialect)

    # Step 5: Standardize placeholders for SQLite
    sql = standardize_placeholders(sql, dialect=dialect)

    # Step 6: Type conversion
    from database.types import TypeConverter
    args = TypeConverter.convert_params(args)

    return sql, args


def has_placeholders(sql: str | None) -> bool:
    """Check if SQL has any parameter placeholders (%s, ?, %(name)s).

    Parameters
        sql: SQL query string

    Returns
        True if SQL contains placeholders
    """
    if not sql:
        return False

    # Quick check before regex
    if '%' not in sql and '?' not in sql:
        return False

    return bool(_PATTERNS['positional'].search(sql) or _PATTERNS['named'].search(sql))


def standardize_placeholders(sql: str, dialect: str = 'postgresql') -> str:
    """Standardize SQL placeholders based on database dialect.

    PostgreSQL uses %s, SQLite uses ?

    Parameters
        sql: SQL query string
        dialect: Database dialect ('postgresql' or 'sqlite')

    Returns
        SQL with standardized placeholders
    """
    if not sql:
        return sql

    if dialect == 'postgresql':
        # Convert ? to %s, but preserve regex patterns in regexp_replace
        if '?' not in sql:
            return sql
        if 'regexp_replace' not in sql.lower():
            return re.sub(r'(?<!\w)\?(?!\w)', '%s', sql)
        return _preserve_regex_patterns(sql)

    elif dialect == 'sqlite':
        # Convert %s to ?, protecting string literals
        if '%s' not in sql:
            return sql
        return _convert_placeholders_sqlite(sql)

    return sql


def escape_percent_signs_in_literals(sql: str) -> str:
    """Escape percent signs in string literals to avoid placeholder conflicts.

    Parameters
        sql: SQL query string

    Returns
        SQL with escaped percent signs in literals
    """
    if not sql or '%' not in sql:
        return sql

    # Don't escape if it's a simple SELECT with no placeholders
    if (sql.strip().upper().startswith('SELECT') and
        "'%" in sql and '%s' not in sql and '?' not in sql):
        return sql

    def escape_literal(match):
        literal = match.group(0)
        quote = literal[0]
        content = literal[1:-1]
        # Replace single % with %%, but not if already %%
        escaped = re.sub(r'(?<!%)%(?!%)', '%%', content)
        return f'{quote}{escaped}{quote}'

    return _PATTERNS['string_literal'].sub(escape_literal, sql)


def handle_null_is_operators(sql: str, args: Any) -> tuple[str, Any]:
    """Handle NULL values with IS and IS NOT operators.

    Converts 'IS %s' with None parameter to 'IS NULL', removing the parameter.

    Parameters
        sql: SQL query string
        args: Query parameters

    Returns
        Tuple of (processed_sql, processed_args)
    """
    if not args or not sql:
        return sql, args

    if isinstance(args, dict):
        return _handle_null_named(sql, args)
    else:
        return _handle_null_positional(sql, args)


def handle_in_clause_params(sql: str, args: Any, dialect: str = 'postgresql') -> tuple[str, Any]:
    """Expand list/tuple parameters for IN clauses.

    Handles various formats:
    - "WHERE x IN %s" with args [(1,2,3)] -> "WHERE x IN (%s,%s,%s)" with (1,2,3)
    - "WHERE x IN %s" with args [1,2,3] -> "WHERE x IN (%s,%s,%s)" with (1,2,3)
    - "WHERE x IN %(ids)s" with {'ids': [1,2,3]} -> expanded named params
    - Empty sequences -> "WHERE x IN (NULL)"

    Parameters
        sql: SQL query string
        args: Query parameters
        dialect: Database dialect

    Returns
        Tuple of (processed_sql, processed_args)
    """
    if not args or not sql or ' in ' not in sql.lower():
        return sql, args

    # Handle tuple-wrapped dict (common when dict passed via *args)
    if isinstance(args, tuple) and len(args) == 1 and isinstance(args[0], dict):
        sql, result_dict = _handle_in_named(sql, args[0])
        return sql, (result_dict,)

    # Route based on parameter type
    if isinstance(args, dict):
        return _handle_in_named(sql, args)
    elif isinstance(args, (list, tuple)):
        return _handle_in_positional(sql, args, dialect)

    return sql, args


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


# Decorator for backwards compatibility (will be removed in Phase 5)
def handle_query_params(func: Any) -> Any:
    """Decorator to handle query parameters (DEPRECATED - use prepare_query directly)."""
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


def process_query_parameters(cn: Any, sql: str, args: Any) -> tuple[str, Any]:
    """Process SQL query parameters (backwards compatible wrapper).

    This function is kept for backwards compatibility with tests.
    New code should use prepare_query() directly.
    """
    from database.utils.connection_utils import get_dialect_name
    dialect = get_dialect_name(cn)

    if not sql or not args:
        return sql, args

    if not has_placeholders(sql):
        return sql, ()

    # Step 1: Unwrap nested parameters
    args = _unwrap_args(sql, args)

    # Step 2: Handle named IN clauses early
    if ' in ' in sql.lower():
        if isinstance(args, dict):
            sql, args = _handle_in_named(sql, args)
        elif args and len(args) == 1 and isinstance(args[0], dict):
            sql, args_dict = _handle_in_named(sql, args[0])
            args = (args_dict,)

    # Step 3: Standardize placeholders
    sql = standardize_placeholders(sql, dialect=dialect)

    # Step 4: Handle IS NULL operators
    sql, args = handle_null_is_operators(sql, args)

    # Step 5: Escape percent signs
    sql = escape_percent_signs_in_literals(sql)

    # Step 6: Handle remaining IN clause params
    sql, args = handle_in_clause_params(sql, args, dialect=dialect)

    return sql, args


# Keep these for backwards compatibility with tests
def prepare_sql_params_for_execution(sql: str, args: Any, dialect: str = 'postgresql') -> tuple[str, Any]:
    """Prepare SQL and parameters for execution (backwards compatible).

    DEPRECATED: This function is redundant. Use prepare_query() instead.
    """
    if ' in ' in sql.lower():
        if isinstance(args, dict):
            sql, args = _handle_in_named(sql, args)
        elif args and len(args) == 1 and isinstance(args[0], dict):
            sql, args_dict = _handle_in_named(sql, args[0])
            args = (args_dict,)

    sql = standardize_placeholders(sql, dialect=dialect)
    sql, args = handle_in_clause_params(sql, args, dialect=dialect)

    from database.types import TypeConverter
    args = TypeConverter.convert_params(args)

    return sql, args


def chunk_sql_parameters(sql: str, args: list | tuple, param_limit: int) -> list:
    """Split parameters into chunks for batch operations."""
    if not args or len(args) <= param_limit:
        return [args]
    return [args[i:i + param_limit] for i in range(0, len(args), param_limit)]


# Internal helper functions

def _unwrap_args(sql: str, args: Any) -> Any:
    """Unwrap nested parameters if appropriate."""
    if args is None:
        return None

    if not isinstance(args, (list, tuple)) or not args:
        return args

    if len(args) != 1:
        return args

    first = args[0]
    if not issequence(first) or isinstance(first, str):
        return args

    # Unwrap if placeholder count matches inner sequence length
    placeholder_count = sql.count('%s') + sql.count('?')
    if len(first) == placeholder_count:
        return first

    return args


def _unwrap_nested_parameters(sql: str, args: Any) -> Any:
    """Backwards compatible alias for _unwrap_args."""
    return _unwrap_args(sql, args)


def _handle_null_positional(sql: str, args: list | tuple) -> tuple[str, list | tuple]:
    """Handle NULL with IS/IS NOT for positional parameters."""
    args_was_tuple = isinstance(args, tuple)
    args = list(args)

    matches = list(_PATTERNS['null_is_positional'].finditer(sql))

    for match in reversed(matches):
        text_before = sql[:match.start(2)]
        idx = text_before.count('%s') + text_before.count('?')

        if idx < len(args) and args[idx] is None:
            operator = match.group(1).upper()
            sql = sql[:match.start(0)] + f'{operator} NULL' + sql[match.end(0):]
            args.pop(idx)

    return sql, tuple(args) if args_was_tuple else args


def _handle_null_named(sql: str, args: dict) -> tuple[str, dict]:
    """Handle NULL with IS/IS NOT for named parameters."""
    args = args.copy()

    for match in _PATTERNS['null_is_named'].finditer(sql):
        param_name = _PATTERNS['param_name'].search(match.group(2)).group(1)

        if param_name in args and args[param_name] is None:
            operator = match.group(1).upper()
            sql = sql[:match.start(0)] + f'{operator} NULL' + sql[match.end(0):]
            args.pop(param_name)

    return sql, args


def _handle_in_positional(sql: str, args: list | tuple, dialect: str) -> tuple[str, Any]:
    """Handle IN clause expansion for positional parameters."""
    # Normalize args format
    args = _normalize_in_args(sql, args)
    args_list = list(args) if isinstance(args, tuple) else list(args)

    # Find all placeholders and their types
    # Pattern for IN clause placeholders
    in_clause_pattern = _PATTERNS['in_clause']
    # Pattern for all placeholders
    all_placeholder_pattern = re.compile(r'\bIN\s+(%s|\(%s\)|\?|\(\?\))|%s|\?', re.IGNORECASE)

    # Find all matches with their positions
    all_matches = []
    for match in all_placeholder_pattern.finditer(sql):
        matched_text = match.group(0)
        is_in_clause = matched_text.upper().startswith('IN')
        all_matches.append({
            'start': match.start(),
            'end': match.end(),
            'text': matched_text,
            'is_in_clause': is_in_clause
        })

    if not all_matches:
        return sql, args

    # Check if any IN clause has a list/tuple arg that needs expansion
    has_expansion = False
    for i, match_info in enumerate(all_matches):
        if match_info['is_in_clause'] and i < len(args_list):
            param = args_list[i]
            if issequence(param) and not isinstance(param, str):
                has_expansion = True
                break

    if not has_expansion:
        return sql, args

    # Process matches in order, building new SQL and args
    result_sql = sql
    result_args = []
    offset = 0

    for i, match_info in enumerate(all_matches):
        if i >= len(args_list):
            break

        param = args_list[i]
        start = match_info['start'] + offset
        end = match_info['end'] + offset

        if match_info['is_in_clause']:
            # Handle IN clause
            if issequence(param) and not isinstance(param, str):
                # Unwrap single-item nested sequences
                if len(param) == 1 and issequence(param[0]) and not isinstance(param[0], str):
                    param = param[0]

                if not param:
                    # Empty sequence -> IN (NULL)
                    replacement = 'IN (NULL)'
                    result_sql = result_sql[:start] + replacement + result_sql[end:]
                    offset += len(replacement) - (end - start)
                else:
                    # Expand placeholders
                    placeholders = ', '.join(['%s'] * len(param))
                    replacement = f'IN ({placeholders})'
                    result_sql = result_sql[:start] + replacement + result_sql[end:]
                    offset += len(replacement) - (end - start)
                    result_args.extend(param)
            else:
                # Single value for IN clause
                replacement = 'IN (%s)'
                result_sql = result_sql[:start] + replacement + result_sql[end:]
                offset += len(replacement) - (end - start)
                result_args.append(param)
        else:
            # Regular placeholder - keep as-is
            result_args.append(param)

    # Add remaining args
    result_args.extend(args_list[len(all_matches):])

    return result_sql, tuple(result_args)


def _normalize_in_args(sql: str, args: list | tuple) -> list | tuple:
    """Normalize different argument formats for IN clause handling."""
    if not args:
        return args

    # Check if this is a direct list for single IN clause
    in_count = sql.lower().count(' in ')
    placeholder_count = sql.count('%s') + sql.count('?')

    # Only wrap if IN clause directly contains a placeholder (not subquery like "IN (SELECT...)")
    # Check for "IN %s" or "IN ?" pattern
    has_in_placeholder = bool(re.search(r'\bIN\s+(%s|\?)\b', sql, re.IGNORECASE))

    # Case: Direct list like [1, 2, 3] for single IN clause
    if (in_count == 1 and placeholder_count == 1 and has_in_placeholder and
        isiterable(args) and not isinstance(args, str)):
        if all(not isiterable(a) or isinstance(a, str) for a in args):
            return (args,)  # Wrap in tuple

    # Case: Single item list [101]
    if (isinstance(args, list) and len(args) == 1 and
        (not isiterable(args[0]) or isinstance(args[0], str))):
        return ([args[0]],)

    return args


def _handle_in_named(sql: str, args: dict) -> tuple[str, dict]:
    """Handle IN clause expansion for named parameters."""
    args = args.copy()
    matches = list(_PATTERNS['in_clause_named'].finditer(sql))

    for match in reversed(matches):
        param_placeholder = match.group(2)
        param_name = _PATTERNS['param_name'].search(param_placeholder).group(1)

        if param_name not in args:
            continue

        param = args[param_name]
        if not issequence(param) or isinstance(param, str):
            continue

        if not param:
            # Empty sequence
            in_kw = match.group(1)
            sql = sql[:match.start(0)] + f'{in_kw} (NULL)' + sql[match.end(0):]
            args.pop(param_name)
        else:
            # Flatten if needed
            values = param
            if len(param) == 1 and issequence(param[0]) and not isinstance(param[0], str):
                values = list(collapse([param[0]]))

            # Generate placeholders
            placeholders = []
            for i, val in enumerate(values):
                key = f'{param_name}_{i}'
                placeholders.append(f'%({key})s')
                args[key] = val

            in_kw = match.group(1)
            sql = sql[:match.start(0)] + f'{in_kw} ({", ".join(placeholders)})' + sql[match.end(0):]
            args.pop(param_name)

    return sql, args


def _preserve_regex_patterns(sql: str) -> str:
    """Convert ? to %s while preserving regexp_replace patterns."""
    segments = []
    last_end = 0

    for match in _PATTERNS['regexp_replace'].finditer(sql):
        prefix = sql[last_end:match.start()]
        if prefix:
            segments.append(re.sub(r'(?<!\w)\?(?!\w)', '%s', prefix))
        segments.append(match.group(0))
        last_end = match.end()

    if last_end < len(sql):
        segments.append(re.sub(r'(?<!\w)\?(?!\w)', '%s', sql[last_end:]))

    return ''.join(segments)


def _convert_placeholders_sqlite(sql: str) -> str:
    """Convert %s to ? for SQLite, protecting string literals."""
    literals = []

    def save_literal(match):
        literals.append(match.group(0))
        return f'__LITERAL_{len(literals)-1}__'

    protected = _PATTERNS['string_literal'].sub(save_literal, sql)
    converted = protected.replace('%s', '?')

    for i, literal in enumerate(literals):
        converted = converted.replace(f'__LITERAL_{i}__', literal)

    return converted


# Export helper for tests
def _generate_named_placeholders(base_name: str, values: Any, args_dict: dict) -> list:
    """Generate named placeholders for IN clause expansion."""
    placeholders = []
    next_idx = 0

    # Find next available index
    for key in args_dict:
        if key.startswith(f'{base_name}_') and key[len(base_name)+1:].isdigit():
            idx = int(key[len(base_name)+1:])
            next_idx = max(next_idx, idx + 1)

    for i, val in enumerate(values):
        key = f'{base_name}_{next_idx + i}'
        placeholders.append(f'%({key})s')
        args_dict[key] = val

    return placeholders
