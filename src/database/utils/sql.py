"""
SQL formatting and manipulation utilities.
"""
import logging
import re
from collections.abc import Sequence
from functools import wraps

logger = logging.getLogger(__name__)


def standardize_placeholders(db_type, sql):
    """Standardize SQL placeholders between ? and %s based on database type"""
    if db_type in {'postgres', 'sqlserver'}:
        # Replace ? placeholders with %s for postgres and sqlserver
        sql = re.sub(r'(\s\?\s|\(\?\)|\s\?$|\s\?\)|\s\?,|\(\?,)',
                     lambda m: m.group().replace('?', '%s'),
                     ' ' + sql + ' ')
        sql = sql.strip()
    elif db_type == 'sqlite':
        # Replace %s placeholders with ? for SQLite
        sql = re.sub(r'(\s%s\s|\(%s\)|\s%s$|\s%s\)|\s%s,|\(%s,)',
                     lambda m: m.group().replace('%s', '?'),
                     ' ' + sql + ' ')
        sql = sql.strip()

    return sql


def handle_in_clause_params(sql, args):
    """
    Expand list/tuple parameters for IN clauses across different database drivers

    For SQL like: "WHERE x IN %s" and args like [('A', 'B', 'C')]
    converts to: "WHERE x IN (%s, %s, %s)" and args ['A', 'B', 'C']

    Also handles named parameters like: "WHERE x IN %(foo)s"

    Enhanced to support direct list parameters for convenience:
    "WHERE x IN %s" and args like [1, 2, 3] will also work
    """
    if not args or not sql or ' in ' not in sql.lower():
        return sql, args

    modified_sql = sql
    modified_args = args

    # Auto-detect and convert direct list parameters for IN clauses
    # This makes db.select(cn, "SELECT * FROM users WHERE id IN %s", [1, 2, 3]) work
    if isinstance(args, list) and len(args) > 0 and sql.count('%s') == 1 and ' in ' in sql.lower():
        if not isinstance(args[0], list | tuple):
            # Check if this is likely a direct list parameter (multiple numeric/boolean/etc. items)
            # or a single string/bytes parameter that shouldn't be treated as a sequence
            if len(args) > 1 or not isinstance(args[0], str | bytes):
                # Direct list parameters for a single IN clause
                modified_args = (args,)
        elif isinstance(args[0], list | tuple):
            # First item is already a sequence - standard format
            modified_args = (args[0],)

    # Handle positional parameters (%s) and dictionary parameters (%(name)s)
    if isinstance(args, list | tuple):
        modified_sql, modified_args = _handle_positional_in_params(modified_sql, modified_args)
    elif isinstance(args, dict):
        modified_sql, modified_args = _handle_named_in_params(modified_sql, modified_args)

    # Convert to tuple for consistency
    modified_args = tuple(modified_args) if isinstance(modified_args, tuple | list) else modified_args

    return modified_sql, modified_args


def _handle_positional_in_params(sql, args):
    """Process IN clauses with positional parameters (%s)"""
    # Convert tuple args to list for modification
    if isinstance(args, tuple):
        args = list(args)

    # Find all "IN %s" patterns
    standard_pattern = re.compile(r'\bIN\s+(%s)\b', re.IGNORECASE)
    matches = list(standard_pattern.finditer(sql))
    modified_sql = sql

    # Process matches in reverse to avoid position shifts
    for match in reversed(matches):
        placeholder_pos = match.start(1)

        # Skip named parameters in the count
        text_before = sql[:placeholder_pos]
        param_index = text_before.count('%s')
        named_params = re.findall(r'%\([^)]+\)s', text_before)
        param_index -= len(named_params)

        # Check if parameter index is valid
        if param_index >= len(args):
            continue

        # Get the parameter value
        param = args[param_index]

        # Handle traditional format with tuple containing a single sequence
        # Like: ([1, 2, 3],) - extract the inner sequence
        if (isinstance(param, tuple | list) and len(param) == 1 and
                isinstance(param[0], Sequence) and not isinstance(param[0], str | bytes)):
            param = param[0]

        # Only expand if param is a sequence but not a string or bytes
        if not isinstance(param, Sequence) or isinstance(param, str | bytes):
            continue

        # Handle empty sequences - replace with NULL for empty IN clause
        if not param:
            modified_sql = _replace_match(modified_sql, match, 'in (null)')
            args.pop(param_index)
        else:
            # Create the correct number of placeholders
            placeholders = ', '.join(['%s'] * len(param))
            modified_sql = _replace_match(modified_sql, match, f'in ({placeholders})')

            # Replace the original param with expanded values
            args.pop(param_index)
            for i, val in enumerate(param):
                args.insert(param_index + i, val)

    return modified_sql, args


def _handle_named_in_params(sql, args):
    """Process IN clauses with named parameters (%(name)s)"""
    # Create a new dictionary to avoid modifying the original
    args = args.copy()

    # Find all "IN %(name)s" patterns
    named_pattern = re.compile(r'\bIN\s+(%\([^)]+\)s)\b', re.IGNORECASE)
    named_matches = list(named_pattern.finditer(sql))
    modified_sql = sql

    # Process matches in reverse to avoid position shifts
    for match in reversed(named_matches):
        param_name = match.group(1)  # Gets "%(name)s"
        name = re.search(r'%\(([^)]+)\)s', param_name).group(1)

        # Skip if parameter not in args dictionary
        if name not in args:
            continue

        param = args[name]

        # Only expand if param is a sequence but not a string or bytes
        if not isinstance(param, Sequence) or isinstance(param, str | bytes):
            continue

        # Handle empty sequences
        if not param:
            modified_sql = _replace_match(modified_sql, match, 'in (null)')
            args.pop(name)
        else:
            # Generate named parameters for each value
            placeholders = []
            for i, val in enumerate(param):
                param_key = f'{name}_{i}'
                placeholders.append(f'%({param_key})s')
                args[param_key] = val

            modified_sql = _replace_match(modified_sql, match, f'in ({", ".join(placeholders)})')
            args.pop(name)

    return modified_sql, args


def _replace_match(sql, match, replacement):
    """Helper function to replace a regex match in the SQL string"""
    return sql[:match.start(0)] + replacement + sql[match.end(0):]


def quote_identifier(db_type, identifier):
    """Safely quote database identifiers based on database type"""
    if db_type in {'postgres', 'sqlite'}:
        return '"' + identifier.replace('"', '""') + '"'
    elif db_type == 'sqlserver':
        return '[' + identifier.replace(']', ']]') + ']'
    else:
        raise ValueError(f'Unknown database type: {db_type}')


def escape_like_clause_placeholders(sql):
    """Escape percent signs in LIKE clauses to avoid conflict with parameter placeholders

    Double the percent signs in string literals within LIKE clauses so that
    database drivers interpret them as literal percent signs rather than placeholders.
    """
    if ' LIKE ' in sql or ' like ' in sql:
        # Only double percent signs in LIKE patterns, not in placeholders
        sql = re.sub(r"(LIKE|like)\s+('[^']*'|\"[^\"]*\")",
                    lambda m: m.group(1) + ' ' + m.group(2).replace('%', '%%'),
                    sql)
    return sql


def sanitize_sql_for_logging(sql, args=None):
    """Remove sensitive information from SQL for logging.
    We mask parameters likely to contain sensitive info like passwords.
    """
    # Simple list of sensitive terms
    sensitive_terms = [
        'password', 'passwd', 'secret', 'key', 'token', 'credential',
        'credit_card', 'creditcard', 'card_number', 'cardnumber',
    ]

    # Create copies of the inputs
    sanitized_sql = sql

    # Sanitize the arguments
    if args is not None:
        if isinstance(args, dict):
            # For dictionary args, mask sensitive keys
            sanitized_args = args.copy()
            for key in sanitized_args:
                if any(term in key.lower() for term in sensitive_terms):
                    sanitized_args[key] = '***'
        elif isinstance(args, list | tuple):
            # For list/tuple args, try to infer positions from SQL
            sanitized_args = list(args)

            # Look for sensitive column names in INSERT statements
            if 'insert into' in sanitized_sql.lower():
                columns_match = re.search(r'INSERT\s+INTO\s+\w+\s*\(([^)]+)\)', sql, re.IGNORECASE)
                if columns_match:
                    columns = [col.strip().lower() for col in columns_match.group(1).split(',')]
                    # Mask values for sensitive columns
                    for i, col in enumerate(columns):
                        if i < len(sanitized_args) and any(term in col for term in sensitive_terms):
                            sanitized_args[i] = '***'

            # Check for patterns like "password = %s" and mask corresponding args
            for term in sensitive_terms:
                for match in re.finditer(rf'\b{term}\b\s*=\s*%s', sql, re.IGNORECASE):
                    placeholder_pos = sql[:match.end()].count('%s') - 1
                    if placeholder_pos < len(sanitized_args):
                        sanitized_args[placeholder_pos] = '***'
        else:
            # For other arg types, just pass through
            sanitized_args = args
    else:
        sanitized_args = None

    return sanitized_sql, sanitized_args


def handle_query_params(func):
    """Decorator to handle query parameters for different databases

    1. Standardizes placeholders between ? and %s
    2. Handles IN clause parameter expansion
    3. Escapes percent signs in LIKE clauses
    4. Converts special data types (numpy, pandas)
    """
    from database.adapters.type_converter import TypeConverter
    from database.utils.connection_utils import is_psycopg_connection
    from database.utils.connection_utils import is_pymssql_connection
    from database.utils.connection_utils import is_sqlite3_connection

    @wraps(func)
    def wrapper(cn, sql, *args, **kwargs):
        # Skip processing if no arguments provided
        if not args:
            return func(cn, sql, *args, **kwargs)

        # Get database type for connection
        if hasattr(cn, 'get_driver_type'):
            db_type = cn.get_driver_type()
        else:
            # For transaction objects
            if hasattr(cn, 'connection') and hasattr(cn.connection, 'get_driver_type'):
                db_type = cn.connection.get_driver_type()
            else:
                if is_psycopg_connection(cn):
                    db_type = 'postgres'
                elif is_sqlite3_connection(cn):
                    db_type = 'sqlite'
                elif is_pymssql_connection(cn):
                    db_type = 'sqlserver'
                else:
                    db_type = 'unknown'

        # Standardize placeholders
        sql = standardize_placeholders(db_type, sql)

        # Handle LIKE clause escaping
        sql = escape_like_clause_placeholders(sql)

        # Handle IN clause parameters
        sql, args = handle_in_clause_params(sql, args)

        # Convert numpy and pandas values
        args = TypeConverter.convert_params(args)

        return func(cn, sql, *args, **kwargs)

    return wrapper


def prepare_sql_params_for_execution(sql, args):
    """
    Prepare SQL and parameters for execution, handling special cases like named parameters
    and IN clauses. This centralizes parameter handling logic that needs to happen
    just before database execution.

    Args:
        sql: The SQL query string
        args: Query parameters (can be dict, tuple, list, or single value)

    Returns
        Tuple of (processed_sql, processed_args)
    """
    # Special handling for dictionary parameters with IN clauses
    if isinstance(args, dict):
        sql, args = handle_in_clause_params(sql, args)
    elif len(args) == 1 and isinstance(args[0], dict):
        sql, args = handle_in_clause_params(sql, args[0])
        args = args  # Keep as dict

    return sql, args
