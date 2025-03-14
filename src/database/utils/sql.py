"""
SQL formatting and manipulation utilities.

This module provides utility functions for SQL query formatting and parameter handling:
- SQL placeholder standardization (? vs %s) based on database type
- SQL IN clause parameter expansion for lists/tuples
- SQL LIKE clause escaping
- SQL logging sanitization for sensitive data
"""
import re
from functools import wraps

from libb.iterutils import isiterable, issequence

#
# Public API functions
#


def process_query_parameters(cn, sql, args):
    """
    Single entry point for all parameter processing logic.

    Args:
        cn: Connection
        sql: SQL query string
        args: Parameters (list, tuple, dict, or scalar)

    Returns
        Tuple of (processed_sql, processed_args)
    """
    # Quick exit for empty parameters
    if not args:
        return sql, args
        
    # Handle nested parameters for multi-statement queries
    # This case handles patterns like [(param1, param2, param3)] when multiple
    # statements with multiple placeholders are present
    has_multiple_statements = ';' in sql
    if has_multiple_statements and isinstance(args, (list, tuple)) and len(args) == 1:
        if isinstance(args[0], (list, tuple)):
            # Count placeholders to see if the inner tuple has the exact parameters we need
            placeholder_count = sql.count('%s') + sql.count('?')
            inner_params = args[0]
            
            # If the inner parameter collection matches our placeholder count,
            # we'll unwrap it to be the primary parameter collection
            if len(inner_params) == placeholder_count:
                args = inner_params

    # Special handling for named parameters with IN clauses
    if ' in ' in sql.lower():
        if isinstance(args, dict):
            sql, args = _handle_named_in_params(sql, args)
        elif args and len(args) == 1 and isinstance(args[0], dict):
            sql, args_dict = _handle_named_in_params(sql, args[0])
            args = (args_dict,)  # Restore original tuple structure

    # 1. Standardize placeholders based on DB type
    sql = standardize_placeholders(cn, sql)

    # 2. Handle NULL values with IS/IS NOT operators
    sql, args = handle_null_is_operators(sql, args)

    # 3. Escape percent signs in all string literals
    sql = escape_percent_signs_in_literals(sql)

    # 4. Handle IN clause parameters
    sql, args = handle_in_clause_params(sql, args)

    # 5. Convert numpy and pandas values
    from database.adapters.type_conversion import TypeConverter
    args = TypeConverter.convert_params(args)

    return sql, args


def standardize_placeholders(cn, sql):
    """
    Standardize SQL placeholders between ? and %s based on database type.

    Args:
        cn: Connection
        sql: SQL query string

    Returns
        SQL with standardized placeholders
    """
    from database.utils.connection_utils import is_psycopg_connection
    from database.utils.connection_utils import is_pyodbc_connection
    from database.utils.connection_utils import is_sqlite3_connection

    if not sql:
        return sql

    if is_psycopg_connection(cn):
        # For PostgreSQL, convert ? to %s, but preserve regex patterns
        if 'regexp_replace' in sql:
            return _preserve_regex_patterns_in_sql(sql)
        else:
            # No regexp_replace, simple replacement
            return re.sub(r'(?<!\w)\?(?!\w)', '%s', sql)
    elif is_pyodbc_connection(cn) or is_sqlite3_connection(cn):
        # For SQL Server and SQLite, convert %s to ?
        # Pattern to handle %s placeholders not inside string literals
        return re.sub(r'(?<![\'"])%s(?![\'"])', '?', sql)

    return sql


def handle_in_clause_params(sql, args):
    """
    Expand list/tuple parameters for IN clauses across different database drivers.

    For SQL like: "WHERE x IN %s" and args like [('A', 'B', 'C')]
    converts to: "WHERE x IN (%s, %s, %s)" and args ['A', 'B', 'C']

    Also handles named parameters like: "WHERE x IN %(foo)s"

    Enhanced to support direct list parameters for convenience:
    "WHERE x IN %s" and args like [1, 2, 3] will also work.

    Args:
        sql: SQL query string
        args: Query parameters (list, tuple, dict)

    Returns
        Tuple of (processed_sql, processed_args)
    """
    # Quick exit if nothing to process
    if not args or not sql or ' in ' not in sql.lower():
        return sql, args

    # Preprocessing - detect and normalize various parameter formats
    modified_args = _preprocess_in_clause_params(sql, args)

    # Process parameters based on type
    if isinstance(modified_args, list | tuple):
        modified_sql, modified_args = _handle_positional_in_params(sql, modified_args)
    elif isinstance(modified_args, dict):
        modified_sql, modified_args = _handle_named_in_params(sql, modified_args)
    else:
        # No changes needed for other parameter types
        return sql, args

    # Ensure we return a tuple if we're returning a flattened list of parameters
    if isinstance(modified_args, list) and not isinstance(args, dict):
        modified_args = tuple(modified_args)

    return modified_sql, modified_args


def quote_identifier(db_type, identifier):
    """
    Safely quote database identifiers based on database type.

    Args:
        db_type: Database type ('postgresql', 'mssql', 'sqlite')
        identifier: Database identifier (table name, column name, etc.)

    Returns
        str: Quoted identifier string
    """
    if db_type in {'postgresql', 'sqlite'}:
        return '"' + identifier.replace('"', '""') + '"'
    elif db_type == 'mssql':
        return '[' + identifier.replace(']', ']]') + ']'
    else:
        raise ValueError(f'Unknown database type: {db_type}')


def get_param_limit_for_db(db_type):
    """
    Get the parameter limit for a given database type.

    Args:
        db_type: Database type ('postgresql', 'sqlite', 'mssql', or other)

    Returns
        int: Parameter limit for the database
    """
    if db_type == 'postgresql':
        return 65000  # PostgreSQL limit is 65535, using 65000 for safety
    if db_type == 'sqlite':
        return 900    # SQLite default is 999, using 900 for safety
    if db_type == 'mssql':
        return 2000   # SQL Server limit is 2100, using 2000 for safety
    return 900    # Conservative default for unknown databases


def chunk_sql_parameters(sql, args, param_limit):
    """
    Split parameters into chunks to avoid database parameter limits.

    Args:
        sql: SQL statement
        args: Parameters (tuple or list)
        param_limit: Maximum parameters per execution

    Returns
        List of parameter chunks to execute separately
    """
    if not args or len(args) <= param_limit:
        return [args]

    # Create chunks of parameters
    chunks = []
    for i in range(0, len(args), param_limit):
        chunks.append(args[i:i + param_limit])

    return chunks


def prepare_parameters_for_execution(sql: str, args: tuple,
                                     db_type: str, param_limit: int) -> list:
    """
    Prepare SQL parameters for execution, handling chunking if needed.

    Args:
        sql: SQL query to execute
        args: Parameters for the query
        db_type: Database type (postgresql, sqlite, mssql)
        param_limit: Parameter limit for this database type

    Returns
        list: List of parameter chunks to execute
    """
    # No chunking needed for empty args or parameters within limits
    if not args or len(args) <= param_limit:
        return [args]

    # Return list of parameter chunks
    return chunk_sql_parameters(sql, args, param_limit)


def prepare_stored_procedure_parameters(sql: str, args: tuple) -> tuple:
    """
    Prepare parameters for SQL Server stored procedure execution.

    Converts named parameters in stored procedures to positional parameters
    and adjusts parameter counts to match placeholders.

    Args:
        sql: SQL query string for a stored procedure
        args: Original parameters

    Returns
        tuple: (processed_sql, processed_args_list) where processed_args_list is
               a list of parameter chunks (usually just one for stored procedures)
    """
    # This incorporates the SQL Server-specific parameter handling logic
    from database.utils.sqlserver_utils import prepare_sqlserver_params

    # Convert parameters to positional placeholders
    processed_sql, processed_args = prepare_sqlserver_params(sql, args)

    # Count placeholders in the SQL
    placeholder_count = processed_sql.count('?')
    param_count = len(processed_args) if processed_args else 0

    if not processed_args:
        return processed_sql, [[]]

    # Adjust parameter count to match placeholders
    if placeholder_count != param_count:
        if placeholder_count > param_count:
            # Add None values if we need more parameters
            processed_args = list(processed_args)
            processed_args.extend([None] * (placeholder_count - param_count))
        else:
            # Truncate if we have too many parameters
            processed_args = processed_args[:placeholder_count]

    # Handle single parameter case specially
    if placeholder_count == 1 and len(processed_args) >= 1:
        return processed_sql, [[processed_args[0]]]

    return processed_sql, [processed_args]


def sanitize_sql_for_logging(sql, args=None):
    """
    Remove sensitive information from SQL for logging.

    Masks parameters likely to contain sensitive info like passwords.

    Args:
        sql: SQL query string
        args: Query parameters

    Returns
        Tuple of (sanitized_sql, sanitized_args)
    """
    # List of terms considered sensitive
    sensitive_terms = [
        'password', 'passwd', 'secret', 'token', 'credential',
        'credit_card', 'creditcard', 'card_number', 'cardnumber', 'auth',
    ]

    # Special handling for 'key' to not mask 'key_id'
    sensitive_exact_terms = ['key']

    # Create copies of the inputs
    sanitized_sql = sql

    # Sanitize the arguments
    if args is not None:
        if isinstance(args, dict):
            # For dictionary args, mask sensitive keys
            sanitized_args = args.copy()
            for key in sanitized_args:
                key_lower = key.lower()

                # Check for exact sensitive terms, but exclude special exceptions
                if any(term == key_lower for term in sensitive_exact_terms) and not key_lower.endswith('_id'):
                    sanitized_args[key] = '***'
                # Check for contained sensitive terms
                elif any(term in key_lower for term in sensitive_terms):
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
                        # Handle exact 'key' term separately to ignore 'key_id'
                        if i < len(sanitized_args) and col == 'key':
                            sanitized_args[i] = '***'
                        # Handle other sensitive terms normally
                        elif i < len(sanitized_args) and any(term in col for term in sensitive_terms):
                            sanitized_args[i] = '***'

            # Check for patterns like "password = %s", "auth_token = %s" and mask corresponding args
            # Handle exact 'key' term separately
            for term in sensitive_exact_terms:
                pattern = rf'\b{term}\b\s*=\s*(%s|\?)'
                for match in re.finditer(pattern, sql, re.IGNORECASE):
                    text_before = sql[:match.start(1)]
                    placeholder_count = text_before.count('%s') + text_before.count('?')
                    param_index = placeholder_count
                    if 0 <= param_index < len(sanitized_args):
                        sanitized_args[param_index] = '***'

            # Handle other sensitive terms
            for term in sensitive_terms:
                # Match sensitive terms in column references
                pattern = rf'\b\w*{term}\w*\b\s*=\s*(%s|\?)'
                for match in re.finditer(pattern, sql, re.IGNORECASE):
                    # Count placeholders up to this point to determine position
                    text_before = sql[:match.start(1)]  # Position of the %s or ? placeholder
                    placeholder_count = text_before.count('%s') + text_before.count('?')
                    param_index = placeholder_count
                    if 0 <= param_index < len(sanitized_args):
                        sanitized_args[param_index] = '***'
        else:
            # For other arg types, just pass through
            sanitized_args = args
    else:
        sanitized_args = None

    return sanitized_sql, sanitized_args


def handle_query_params(func):
    """
    Decorator to handle query parameters for different databases.

    Performs the following actions on SQL queries:
    1. Standardizes placeholders between ? and %s
    2. Handles IN clause parameter expansion
    3. Escapes percent signs in LIKE clauses
    4. Converts special data types (numpy, pandas)

    Args:
        func: Function to decorate

    Returns
        Decorated function
    """
    @wraps(func)
    def wrapper(cn, sql, *args, **kwargs):
        # Always escape percent signs in literals for all SQL queries
        sql = escape_percent_signs_in_literals(sql)

        # Skip the rest of the processing if no arguments provided
        if not args:
            return func(cn, sql, *args, **kwargs)

        # Get database type and process parameters
        processed_sql, processed_args = process_query_parameters(cn, sql, args)

        return func(cn, processed_sql, *processed_args, **kwargs)

    return wrapper


def escape_percent_signs_in_literals(sql):
    """
    Escape percent signs in all string literals to avoid conflict with parameter placeholders.

    This function ensures that any % character inside string literals gets properly escaped as %%
    so that the database driver doesn't interpret it as a parameter placeholder.

    Args:
        sql: SQL query string

    Returns
        str: SQL with escaped percent signs in string literals
    """
    if not sql or '%' not in sql:
        return sql

    # Helper function to escape % in single-quoted strings
    # Handles SQL escaping of quotes ('' → ')
    def escape_single_quoted(match):
        content = match.group(1)
        # Replace single % with %%, but only if not already part of %%
        return "'" + re.sub(r'(?<!%)%(?!%)', '%%', content) + "'"

    # Helper function to escape % in double-quoted strings
    # Handles SQL escaping of quotes ("" → ")
    def escape_double_quoted(match):
        content = match.group(1)
        # Replace single % with %%, but only if not already part of %%
        return '"' + re.sub(r'(?<!%)%(?!%)', '%%', content) + '"'

    # First pass: handle single-quoted strings with '' escaping
    # The regex pattern (?:[^']|'')*? matches content allowing for '' as escaped '
    pattern_single = r"'((?:[^']|'')*?)'"
    sql = re.sub(pattern_single, escape_single_quoted, sql)

    # Second pass: handle double-quoted strings with "" escaping
    pattern_double = r'"((?:[^"]|"")*?)"'
    sql = re.sub(pattern_double, escape_double_quoted, sql)

    return sql


def handle_null_is_operators(sql, args):
    """
    Handle NULL values used with IS and IS NOT operators.

    Converts 'IS %s' and 'IS NOT %s' with None parameters to 'IS NULL' and 'IS NOT NULL'
    instead of trying to use parameterized NULL values which causes syntax errors.
    Also handles named parameters like 'IS %(param)s'.

    Args:
        sql: SQL query string
        args: Query parameters

    Returns
        Tuple of (processed_sql, processed_args)
    """
    if not args or not sql:
        return sql, args

    # Process differently based on whether args is a dict (named params) or tuple/list (positional)
    if isinstance(args, dict):
        return _handle_null_is_named_params(sql, args)
    else:
        return _handle_null_is_positional_params(sql, args)


def _handle_null_is_positional_params(sql, args):
    """
    Handle NULL values with IS/IS NOT for positional parameters (%s, ?).

    Args:
        sql: SQL query string
        args: List or tuple of parameters

    Returns
        Tuple of (processed_sql, processed_args)
    """
    # Pattern to find IS or IS NOT followed by a positional placeholder
    pattern = r'\b(IS\s+NOT|IS)\s+(%s|\?)\b'

    # Convert list/tuple args to a list for modification
    args_was_tuple = isinstance(args, tuple)
    if args_was_tuple:
        args = list(args)

    # Find all matches of IS or IS NOT with placeholder
    matches = list(re.finditer(pattern, sql, re.IGNORECASE))

    # Process matches in reverse to avoid position shifts
    for match in reversed(matches):
        # Find the position of this placeholder in the SQL
        text_before = sql[:match.start(2)]
        placeholder_count = text_before.count('%s') + text_before.count('?')

        # Check if we have enough arguments
        if placeholder_count < len(args):
            param_value = args[placeholder_count]

            # If the parameter is None, replace the pattern with IS NULL or IS NOT NULL
            if param_value is None:
                operator = match.group(1).upper()  # IS or IS NOT
                replacement = f'{operator} NULL'

                # Replace in SQL
                start_pos = match.start(0)
                end_pos = match.end(0)
                sql = sql[:start_pos] + replacement + sql[end_pos:]

                # Remove the parameter from args
                args.pop(placeholder_count)

    # Convert back to tuple if the input was a tuple
    if args_was_tuple:
        args = tuple(args)

    return sql, args


def _handle_null_is_named_params(sql, args):
    """
    Handle NULL values with IS/IS NOT for named parameters (%(name)s).

    Args:
        sql: SQL query string
        args: Dictionary of named parameters

    Returns
        Tuple of (processed_sql, processed_args)
    """
    # Pattern to find IS or IS NOT followed by a named parameter
    pattern = r'\b(IS\s+NOT|IS)\s+(%\([^)]+\)s)\b'

    # Make a copy of the args dictionary
    args = args.copy()

    # Find all matches of IS or IS NOT with named parameter
    matches = list(re.finditer(pattern, sql, re.IGNORECASE))

    # Process each match
    for match in matches:
        param_placeholder = match.group(2)  # Gets "%(name)s"
        param_name = re.search(r'%\(([^)]+)\)s', param_placeholder).group(1)

        # Check if this parameter exists and is None
        if param_name in args and args[param_name] is None:
            operator = match.group(1).upper()  # IS or IS NOT
            replacement = f'{operator} NULL'

            # Replace in SQL
            start_pos = match.start(0)
            end_pos = match.end(0)
            sql = sql[:start_pos] + replacement + sql[end_pos:]

            # Remove the parameter from args
            args.pop(param_name)

    return sql, args


def prepare_sql_params_for_execution(sql, args, cn=None):
    """
    Prepare SQL and parameters for execution before sending to database.

    Handles special cases like named parameters and IN clauses. This centralizes
    parameter handling logic that needs to happen just before database execution.

    Args:
        sql: The SQL query string
        args: Query parameters (can be dict, tuple, list, or single value)
        cn: Optional connection for db_type detection

    Returns
        Tuple of (processed_sql, processed_args)
    """
    # If we have a connection, use the full parameter processing
    if cn is not None:
        # Check for named parameters with IN clauses before standard processing
        if ' in ' in sql.lower():
            if isinstance(args, dict):
                sql, args = _handle_named_in_params(sql, args)
            elif args and len(args) == 1 and isinstance(args[0], dict) and ' in ' in sql.lower():
                sql, args_dict = _handle_named_in_params(sql, args[0])
                args = (args_dict,)  # Restore original tuple structure

        # Continue with regular parameter processing
        sql = standardize_placeholders(cn, sql)
        sql, args = handle_in_clause_params(sql, args)

        # Convert special types
        from database.adapters.type_conversion import TypeConverter
        args = TypeConverter.convert_params(args)

        # Database-specific parameter handling
        from database.utils.connection_utils import is_pyodbc_connection
        if is_pyodbc_connection(cn):
            from database.utils.sqlserver_utils import _handle_in_clause

            # SQL Server-specific IN clause handling
            sql, args = _handle_in_clause(sql, args)

        return sql, args

    # Otherwise, handle dictionary parameters with IN clauses
    if isinstance(args, dict) and ' in ' in sql.lower():
        sql, args = _handle_named_in_params(sql, args)
    elif args and len(args) == 1 and isinstance(args[0], dict) and ' in ' in sql.lower():
        sql, args = _handle_named_in_params(sql, args[0])
        args = (args,)  # Restore original tuple structure

    return sql, args


#
# Private helper functions for IN clause parameter handling
#

def _is_simple_scalar(obj):
    """
    Check if the object is a simple scalar value (not a container type).

    Args:
        obj: Object to check

    Returns
        bool: True if object is a simple scalar
    """
    # A simple scalar is anything that is not an iterable, or is a string
    # (which are technically sequences but treated as scalars for SQL parameters)
    return not isiterable(obj) or isinstance(obj, str)


def _count_in_clauses(sql):
    """
    Count the number of IN clauses in an SQL query.

    Args:
        sql: SQL query string

    Returns
        int: Number of IN clauses
    """
    return sql.lower().count(' in ')


def _is_single_in_clause_query(sql):
    """
    Check if the SQL has exactly one IN clause and one placeholder.

    Args:
        sql: SQL query string

    Returns
        bool: True if query has one IN clause and one placeholder
    """
    return ' in ' in sql.lower() and sql.count('%s') == 1


def _is_direct_list_parameter(args, sql):
    """
    Determine if args is a direct list parameter for an IN clause.

    A direct list parameter is a list or tuple of values that should be
    expanded for an IN clause, without requiring the extra nesting.

    Args:
        args: Function arguments
        sql: SQL query string

    Returns
        bool: True if this is a direct list parameter
    """
    # For a direct list/tuple like [1, 2, 3] or (1, 2, 3)
    if isinstance(args, list | tuple) and _is_single_in_clause_query(sql):
        # Make sure all items are simple scalar values (not iterables except strings)
        if all(not isiterable(arg) or isinstance(arg, str) for arg in args):
            return True
    return False


def _is_single_item_list(args):
    """
    Check if the argument is a single-item list that needs special handling.

    Args:
        args: Function arguments

    Returns
        bool: True if this is a single-item list
    """
    return (isinstance(args, list) and
            len(args) == 1 and
            (not isiterable(args[0]) or isinstance(args[0], str)))


def _is_multiple_in_clause_with_lists(args, sql):
    """
    Check if query has multiple IN clauses with matching list parameters.

    Args:
        args: Function arguments
        sql: SQL query string

    Returns
        bool: True if this is a multiple IN clause query with list parameters
    """
    return (isinstance(args, list) and
            ' and ' in sql.lower() and
            ' in ' in sql.lower() and
            len(args) == _count_in_clauses(sql) and
            all((not isiterable(arg) or isinstance(arg, str)) or isinstance(arg, list | tuple) for arg in args))


def _preprocess_in_clause_params(sql, args):
    """
    Preprocess IN clause parameters to handle various input formats.

    This function normalizes different parameter formats into a consistent
    structure that can be processed by the detailed handler functions.

    Args:
        sql: SQL query string
        args: Query parameters (list, tuple, dict)

    Returns
        processed_args: Args in a consistent format for further processing
    """
    # Quick check for no args or no IN clause
    if not args or not sql or ' in ' not in sql.lower():
        return args

    # Check for empty args or no IN clause
    if not args or not sql or ' in ' not in sql.lower():
        return args

    # Case 1: Direct list/tuple parameters [1, 2, 3] or (1, 2, 3)
    if _is_direct_list_parameter(args, sql):
        # Wrap in tuple to get ([1, 2, 3],) format expected by handler
        result = (args,)
        return result

    # Case 2: Single-item list [101]
    if _is_single_item_list(args):
        # Convert [101] to ([101],) format - IMPORTANT: keep actual value, don't convert to string
        # This preserves the original type (int, float, etc.) and prevents string conversion
        result = ([args[0]],)
        return result

    # Case 3: Multiple IN clauses with direct lists
    if _is_multiple_in_clause_with_lists(args, sql):
        # Convert each list to a tuple for standard parameter handling
        result = tuple(tuple(item) if isinstance(item, list) else
                       [item] if not issequence(item) else item
                       for item in args)
        return result

    # Default: no preprocessing needed
    return args


def _handle_positional_in_params(sql, args):
    """
    Process IN clauses with positional parameters (%s).

    Expands list/tuple parameters into multiple placeholders for IN clauses
    and flattens the parameter list accordingly.

    Args:
        sql: SQL query string
        args: List or tuple of parameters

    Returns
        Tuple of (processed_sql, processed_args)
    """
    # Convert tuple args to list for modification
    args_was_tuple = isinstance(args, tuple)
    if args_was_tuple:
        args = list(args)

    # If we have a top-level tuple containing exactly one item
    # and that item is itself a single-item tuple, flatten it now.
    if len(args) == 1 and isinstance(args[0], tuple) and len(args[0]) == 1:
        # e.g. ((101,), ) becomes just (101,)
        args = [args[0][0]]

    # Find all "IN %s" or "IN (%s)" patterns - careful not to match "IN (%s))" patterns
    standard_pattern = re.compile(r'\bIN\s+(%s|\(%s\)(?!\)))\b', re.IGNORECASE)
    matches = list(standard_pattern.finditer(sql))

    # Handle case with no IN clauses or no args
    if not matches or not args:
        return sql, args

    # Track the parameters we've processed
    processed = []
    result_args = []

    # Make a copy of the SQL to progressively modify
    current_sql = sql
    offset = 0  # Track position offset as we modify strings

    # Process each match with its corresponding parameter
    for i, match in enumerate(matches):
        if i >= len(args):
            break

        # Calculate positions with current offset
        start_pos = match.start(0) + offset
        end_pos = match.end(0) + offset
        param = args[i]

        # Only process if it's a sequence (but not a string or bytes)
        if issequence(param):
            # Handle case with tuple containing a sequence: ([1,2,3],) or a nested sequence
            if len(param) == 1 and issequence(param[0]):
                param = param[0]

            # Handle empty sequences
            if not param:
                current_sql = _replace_with_null(current_sql, start_pos, end_pos)
                offset += len('IN (NULL)') - (end_pos - start_pos)
                processed.append(None)
                # Don't add args for NULL
            # Special case for single-item sequence to preserve original type
            elif len(param) == 1:
                # Single placeholder for the single value
                placeholders = '%s'
                replacement = f'IN ({placeholders})'

                # Replace just this specific occurrence
                current_sql = current_sql[:start_pos] + replacement + current_sql[end_pos:]

                # Update the offset for the next match
                offset += len(replacement) - (end_pos - start_pos)

                # Add the single value directly (not as a string)
                value = param[0]  # Extract the actual item from the list/tuple
                # Flatten single-item sequences until we reach a non-sequence or multi-item sequence
                while issequence(value) and not isinstance(value, str) and len(value) == 1:
                    value = value[0]

                processed.append(param)
                result_args.append(value)  # Use the actual value, preserving its type
            else:
                # Expand the parameter for multiple values
                placeholders = ', '.join(['%s'] * len(param))
                replacement = f'IN ({placeholders})'

                # Replace just this specific occurrence
                current_sql = current_sql[:start_pos] + replacement + current_sql[end_pos:]

                # Update the offset for the next match
                offset += len(replacement) - (end_pos - start_pos)

                # Add expanded parameters
                processed.append(param)
                result_args.extend(param)
        else:
            # All non-sequence parameters should be treated as single values for IN clauses
            # (this includes strings, which are technically sequences but treated as scalars here)
            # Create a single-item placeholder
            replacement = 'IN (%s)'

            # Replace just this specific occurrence
            current_sql = current_sql[:start_pos] + replacement + current_sql[end_pos:]

            # Update the offset for the next match
            offset += len(replacement) - (end_pos - start_pos)

            # Add the parameter directly (not as a string)
            processed.append(param)
            result_args.append(param)

    # Add any remaining args that weren't part of IN clauses
    for i in range(len(processed), len(args)):
        result_args.append(args[i])

    # Convert back to tuple if the input was a tuple
    if args_was_tuple:
        result_args = tuple(result_args)

    # Handle a potential issue with single-value tuples being incorrectly formatted
    if args_was_tuple and isinstance(result_args, tuple) and len(result_args) == 1:
        if isinstance(result_args[0], tuple) and len(result_args[0]) == 1:
            # This is the problematic case: ((101,),) -> unwrap to just (101,)
            result_args = result_args[0]

    return current_sql, result_args


def _replace_with_null(sql, start_pos, end_pos):
    """
    Replace an IN clause placeholder with NULL.

    Args:
        sql: SQL query string
        start_pos: Starting position of the replacement
        end_pos: Ending position of the replacement

    Returns
        str: SQL with the replacement applied
    """
    return sql[:start_pos] + 'IN (NULL)' + sql[end_pos:]


def _handle_named_in_params(sql, args):
    """
    Process IN clauses with named parameters (%(name)s).

    Expands list/tuple parameters into multiple named placeholders for IN clauses
    and updates the parameter dictionary accordingly.

    Args:
        sql: SQL query string
        args: Dictionary of named parameters

    Returns
        Tuple of (processed_sql, processed_args)
    """
    # Handle case where args is a tuple/list containing a dictionary
    if isinstance(args, tuple | list) and len(args) == 1 and isinstance(args[0], dict):
        args = args[0]

    # Ensure args is a dictionary for processing
    if not isinstance(args, dict):
        return sql, args

    # Create a new dictionary to avoid modifying the original
    args = args.copy()

    # Improved regex pattern to match IN clause with named parameters
    # This pattern is more flexible with whitespace and works with different case styles
    named_pattern = re.compile(r'(?i)\b(in)\s+(%\([^)]+\)s)\b', re.IGNORECASE)
    named_matches = list(named_pattern.finditer(sql))

    modified_sql = sql

    # Process matches in reverse to avoid position shifts
    for match in reversed(named_matches):
        param_name = match.group(2)  # Gets "%(name)s"
        name = _extract_param_name(param_name)

        # Skip if parameter not in args dictionary
        if name not in args:
            continue

        param = args[name]

        # Only expand if param is a sequence but not a string or bytes
        if not issequence(param):
            continue

        # Handle empty sequences
        if not param:
            modified_sql = _replace_match(modified_sql, match, 'in (null)')
            args.pop(name)
        else:
            # Handle single-item tuples specially to unwrap values properly
            values_to_expand = param
            # Unwrap nested single-item sequences to get actual values
            if len(param) == 1 and issequence(param[0]) and not isinstance(param[0], str):
                values_to_expand = param[0]

            # Generate named parameters for each value
            placeholders = _generate_named_placeholders(name, values_to_expand, args)

            # Replace the pattern in SQL
            original_segment = match.group(0)
            new_segment = f'in ({", ".join(placeholders)})'
            modified_sql = _replace_match(modified_sql, match, new_segment)

            # Remove original parameter
            args.pop(name)

    return modified_sql, args


def _preserve_regex_patterns_in_sql(sql):
    """
    Process SQL with regexp_replace functions, preserving their regex patterns.

    This function identifies regexp_replace function calls and ensures their
    regex patterns containing ? characters are not mistakenly converted to
    PostgreSQL's %s placeholders.

    Args:
        sql: SQL query containing regexp_replace functions

    Returns
        SQL with placeholders standardized but regexp patterns preserved
    """
    # Strategy 1: Use regex to find and preserve regexp_replace patterns
    regexp_pattern = r'(regexp_replace\s*\([^,]+,\s*[\'"])(.*?)([\'"][^)]*\))'
    matches = list(re.finditer(regexp_pattern, sql, re.IGNORECASE))

    if matches:
        parts = []
        last_end = 0

        for match in matches:
            # Process text before this regexp_replace
            before_part = sql[last_end:match.start()]
            regexp_part = sql[match.start():match.end()]

            # Only standardize placeholders in non-regexp parts
            before_part = re.sub(r'(?<!\w)\?(?!\w)', '%s', before_part)

            # Add the parts to our result
            parts.extend((before_part, regexp_part))

            last_end = match.end()

        # Add any remaining text after the last regexp_replace
        if last_end < len(sql):
            remaining = sql[last_end:]
            remaining = re.sub(r'(?<!\w)\?(?!\w)', '%s', remaining)
            parts.append(remaining)

        return ''.join(parts)

    # Strategy 2: Character-by-character parsing as fallback
    parts = []
    in_regexp = False
    open_parens = 0
    start_pos = 0

    # Split by regexp_replace function calls
    for i, char in enumerate(sql):
        if not in_regexp and i+14 <= len(sql) and sql[i:i+14].lower() == 'regexp_replace':
            in_regexp = True
            # Process the part before regexp_replace
            if i > start_pos:
                part = sql[start_pos:i]
                # Only replace placeholders in non-regexp parts
                part = re.sub(r'(?<!\w)\?(?!\w)', '%s', part)
                parts.append(part)
            start_pos = i

        if in_regexp:
            if char == '(':
                open_parens += 1
            elif char == ')':
                open_parens -= 1
                if open_parens == 0:
                    in_regexp = False
                    # Don't modify the regexp_replace part
                    part = sql[start_pos:i+1]
                    parts.append(part)
                    start_pos = i+1

    # Add any remaining part
    if start_pos < len(sql):
        part = sql[start_pos:]
        # Only replace placeholders in non-regexp parts
        part = re.sub(r'(?<!\w)\?(?!\w)', '%s', part)
        parts.append(part)

    return ''.join(parts)


def _extract_param_name(param_placeholder):
    """
    Extract parameter name from a named parameter placeholder.

    Args:
        param_placeholder: Named parameter placeholder like "%(name)s"

    Returns
        str: Extracted parameter name
    """
    return re.search(r'%\(([^)]+)\)s', param_placeholder).group(1)


def _generate_named_placeholders(base_name, param_values, args_dict):
    """
    Generate named placeholders for IN clause and update args dictionary.

    Args:
        base_name: Base parameter name
        param_values: Parameter values to expand
        args_dict: Args dictionary to update

    Returns
        list: List of named placeholder strings
    """
    placeholders = []
    for i, val in enumerate(param_values):
        param_key = f'{base_name}_{i}'
        placeholders.append(f'%({param_key})s')
        args_dict[param_key] = val
    return placeholders


def _replace_match(sql, match, replacement):
    """
    Replace a regex match in the SQL string.

    Args:
        sql: SQL query string
        match: Regex match object
        replacement: Replacement string

    Returns
        str: SQL with the replacement applied
    """
    return sql[:match.start(0)] + replacement + sql[match.end(0):]
