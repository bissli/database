"""
SQL Server-specific utilities for database operations with ODBC Driver 18+.

This module contains utilities for handling SQL Server-specific tasks:
- Parameter placeholder conversion
- Type conversion for date/time types
- Query result handling
- Expression aliasing for calculated columns

ODBC Driver 18+ for SQL Server fully handles column names without truncation,
so this module focuses only on the minimal processing needed for
expression columns and special query types.

NOTE: ODBC Driver 18 or newer is REQUIRED for proper column name handling.
Earlier driver versions will cause issues with column name truncation.
"""
import datetime
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)


def generate_expression_alias(expression):
    """
    Generate a consistent, valid alias for a SQL expression.

    Args:
        expression: SQL expression to create an alias for

    Returns
        A valid SQL alias name
    """
    # Simplified alias generation for expressions

    # Special cases:
    if expression.upper().strip() == 'COUNT(*)':
        return 'count_result'

    # Function calls
    func_match = re.match(r'^([\w]+)\s*\(', expression)
    if func_match:
        func_name = func_match.group(1).lower()
        common_funcs = {'sum', 'min', 'max', 'avg', 'count', 'convert', 'cast'}
        if func_name in common_funcs:
            return f'{func_name}_result'

    # Mathematical expressions
    if any(op in expression for op in ['+', '-', '*', '/', '%']):
        return 'calc_result'

    # CASE expressions
    if re.search(r'(?i)\bCASE\b', expression):
        return 'case_result'

    # Subqueries
    if '(' in expression and re.search(r'(?i)\bSELECT\b', expression):
        return 'subquery_result'

    # Fallback for other expressions
    return _generate_hash_alias(expression)


def _generate_hash_alias(expression):
    """Generate a hash-based alias for complex SQL expressions"""
    import hashlib
    hash_result = hashlib.md5(expression.encode()).hexdigest()[:8]
    result = f'expr_{hash_result}'
    logger.debug(f"Generated hash-based alias: '{expression}' -> '{result}'")
    return result


def parse_sql_columns(columns_text):
    """
    Parse columns in a SQL statement, handling complex expressions and nested structures.

    Args:
        columns_text: The columns portion of a SQL SELECT statement

    Returns
        List of individual column expressions
    """
    logger.debug(f"parse_sql_columns called with: '{columns_text}'")

    columns = []
    current = ''
    paren_level = 0
    in_quotes = False
    quote_char = None

    for char in columns_text:
        # Handle quotes
        if char in {"'", '"'} and (not current.endswith('\\') or current.endswith('\\\\')):
            if not in_quotes:
                in_quotes = True
                quote_char = char
            elif char == quote_char:
                in_quotes = False
                quote_char = None

        # Track parentheses outside quotes
        if not in_quotes:
            if char == '(':
                paren_level += 1
            elif char == ')':
                paren_level -= 1

        # Split only on commas outside functions and quotes
        if char == ',' and paren_level == 0 and not in_quotes:
            col = current.strip()
            columns.append(col)
            current = ''
        else:
            current += char

    # Add the last column
    if current.strip():
        col = current.strip()
        columns.append(col)

    logger.debug(f'Parsed {len(columns)} columns: {columns}')
    return columns


def _handle_multiple_set_operations(sql):
    """
    Process SQL with multiple set operations (UNION, INTERSECT, EXCEPT).

    With ODBC Driver 18+, only expressions in the first SELECT need aliases.
    For most typical queries, no changes are needed.

    Args:
        sql: SQL query with multiple set operations

    Returns
        Processed SQL with minimal column aliasing (often unchanged)
    """
    # As with the main function, we only need to check for expressions needing aliases
    parts = re.split(r'(?i)\s+(union|intersect|except)(?:\s+all)?\s+', sql)
    if len(parts) <= 1:
        return sql

    # Check first SELECT for expressions without aliases
    first_select = parts[0]
    match = re.search(r'(?i)SELECT\s+(.*?)(?=\s+FROM)', first_select)
    if not match:
        return sql

    columns = parse_sql_columns(match.group(1))
    needs_aliases = any(
        not re.match(r'^[\w\[\]."]+$', col) and  # Not a simple column ref
        ' as ' not in col.lower()                 # Doesn't already have an alias
        for col in columns
    )

    # Only add aliases if needed
    if needs_aliases:
        parts[0] = add_explicit_column_aliases(first_select)

        # Reconstruct the SQL
        result = parts[0]
        for i in range(1, len(parts), 2):
            if i+1 < len(parts):
                result += f' {parts[i].upper()} {parts[i+1]}'
        return result

    # Otherwise return unchanged
    return sql


def handle_set_operations(sql):
    """
    Process SQL with UNION, INTERSECT, or EXCEPT operations.

    With ODBC Driver 18+, column names are fully preserved, so we only need
    to add aliases for expressions, not for simple columns. For most typical
    UNION queries, no changes are needed at all.

    Args:
        sql: SQL query possibly containing set operations

    Returns
        Processed SQL with minimal column aliasing (often unchanged)
    """
    # With ODBC Driver 18+, column names from the first SELECT are preserved
    # Simply return the original SQL in most cases
    return sql


def add_explicit_column_aliases(sql):
    """
    Add explicit aliases only to expression columns in SQL Server SELECT statements.

    With ODBC Driver 18+, this is only needed for expressions and calculated columns
    to ensure they have proper names in the result set. Simple column references
    are preserved correctly by the driver without needing aliases.

    This function is a simplified version that only handles:
    1. Function calls (SUM, COUNT, etc.)
    2. Expressions with operators (+, -, *, etc.)
    3. CASE statements
    4. Subqueries

    Args:
        sql: Original SQL query

    Returns
        SQL with minimal necessary expression aliases
    """
    # Skip processing if not needed
    if not sql or not isinstance(sql, str):
        return sql

    # Only process SELECT statements
    if not sql.lstrip().upper().startswith('SELECT'):
        return sql

    # No need to process INTO statements
    if ' INTO ' in sql.upper():
        return sql

    # Match the columns portion of the SELECT statement
    match = re.search(r'SELECT\s+(.*?)(?=\s+FROM\s+)', sql, re.IGNORECASE | re.DOTALL)
    if not match:
        return sql

    # Parse columns - this gives us individual column expressions
    columns = parse_sql_columns(match.group(1).strip())
    aliased_columns = []
    needs_processing = False  # Track if we actually need to change anything

    for col in columns:
        # Keep existing aliases
        if re.search(r'\sAS\s+', col, re.IGNORECASE):
            aliased_columns.append(col)
            continue

        # Simple column identifiers don't need aliases with Driver 18+
        if re.match(r'^[\w\[\]."]+$', col):  # Updated to include quoted columns
            aliased_columns.append(col)
            continue

        # Only add aliases for special cases that need them:
        # 1. Function calls without aliases
        # 2. Expressions with operators (+, -, etc.)
        # 3. CASE statements
        # 4. Subqueries
        needs_processing = True
        alias = generate_expression_alias(col)
        aliased_columns.append(f'{col} AS [{alias}]')

    # Only modify SQL if we added aliases - otherwise return original
    if needs_processing:
        select_part = f"SELECT {', '.join(aliased_columns)}"
        after_columns = sql[match.end():]
        return select_part + after_columns

    return sql


def extract_balanced_parentheses(text, start_pos):
    """
    Extract text with balanced parentheses from a given start position.

    Args:
        text: The text to extract from
        start_pos: Starting position of the opening parenthesis

    Returns
        String with balanced parentheses, including the outer ones
    """
    if start_pos >= len(text) or text[start_pos] != '(':
        return ''

    try:
        paren_level = 0
        end_pos = start_pos
        in_string = False
        string_char = None

        for i in range(start_pos, len(text)):
            char = text[i]

            # Handle string literals
            if char in {"'", '"'} and (i == 0 or text[i-1] != '\\'):
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char:
                    in_string = False
                    string_char = None

            # Only track parentheses outside strings
            if not in_string:
                if char == '(':
                    paren_level += 1
                elif char == ')':
                    paren_level -= 1

                    # Found matching closing parenthesis
                    if paren_level == 0:
                        end_pos = i
                        break

        # Return the substring with balanced parentheses
        return text[start_pos:end_pos+1]
    except Exception as e:
        logger.debug(f'Error extracting balanced parentheses: {e}')
        return f"({text[start_pos:].split(')', 1)[0]})" if ')' in text[start_pos:] else text[start_pos:]


def clean_column_name(name):
    """
    Clean SQL Server column name by removing null bytes and other problematic characters.

    With ODBC Driver 18+, column names are preserved correctly and rarely need
    cleaning. This function only handles extreme edge cases.

    Args:
        name: The column name to clean

    Returns
        Cleaned column name
    """
    if not isinstance(name, str):
        return name

    # Minimal sanitization - only remove truly problematic characters
    return name.replace('\x00', '')


def process_sqlserver_column(column_name, description_item=None, sql=None):
    """
    Process SQL Server column name to remove invalid characters.

    With ODBC Driver 18+, column names are preserved correctly without truncation,
    so this function only needs to perform basic sanitization.

    Args:
        column_name: The column name to process
        description_item: Optional cursor description item (not used)
        sql: Optional SQL query (not used)

    Returns
        Cleaned column name
    """
    if not column_name or not isinstance(column_name, str):
        return column_name

    # Basic sanitization only - Driver 18+ handles name preservation
    return clean_column_name(column_name)


def _has_pattern(sql, pattern):
    """Check if SQL contains a regex pattern."""
    # Make sure we don't double (?i) if it's already in the pattern
    if pattern.startswith('(?i)'):
        return bool(re.search(pattern, sql))
    else:
        return bool(re.search(pattern, sql.lower()))


def ensure_identity_column_named(sql):
    """
    Ensures special expressions have explicit column names in SQL Server queries.

    With ODBC Driver 18+, we only need to add aliases for a few critical cases:
    - @@identity (which has no intrinsic name)
    - COUNT(*) (when used as the only column)
    - SELECT 1 (literal values need names)

    Args:
        sql: The SQL query to check and modify

    Returns
        SQL with minimal naming for special expressions
    """
    # Skip processing if not needed
    if not sql or not isinstance(sql, str):
        return sql

    # Handle @@identity - always needs an alias
    if '@@identity' in sql.lower() and 'as ' not in sql.lower():
        sql = re.sub(r'(?i)@@identity\b', '@@identity AS id_col', sql)

    # Only handle "SELECT 1" queries when they're the only column
    if re.search(r'(?i)^\s*select\s+1\s+from', sql) and not re.search(r'(?i)select\s+1\s+as\s+', sql):
        sql = re.sub(r'(?i)^(\s*select\s+)1(\s+from)', r'\g<1>1 AS result_col\g<2>', sql)

    # Only handle COUNT(*) when it's the only column
    if re.search(r'(?i)^\s*select\s+count\(\*\)\s+from', sql) and not re.search(r'(?i)count\(\*\)\s+as\s+', sql):
        sql = re.sub(r'(?i)^(\s*select\s+)count\(\*\)(\s+from)', r'\g<1>COUNT(*) AS count_col\g<2>', sql)

    return sql


def handle_unnamed_columns_error(e, sql, args):
    """
    Handles SQL Server errors related to unnamed columns.

    Args:
        e: The exception that was raised
        sql: The SQL query that caused the error
        args: Query parameters

    Returns
        Tuple of (modified_sql, should_retry)
    """
    error_msg = str(e).lower()

    # Define error patterns that indicate unnamed column issues
    # These are still needed in rare cases even with Driver 18+
    unnamed_column_errors = [
        'columns with no names',
        'invalid column name',
        'column name or number'
    ]

    # Check if error is related to unnamed columns
    if not any(pattern in error_msg for pattern in unnamed_column_errors):
        return sql, False

    # Common patterns that need column names
    patterns = [
        ('@@identity', '@@identity AS id_col'),
        ('SELECT 1 FROM', 'SELECT 1 AS result_col FROM'),
        ('COUNT(*)', 'COUNT(*) AS count_col')
    ]

    # Try each pattern
    for pattern, replacement in patterns:
        if pattern.lower() in sql.lower() and 'as ' not in sql.lower():
            modified_sql = re.sub(rf'(?i){re.escape(pattern)}', replacement, sql)
            return modified_sql, True

    # Apply ensure_identity_column_named for simpler cases
    try:
        modified_sql = ensure_identity_column_named(sql)
        if modified_sql != sql:
            return modified_sql, True
    except Exception:
        pass

    return sql, False


def extract_identity_from_result(result):
    """
    Safely extract identity value from a SQL Server result row.

    With pyodbc, identity values typically come back with column
    name 'id' from our 'SELECT @@identity AS id' query.

    Args:
        result: Result row from cursor.fetchone()

    Returns
        The identity value
    """
    if result is None:
        return None

    # Try multiple access methods in order of likelihood
    access_methods = [
        # Method 1: Direct attribute access
        lambda r: getattr(r, 'id', None) if hasattr(r, 'id') else None,

        # Method 2: Dictionary access
        lambda r: r.get('id') if isinstance(r, dict) else None,

        # Method 3: For pyodbc Row objects with __members__
        lambda r: getattr(r, 'id', None) if hasattr(r, '__members__') and 'id' in r.__members__ else None,

        # Method 4: Index access (first column)
        lambda r: r[0] if hasattr(r, '__getitem__') else None
    ]

    # Try each method until we get a non-None result
    for method in access_methods:
        try:
            value = method(result)
            if value is not None:
                return value
        except (AttributeError, KeyError, IndexError, TypeError):
            continue

    # Return the result itself if we couldn't extract a value
    return result


def ensure_timezone_naive_datetime(dt):
    """
    Ensure a datetime object is timezone-naive for SQL Server compatibility.
    All SQL Server datetime values should be timezone-naive except DATETIMEOFFSET.

    Args:
        dt: datetime object to check

    Returns
        Timezone-naive datetime object
    """

    if dt is None or not isinstance(dt, datetime.datetime):
        return dt

    # Remove timezone if present
    return dt.replace(tzinfo=None) if dt.tzinfo is not None else dt


def convert_to_date_if_no_time(dt):
    """
    Convert a datetime to a date object if it has no time component.

    Args:
        dt: datetime object to check

    Returns
        The original datetime (conservative approach)
    """

    # SQL Server date handling is specific - return as-is and let the caller decide
    return dt


def _handle_datetimeoffset(dto_value):
    """
    Convert SQL Server datetimeoffset to Python datetime with timezone

    Args:
        dto_value: The datetimeoffset value from SQL Server

    Returns
        datetime object with timezone information
    """

    if dto_value is None:
        return None

    # pyodbc usually returns datetimeoffset values as datetime with tzinfo already
    if isinstance(dto_value, datetime.datetime):
        # With ODBC Driver 18+, the driver handles timezone conversion correctly
        # Just ensure we have the appropriate tzinfo
        if dto_value.tzinfo is None:
            # Default to UTC if no timezone info is provided
            return dto_value.replace(tzinfo=datetime.UTC)
        return dto_value

    # Handle string values (less common)
    if isinstance(dto_value, str):
        try:
            import dateutil.parser
            return dateutil.parser.parse(dto_value)
        except (ValueError, ImportError):
            logger.warning(f'Failed to parse datetimeoffset string: {dto_value}')
            # Fall through to return original value as datetime
            return datetime.datetime.now()  # Return current time as fallback

    # Handle byte values (don't try to parse as they're binary representation)
    if isinstance(dto_value, bytes):
        # With newer pyodbc versions, bytes shouldn't be returned for datetimeoffset
        # Return a datetime object instead of failing
        logger.warning('Received bytes for datetimeoffset value (driver issue)')
        return datetime.datetime.now()  # Return current time as fallback

    # For any other unexpected type, return a datetime
    if not isinstance(dto_value, datetime.datetime):
        logger.warning(f'Unexpected type for datetimeoffset: {type(dto_value)}')
        return datetime.datetime.now()  # Return current time as fallback

    return dto_value


def register_datetimeoffset_converter(connection):
    """
    Register a converter for SQL Server's datetimeoffset data type to Python's
    datetime with timezone.

    Args:
        connection: pyodbc connection to register the converter with
    """
    # Register the converter for the datetimeoffset SQL type (-155)
    try:
        connection.add_output_converter(-155, _handle_datetimeoffset)
        logger.debug('Successfully registered datetimeoffset converter')
    except AttributeError:
        logger.warning('Could not register datetimeoffset converter - pyodbc may be outdated')
    except Exception as e:
        logger.warning(f'Error registering datetimeoffset converter: {e}')


def get_sqlserver_column_value(row: Any, key: str | int, cursor=None) -> Any | None:
    """
    Get column value from SQL Server row.

    With ODBC Driver 18+, column names are preserved correctly, so this function
    has been greatly simplified. We now rely primarily on direct attribute access.

    Args:
        row: SQL Server row object
        key: Column name or index
        cursor: Optional cursor for additional context

    Returns
        Column value if found, None otherwise
    """
    if row is None:
        return None

    try:
        # Direct attribute access (most common with Driver 18+)
        if hasattr(row, key):
            return getattr(row, key)

        # For pyodbc Row objects, check __members__
        if hasattr(row, '__members__'):
            if key in row.__members__:
                return getattr(row, key)

            # Try case-insensitive match for string keys
            if isinstance(key, str):
                key_lower = key.lower()
                for member in row.__members__:
                    if isinstance(member, str) and member.lower() == key_lower:
                        return getattr(row, member)

        # Dictionary access for dict-like objects
        if isinstance(row, dict) and key in row:
            return row[key]

        # Index access for numeric keys on sequence-like objects
        if isinstance(key, int) and hasattr(row, '__getitem__'):
            try:
                return row[key]
            except (IndexError, TypeError):
                pass
    except Exception as e:
        logger.debug(f'Error getting column value: {e}')

    return None


def is_ssl_certificate_error(error):
    """
    Detect if an error is related to SSL certificate verification.

    Args:
        error: The exception object or error message string

    Returns
        bool: True if the error is related to SSL certificate verification
    """
    error_str = str(error).lower()
    return any(msg in error_str for msg in [
        'certificate verify failed',
        'ssl provider',
        'ssl security error',
        'certificate chain was issued by an authority that is not trusted',
        'encryption not supported on the server'
    ])


def get_sqlserver_error_code(error):
    """
    Extract SQL Server error code from exception message.

    Args:
        error: The exception object

    Returns
        tuple: (error_code, error_message) or (None, original_message)
    """
    error_str = str(error)
    # Try to match SQL Server error format: (code, [state] message)
    match = re.search(r'\((\d+)\)', error_str)
    if match:
        return match.group(1), error_str

    # Try to match ODBC format: [IM002] [Microsoft]...
    match = re.search(r'\[([A-Z0-9]+)\]', error_str)
    if match:
        return match.group(1), error_str

    return None, error_str


def adapt_sql_for_sqlserver(sql):
    """
    Apply minimal SQL adaptations for SQL Server compatibility.

    With ODBC Driver 18+, this function only needs to make essential changes:
    1. Handle special cases like @@identity and COUNT(*) that need explicit naming
    2. Convert PostgreSQL-style placeholders to SQL Server style
    3. Add aliases only for expression columns

    Args:
        sql: SQL query to adapt

    Returns
        SQL Server compatible query with minimal changes
    """
    if not sql or not isinstance(sql, str):
        return sql

    try:
        # Step 1: Handle special cases like @@identity
        sql = ensure_identity_column_named(sql)

        # Step 2: Convert PostgreSQL-style placeholders to SQL Server style
        sql = sql.replace('%s', '?')

        # Step 3: Add aliases only for expressions in SELECT queries
        if sql.upper().lstrip().startswith('SELECT'):
            sql = add_explicit_column_aliases(sql)

        return sql
    except Exception as e:
        logger.warning(f'Error adapting SQL for SQL Server: {e}')
        # Return original SQL if adaptation fails
        return sql


def process_sqlserver_result(cursor, columns=None):
    """
    Process SQL Server query results.

    Args:
        cursor: SQL Server cursor with results
        columns: Optional list of expected Column objects

    Returns
        List of dictionaries with processed results
    """
    from database.adapters.structure import RowStructureAdapter

    # If there's no description, there are no results to process
    if not cursor.description:
        return []

    # Try to fetch rows, handling the "No results" error
    try:
        # Log raw column information for debugging
        if cursor.description:
            logger.debug(f'SQL Server raw cursor description names: {[desc[0] for desc in cursor.description]}')

            # Log pyodbc row members if first row is available
            rows = cursor.fetchall()
            if rows and hasattr(rows[0], '__members__'):
                logger.debug(f'First row __members__: {rows[0].__members__}')
            # Reset cursor position by rebinding rows
            cursor._rows = rows
        else:
            rows = []
    except Exception as e:
        # Handle the specific "No results" error
        if 'No results' in str(e):
            logger.debug(f'No results to process: {e}')
            return []
        # Reraise other exceptions
        raise

    # If no rows were returned, return an empty list
    if not rows:
        return []

    # Ensure we have column information
    if columns is None:
        from database.operations.query import extract_column_info
        columns = extract_column_info(cursor)

    # Store expected column names on cursor for row adapters to use
    cursor.expected_column_names = [col.name for col in columns if col.name]
    logger.debug(f'Expected column names: {cursor.expected_column_names}')

    # Process rows with minimal transformation to preserve column names
    processed_rows = []

    for row in rows:
        try:
            # Create adapter with cursor context to access description
            adapter = RowStructureAdapter.create(cursor.connwrapper, row)
            adapter.cursor = cursor
            row_dict = adapter.to_dict()

            # Verify we got column names that match our expectations
            if row_dict and cursor.expected_column_names:
                # Clean any null bytes from expected column names too
                clean_expected = [name.replace('\x00', '') for name in cursor.expected_column_names]

                # Clean any null bytes from actual column names
                row_keys = list(row_dict.keys())
                clean_keys = [key.replace('\x00', '') for key in row_keys]

                # Log if column names don't match expectations
                if set(clean_keys) != set(clean_expected) and len(clean_keys) == len(clean_expected):
                    logger.warning(f'Column name mismatch - expected: {clean_expected}, actual: {row_keys}')

            processed_rows.append(row_dict)
        except Exception as e:
            logger.error(f'Error processing row: {e}')
            # Simple fallback for error cases using column names from description
            if hasattr(row, '__getitem__') and hasattr(row, '__len__') and cursor.description:
                fallback_dict = {}
                for i, desc in enumerate(cursor.description):
                    if i < len(row):
                        col_name = desc[0] or f'column_{i}'
                        fallback_dict[col_name] = row[i]
                processed_rows.append(fallback_dict)
            else:
                # Last resort fallback
                processed_rows.append({f'column_{i}': v for i, v in enumerate(row)})

    return processed_rows


def _handle_in_clause(query_text, params):
    """
    Handle IN clauses in SQL Server queries by expanding parameters.

    This function identifies IN clauses with placeholders and ensures the parameters
    are properly formatted for SQL Server.

    Args:
        query_text: The SQL query text
        params: The parameters to use

    Returns
        Tuple of (modified_query, expanded_params)
    """
    if not params or not isinstance(params, list | tuple) or not query_text:
        return query_text, params

    if 'IN' not in query_text.upper():
        return query_text, params

    # Look for 'IN (?)' patterns where a single parameter might represent multiple values
    in_clause_pattern = r'IN\s*\(\s*\?\s*\)'
    matches = list(re.finditer(in_clause_pattern, query_text, re.IGNORECASE))

    if not matches:
        return query_text, params

    # Track the number of params used thus far
    param_index = 0
    expanded_params = []
    modified_query = query_text

    # Adjust offset as we modify the string
    offset = 0

    for match in matches:
        # Current parameter corresponding to this ? placeholder
        if param_index >= len(params):
            logger.warning(f'More IN clause placeholders than parameters: {query_text}')
            break

        current_param = params[param_index]

        # Only process if the parameter is a list/tuple meant for an IN clause
        if isinstance(current_param, list | tuple):
            # Generate the appropriate number of placeholders
            if not current_param:  # Empty list case
                # Handle empty IN clause case specially
                placeholder_str = 'NULL'  # This ensures IN (NULL) never matches, which is the behavior we want
            else:
                placeholder_str = ', '.join(['?'] * len(current_param))

            # Replace the single ? with multiple ?s
            start = match.start() + offset
            end = match.end() + offset

            before = modified_query[:start]
            after = modified_query[end:]
            middle = f'IN ({placeholder_str})'
            modified_query = before + middle + after

            # Adjust offset for this replacement
            offset += len(middle) - (end - start)

            # Add each parameter value individually to the expanded params
            if current_param:  # Skip empty lists
                expanded_params.extend(current_param)
        else:
            # Keep regular parameters unchanged
            expanded_params.append(current_param)

        param_index += 1

    # Add any remaining parameters unchanged
    if param_index < len(params):
        expanded_params.extend(params[param_index:])

    return modified_query, expanded_params


def prepare_sqlserver_params(query_text, params):
    """
    Prepare parameters for SQL Server execution, handling special cases like IN clauses.

    This function should be called before executing a query with pyodbc to ensure
    parameters are properly formatted for SQL Server, especially for IN clauses.

    Args:
        query_text: SQL query text
        params: Query parameters

    Returns
        Tuple of (modified_query, expanded_params)
    """
    if not query_text or params is None:
        return query_text, params

    # Handle stored procedure calls with named parameters
    if 'EXEC ' in query_text.upper() and '@' in query_text:
        return _handle_stored_procedure_params(query_text, params)

    # Handle IN clause parameter expansion
    return _handle_in_clause(query_text, params)


def _handle_stored_procedure_params(query_text, params):
    """
    Handle named parameters in stored procedure calls.
    
    Converts a stored procedure call with named parameters to use ? placeholders.
    
    Args:
        query_text: Stored procedure call SQL text 
        params: Parameter values
    
    Returns:
        Tuple of (modified_query, expanded_params)
    """
    # Early return if no parameters
    if not params:
        return query_text, params

    # Special case for the syntax "EXEC proc @param ?" where the ? is already present
    if '?' in query_text and '@' in query_text:
        return _handle_parameterized_procedure(query_text, params)

    # For the format "EXEC proc @param" without ? placeholders
    return _handle_named_parameter_procedure(query_text, params)


def _handle_parameterized_procedure(query_text, params):
    """
    Handle stored procedure with parameters already using placeholders.
    
    Args:
        query_text: SQL text with procedure call 
        params: Parameter values
        
    Returns:
        Tuple of (query_text, flattened_params)
    """
    # Just pass the parameters directly without substitution
    flattened_params = []
    if len(params) == 1 and isinstance(params[0], (list, tuple)):
        # Handle single parameter that's a list/tuple
        flattened_params = list(params[0])
    else:
        # Convert all parameters to a flat list
        for param in params:
            if isinstance(param, (list, tuple)) and len(param) == 1:
                flattened_params.append(param[0])
            else:
                flattened_params.append(param)
    
    return query_text, flattened_params


def _handle_named_parameter_procedure(query_text, params):
    """
    Handle stored procedure with named parameters.
    
    Args:
        query_text: SQL text with named parameters
        params: Parameter values
        
    Returns:
        Tuple of (modified_query, flattened_params)
    """
    # Extract named parameters from query
    named_params = []
    param_pattern = r'@(\w+)'
    matches = re.finditer(param_pattern, query_text)
    
    for match in matches:
        named_params.append(match.group(0))  # Include the @ symbol
    
    # Early return if no parameters found
    if not named_params:
        return query_text, params
    
    # Unnest parameters to a flat list
    flattened_params = []
    if len(params) == 1 and isinstance(params[0], (list, tuple)):
        # Handle single parameter that's a list/tuple
        flattened_params = list(params[0])
    else:
        # Convert all parameters to a flat list
        for param in params:
            if isinstance(param, (list, tuple)) and len(param) == 1:
                # Unnest single-item lists/tuples
                flattened_params.append(param[0])
            else:
                flattened_params.append(param)
    
    # Replace named parameters with ? placeholders
    modified_query = query_text
    
    # Ensure we have enough parameters
    if len(named_params) > len(flattened_params):
        logger.warning(f"SQL has {len(named_params)} parameter names but only {len(flattened_params)} parameters were provided")
        # Pad with None values to match parameter count
        flattened_params.extend([None] * (len(named_params) - len(flattened_params)))
    elif len(named_params) < len(flattened_params):
        logger.warning(f"SQL has {len(named_params)} parameter names but {len(flattened_params)} parameters were provided - truncating")
        # Truncate params to match parameter count
        flattened_params = flattened_params[:len(named_params)]
    
    # Replace each parameter name with ?
    for param_name in named_params:
        modified_query = modified_query.replace(param_name, '?', 1)
    
    return modified_query, flattened_params
