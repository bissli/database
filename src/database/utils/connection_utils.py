"""
Database connection type utilities with no external dependencies.
"""


def is_psycopg_connection(obj):
    """Check if object is a psycopg connection or wrapper"""
    # Unwrap any wrappers by checking patterns
    if hasattr(obj, 'connection') and not hasattr(obj, 'execute'):
        obj = obj.connection

    # Check for transaction wrapper pattern
    if hasattr(obj, 'connection') and hasattr(obj, 'execute'):
        obj = obj.connection
        if hasattr(obj, 'connection'):
            obj = obj.connection

    # Check class name or module without importing the class
    # Direct instance check for psycopg.Connection
    if 'psycopg.Connection' in str(type(obj)):
        return True

    # Check via _spec_class attribute
    return (hasattr(obj, '_spec_class') and
            ('psycopg.Connection' in str(obj._spec_class) or
             'psycopg' in str(obj._spec_class)))


def is_pymssql_connection(obj):
    """Check if object is a pymssql connection or wrapper"""
    # Unwrap any wrappers by checking patterns
    if hasattr(obj, 'connection') and not hasattr(obj, 'execute'):
        obj = obj.connection

    # Check for transaction wrapper pattern
    if hasattr(obj, 'connection') and hasattr(obj, 'execute'):
        obj = obj.connection
        if hasattr(obj, 'connection'):
            obj = obj.connection

    # Check class name or module without importing the class
    # Direct instance check for pymssql.Connection
    if 'pymssql.Connection' in str(type(obj)):
        return True

    # Check via _spec_class attribute
    return (hasattr(obj, '_spec_class') and
            ('pymssql.Connection' in str(obj._spec_class) or
             'pymssql' in str(obj._spec_class)))


def is_sqlite3_connection(obj):
    """Check if object is a sqlite3 connection or wrapper"""
    # Unwrap any wrappers by checking patterns
    if hasattr(obj, 'connection') and not hasattr(obj, 'execute'):
        obj = obj.connection

    # Check for transaction wrapper pattern
    if hasattr(obj, 'connection') and hasattr(obj, 'execute'):
        obj = obj.connection
        if hasattr(obj, 'connection'):
            obj = obj.connection

    # Check class name or module without importing the class
    # Direct instance check for sqlite3.Connection
    if 'sqlite3.Connection' in str(type(obj)):
        return True

    # Check via _spec_class attribute
    return (hasattr(obj, '_spec_class') and
            ('sqlite3.Connection' in str(obj._spec_class) or
             'sqlite3' in str(obj._spec_class)))


def isconnection(obj):
    """Check if object is any supported database connection"""
    return (is_psycopg_connection(obj) or
            is_pymssql_connection(obj) or
            is_sqlite3_connection(obj))
