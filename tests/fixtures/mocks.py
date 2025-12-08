"""
Common mock objects and fixtures for database tests.

This module provides mock database connections for testing the database layer
without requiring actual database connections. These mocks are designed to simulate
the behavior of real database connections without requiring actual database servers,
making them ideal for unit testing.

It includes:
- Simple connection mocking utilities for type checking
- Comprehensive mock database connections for functional testing
- Mock transaction support

Usage examples:

    # Simple connection type mocking
    from tests.fixtures.mocks import create_mock_connection

    def test_connection_detection():
        pg_conn = create_mock_connection('postgresql')
        assert is_psycopg_connection(pg_conn) is True

    # Transaction mocking
    from tests.fixtures.mocks import create_mock_transaction

    def test_with_transaction():
        conn = create_mock_connection('postgresql')
        tx = create_mock_transaction(conn)
        assert is_psycopg_connection(tx) is True

    # Comprehensive PostgreSQL mock example
    def test_with_postgres(mock_postgres_conn):
        result = db.select(mock_postgres_conn, "SELECT * FROM users")
        assert len(result) > 0

    # SQLite mock example
    def test_with_sqlite(mock_sqlite_conn):
        db.execute(mock_sqlite_conn, "INSERT INTO users (name) VALUES (?)", "test_user")
        # Access last executed SQL for verification
        assert "INSERT INTO" in mock_sqlite_conn.cursor().last_sql

    # Transaction usage example
    def test_with_transaction(mock_postgres_conn):
        with db.transaction(mock_postgres_conn) as tx:
            tx.execute("INSERT INTO users (name) VALUES (%s)", "test_user")
            # Access transaction's last SQL
            assert "INSERT INTO" in tx.cursor.last_sql

    # Error simulation example
    def test_error_handling(mock_postgres_conn):
        # Configure the mock to raise a specific error
        mock_postgres_conn.trigger_error('unique_violation')
        with pytest.raises(db.IntegrityError):
            db.execute(mock_postgres_conn, "INSERT INTO users (name) VALUES (%s)", "test_user")

    # Connection failure testing example
    def test_reconnection(mock_postgres_conn):
        mock_postgres_conn.simulate_disconnect()
        @db.core.transaction.check_connection(max_retries=3)
        def operation(conn):
            return db.select_scalar(conn, "SELECT 1")
        # Will automatically reconnect
        assert operation(mock_postgres_conn) == 1

    # Enhanced column metadata example
    def test_with_complex_types(mock_postgres_conn_with_types):
        result = db.select(mock_postgres_conn_with_types, "SELECT data FROM json_table")
        # JSON is automatically parsed to dict
        assert isinstance(result[0]['data'], dict)

Each mock provides:
- Simulated connection and cursor objects
- Mocked transaction support (commit/rollback)
- Pre-configured return values for queries
- Connection type detection compatible with database module
- Helper attributes for inspecting calls (last_sql, last_params)
- Error simulation capabilities
- Connection failure and retry testing
- Enhanced column type support
- Multiple result sets for stored procedures
- Schema operations tracking
"""
import datetime
import json
import logging
from collections import UserDict
from unittest.mock import MagicMock, patch

import pytest

logger = logging.getLogger(__name__)

""" TODO: each mock should have one of these with the proper values set for the driver.

    # Mock dialect name detection to prevent recursion
    def mock_get_dialect_name(obj):
        if obj is my_mock_conn:
            return 'postgresql'  # or 'sqlite'
        return None

    mocker.patch('database.utils.connection_utils.get_dialect_name',
                side_effect=mock_get_dialect_name)
"""


def _create_simple_mock_connection(connection_type='postgresql'):
    """
    Create a simple mock database connection with the specified connection type.

    This is a lightweight mock specifically designed for connection type detection
    testing. For more comprehensive mocking, use the fixture-based mocks like
    mock_postgres_conn.

    Args:
        connection_type: Database type ('postgresql', 'sqlite', 'unknown')

    Returns
        Simple mock connection object that will pass type detection
    """
    # Create the base mock connection
    class MockConn:
        def __init__(self):
            pass

    conn = MockConn()

    # Define a dynamic class name to match what we want
    if connection_type == 'postgresql':
        # Creates a class that shows up as 'psycopg.Connection' in str(type(obj))
        conn.__class__.__module__ = 'psycopg'
        conn.__class__.__qualname__ = 'Connection'
        conn.__class__.__name__ = 'Connection'
    elif connection_type == 'sqlite':
        conn.__class__.__module__ = 'sqlite3'
        conn.__class__.__qualname__ = 'Connection'
        conn.__class__.__name__ = 'Connection'
    elif connection_type == 'unknown':
        # For testing with unrecognized connection types
        conn.__class__.__module__ = 'unknown_db'
        conn.__class__.__qualname__ = 'Connection'
        conn.__class__.__name__ = 'Connection'

    return conn


def _create_simple_mock_transaction(connection):
    """
    Create a simple mock transaction object that contains a connection reference.

    This is a lightweight mock specifically designed for connection type detection
    in transactions. The mock transaction contains a connection attribute, which
    allows the connection type detection to work correctly.

    Args:
        connection: A mock connection object created with create_mock_connection

    Returns
        Simple mock transaction with a connection attribute
    """
    # Simple object with just the connection attribute
    class MockTransaction:
        def __init__(self, conn):
            self.connection = conn

    return MockTransaction(connection)


@pytest.fixture
def simple_mock_postgresql_connection():
    """
    Fixture that provides a simple PostgreSQL mock connection.

    This fixture is designed for testing connection type detection. For more
    comprehensive mocks, use mock_postgres_conn.

    Returns
        Simple mock PostgreSQL connection object
    """
    return _create_simple_mock_connection('postgresql')


@pytest.fixture
def simple_mock_sqlite_connection():
    """
    Fixture that provides a simple SQLite mock connection.

    This fixture is designed for testing connection type detection. For more
    comprehensive mocks, use mock_sqlite_conn.

    Returns
        Simple mock SQLite connection object
    """
    return _create_simple_mock_connection('sqlite')


@pytest.fixture
def simple_mock_unknown_connection():
    """
    Fixture that provides a simple unknown type mock connection.

    This fixture is designed for testing connection type detection with
    unrecognized connection types.

    Returns
        Simple mock connection object of unknown type
    """
    return _create_simple_mock_connection('unknown')


@pytest.fixture
def simple_mock_transaction_factory():
    """
    Fixture that provides a factory function to create simple mock transactions.

    This fixture is designed for testing transaction-related functions that need
    to detect the underlying connection type through a transaction object.

    Returns
        Factory function that creates mock transactions from connections
    """
    def factory(connection):
        return _create_simple_mock_transaction(connection)
    return factory


@pytest.fixture
def create_simple_mock_connection():
    """
    Fixture that provides a factory function to create simple mock connections.

    This fixture is designed for testing connection type detection. For more
    comprehensive mocks, use mock_postgres_conn or mock_sqlite_conn.

    Returns
        Factory function that creates mock connections of specified type

    Example usage:
        def test_connection_detection(create_simple_mock_connection):
            # Create connections of different types
            pg_conn = create_simple_mock_connection('postgresql')
            sqlite_conn = create_simple_mock_connection('sqlite')
            unknown_conn = create_simple_mock_connection('unknown')

            # Test type detection functions
            assert is_psycopg_connection(pg_conn) is True
            assert is_sqlite3_connection(sqlite_conn) is True
            assert is_psycopg_connection(unknown_conn) is False
            assert is_sqlite3_connection(unknown_conn) is False
    """
    def factory(connection_type='postgresql'):
        return _create_simple_mock_connection(connection_type)

    return factory


@pytest.fixture
def create_simple_mock_transaction():
    """
    Fixture that provides a factory function to create simple mock transactions.

    This fixture is designed for testing transaction-related functions that need
    to detect the underlying connection type through a transaction object.

    Returns
        Factory function that creates mock transactions with specified connection

    Example usage:
        def test_transaction_detection(create_simple_mock_connection, create_simple_mock_transaction):
            # Create a mock connection
            pg_conn = create_simple_mock_connection('postgresql')

            # Create a transaction that wraps the connection
            tx = create_simple_mock_transaction(pg_conn)

            # Test detection through the transaction wrapper
            assert is_psycopg_connection(tx) is True
    """
    def factory(connection):
        return _create_simple_mock_transaction(connection)

    return factory


# Common helper functions for all mocks
def _setup_mock_conn(conn_type, type_str):
    """
    Create a basic mock connection with common settings.

    This helper configures a mock database connection with all the essential attributes
    and methods required by the database module. It ensures that type detection will
    work correctly and that common operations like commit and rollback are available.

    Args:
        conn_type: Database type ('postgresql', 'sqlite')
        type_str: String representation of the connection type for detection
                  (e.g., 'psycopg.Connection', 'sqlite3.Connection')

    Returns
        Configured mock connection object
    """
    mock_conn = MagicMock()
    conn_mock = MagicMock()

    # Set essential methods needed for testing
    essential_methods = ['cursor', 'commit', 'rollback', 'close']
    for method_name in essential_methods:
        setattr(conn_mock, method_name, MagicMock())

    # Add common exception types
    common_exceptions = [
        'DataError', 'IntegrityError', 'OperationalError', 'ProgrammingError',
        'DatabaseError', 'Error', 'InterfaceError'
    ]
    for exc_name in common_exceptions:
        setattr(conn_mock, exc_name, type(exc_name, (Exception,), {}))

    mock_conn.connection = conn_mock

    # Set string representation for type detection
    mock_conn.connection.__str__ = MagicMock(return_value=f"<class '{type_str}'>")

    # Configure driver type
    mock_conn.driver_type = conn_type  # Used by newer code
    mock_conn._driver_type = conn_type  # For property support (legacy code)

    # Set database-specific autocommit properties
    if conn_type == 'postgresql':
        conn_mock.autocommit = False
    elif conn_type == 'sqlite':
        conn_mock.isolation_level = 'DEFERRED'

    # Add SQLAlchemy-specific attributes needed for auto-commit testing
    mock_conn.sa_connection = MagicMock()
    mock_conn.sa_connection.connection = conn_mock
    mock_conn.sa_connection.execution_options = MagicMock()

    # Add transaction tracking attribute
    mock_conn.in_transaction = False

    return mock_conn


def _create_enhanced_column_info(name, python_type, type_code=None, **kwargs):
    """
    Create a more detailed column info mock.

    This function creates a mock Column object with detailed type information and
    metadata, which can be used for testing with complex types and column-specific behaviors.

    Args:
        name: Column name
        python_type: Python type (e.g., int, str, dict)
        type_code: Database-specific type code
        **kwargs: Additional column attributes (display_size, internal_size, etc.)

    Returns
        Mock Column object with detailed metadata
    """
    column = MagicMock(name=name)
    column.name = name
    column.python_type = python_type
    column.type_code = type_code

    # Set additional attributes
    for key, value in kwargs.items():
        setattr(column, key, value)

    # Add to_dict method that includes all attributes
    def to_dict():
        result = {
            'name': name,
            'python_type': python_type.__name__ if python_type else None,
            'type_code': type_code,
        }
        result.update(dict(kwargs.items()))
        return result

    column.to_dict = to_dict
    return column


def _setup_enhanced_column_info(column_specs):
    """
    Configure richer column information.

    Args:
        column_specs: List of dictionaries with column specifications

    Returns
        List of mock Column objects
    """
    columns = [
        _create_enhanced_column_info(**spec)
        for spec in column_specs
    ]

    return columns


def _setup_cursor(mock_conn, description, result_values=None):
    """
    Setup a cursor with common functionality.

    Configures a mock cursor that simulates query execution and result fetching.
    The cursor includes special attributes for tracking executed SQL and parameters
    to help with test assertions.

    Args:
        mock_conn: Mock connection to attach the cursor to
        description: Column descriptions for the cursor, similar to cursor.description
                     in format [(column_name, type_code, display_size, internal_size,
                     precision, scale, null_ok), ...]
        result_values: Default result values to return from fetch methods,
                      defaults to [('value1', 'value2')]

    Returns
        Configured mock cursor object

    Usage in tests:
        # Access the last executed SQL statement
        assert "SELECT" in mock_conn.cursor().last_sql
        # Access parameters passed to the last query
        assert mock_conn.cursor().last_params == (1, 'test')
    """
    cursor = MagicMock()

    result_values = result_values or [('value1', 'value2')]

    # Core cursor methods
    cursor.execute = MagicMock()
    cursor.executemany = MagicMock()
    cursor.fetchall = MagicMock(return_value=result_values)
    cursor.fetchone = MagicMock(return_value=result_values[0] if result_values else None)
    cursor.fetchmany = MagicMock(return_value=result_values)
    cursor.description = description
    cursor.rowcount = 1
    cursor.nextset = MagicMock(return_value=None)

    # Store SQL and params for test inspection
    def mock_execute(sql, params=None):
        cursor.last_sql = sql
        cursor.last_params = params
        return cursor

    cursor.execute.side_effect = mock_execute

    # Make cursor() function return this cursor directly
    # This ensures both cursor() and cursor.return_value work in tests
    def cursor_func(*args, **kwargs):
        return cursor

    mock_conn.cursor = cursor_func
    mock_conn.cursor.return_value = cursor

    return cursor


def _setup_multiple_resultsets(cursor, result_sets):
    """Configure cursor to simulate multiple result sets.

    This function configures a mock cursor to simulate stored procedures that
    return multiple result sets, which is especially useful for testing stored
    procedure functionality.

    Args:
        cursor: The mock cursor to configure
        result_sets: List of dictionaries, each with 'description' and 'data' keys

    Returns
        Function to update result sets during tests
    """
    # Store original implementations
    original_fetchall = cursor.fetchall
    original_fetchone = cursor.fetchone

    # Track the current result set
    current_resultset = [0]

    def mock_nextset():
        """Simulate moving to the next result set"""
        current_resultset[0] += 1
        if current_resultset[0] < len(result_sets):
            # Update the cursor's description to match the new result set
            cursor.description = result_sets[current_resultset[0]]['description']
            return True
        return None

    def mock_fetchall():
        """Return the current result set data"""
        if 0 <= current_resultset[0] < len(result_sets):
            return result_sets[current_resultset[0]]['data']
        return []

    def mock_fetchone():
        """Return the first row of current result set"""
        if 0 <= current_resultset[0] < len(result_sets) and result_sets[current_resultset[0]]['data']:
            return result_sets[current_resultset[0]]['data'][0]
        return None

    def reset_resultsets():
        """Reset to the first result set"""
        current_resultset[0] = 0
        if result_sets:
            cursor.description = result_sets[0]['description']

    # Set initial description
    if result_sets:
        cursor.description = result_sets[0]['description']

    # Override cursor methods
    cursor.nextset.side_effect = mock_nextset
    cursor.fetchall.side_effect = mock_fetchall
    cursor.fetchone.side_effect = mock_fetchone
    cursor.reset_resultsets = reset_resultsets

    # Return a function to change result sets
    def set_result_sets(new_result_sets):
        nonlocal result_sets
        result_sets = new_result_sets
        reset_resultsets()

    return set_result_sets


def _setup_error_simulation(mock_conn, db_type):
    """
    Setup error simulation capabilities for mock connection.

    This function adds the ability to trigger specific database errors on demand,
    which is useful for testing error handling code.

    Args:
        mock_conn: The mock connection
        db_type: Database type ('postgresql', 'sqlite')

    Returns
        Dictionary of error classes created for this database type
    """

    # Create error types specific to the database
    if db_type == 'postgresql':
        error_classes = {
            'unique_violation': type('UniqueViolation', (Exception,), {}),
            'operation_timeout': type('OperationalError', (Exception,), {'__str__': lambda s: 'SSL connection has been closed unexpectedly'}),
            'connection_closed': type('InterfaceError', (Exception,), {'__str__': lambda s: 'connection already closed'}),
            'foreign_key_violation': type('IntegrityError', (Exception,), {'__str__': lambda s: 'violates foreign key constraint'}),
            'syntax_error': type('ProgrammingError', (Exception,), {'__str__': lambda s: 'syntax error at or near'}),
        }
    elif db_type == 'sqlite':
        error_classes = {
            'unique_violation': type('IntegrityError', (Exception,), {'__str__': lambda s: 'UNIQUE constraint failed'}),
            'operation_timeout': type('OperationalError', (Exception,), {'__str__': lambda s: 'database is locked'}),
            'connection_closed': type('ProgrammingError', (Exception,), {'__str__': lambda s: 'Cannot operate on a closed database'}),
            'foreign_key_violation': type('IntegrityError', (Exception,), {'__str__': lambda s: 'FOREIGN KEY constraint failed'}),
            'syntax_error': type('OperationalError', (Exception,), {'__str__': lambda s: 'near syntax error'}),
        }

    # Function to trigger specific error types
    def trigger_error(error_type, after_n_calls=0):
        """
        Configure the mock to raise a specific error

        Args:
            error_type: Type of error to simulate ('unique_violation', 'connection_closed', etc.)
            after_n_calls: Number of successful calls before error is triggered (0 = next call)

        Returns
            The configured exception that will be raised
        """
        if error_type not in error_classes:
            raise ValueError(f'Unknown error type: {error_type}. Available: {list(error_classes.keys())}')

        error = error_classes[error_type]()

        # Create a function that returns the cursor with error simulation
        def get_cursor_with_error(*args, **kwargs):
            cursor = mock_conn.cursor()
            # Store original side effect
            original_side_effect = getattr(cursor.execute, 'side_effect', None)

            call_count = [0]  # Use list to allow modification in closure

            def execute_with_error(sql, params=None):
                call_count[0] += 1
                if call_count[0] > after_n_calls:
                    # Reset side effect to avoid infinite errors
                    cursor.execute.side_effect = original_side_effect
                    cursor.last_sql = sql
                    cursor.last_params = params
                    raise error

                # Call original execution
                cursor.last_sql = sql
                cursor.last_params = params
                if callable(original_side_effect):
                    return original_side_effect(sql, params)
                return cursor

            cursor.execute.side_effect = execute_with_error
            return cursor

        # Replace the cursor method to include error simulation
        mock_conn.cursor = get_cursor_with_error
        return error

    mock_conn.trigger_error = trigger_error
    mock_conn.error_types = list(error_classes.keys())

    return error_classes


def _setup_reconnection_testing(mock_conn, error_classes):
    """
    Setup connection failure and retry simulation.

    This function configures a mock connection to simulate disconnection and
    reconnection scenarios, which is useful for testing the check_connection decorator.

    Args:
        mock_conn: The mock connection
        error_classes: Dictionary of error classes to use

    Returns
        None
    """

    # Track connection state
    connection_state = {
        'is_connected': True,
        'failure_count': 0,
        'max_failures': 0,
        'reconnect_after': 0
    }

    # Store original connection object
    original_connection = mock_conn.connection

    # Create a proxy connection that we can disconnect
    proxy_connection = MagicMock()
    for attr_name in dir(original_connection):
        if not attr_name.startswith('_'):
            setattr(proxy_connection, attr_name, getattr(original_connection, attr_name))

    # Override cursor method to check connection state
    original_cursor = mock_conn.cursor

    def cursor_with_connection_check(*args, **kwargs):
        if not connection_state['is_connected']:
            connection_state['failure_count'] += 1

            # Check if we should reconnect
            if connection_state['failure_count'] >= connection_state['reconnect_after']:
                connection_state['is_connected'] = True
                connection_state['failure_count'] = 0
                mock_conn.connection = original_connection
                logger.debug('Mock connection automatically reconnected')
                return original_cursor()

            # Otherwise, raise connection error
            if connection_state['failure_count'] <= connection_state['max_failures']:
                raise error_classes['connection_closed']

        return original_cursor()

    mock_conn.cursor = cursor_with_connection_check

    # Methods to control connection state
    def simulate_disconnect():
        """Simulate a disconnection that will cause errors on next query"""
        connection_state['is_connected'] = False
        mock_conn.connection = proxy_connection
        logger.debug('Mock connection disconnected')

    def configure_reconnect_behavior(max_failures=2, reconnect_after=None):
        """Configure how the connection behaves during reconnection tests"""
        connection_state['max_failures'] = max_failures
        connection_state['reconnect_after'] = reconnect_after if reconnect_after is not None else max_failures

    def simulate_reconnect():
        """Force reconnection"""
        connection_state['is_connected'] = True
        connection_state['failure_count'] = 0
        mock_conn.connection = original_connection
        logger.debug('Mock connection manually reconnected')

    # Add methods to the mock connection
    mock_conn.simulate_disconnect = simulate_disconnect
    mock_conn.configure_reconnect_behavior = configure_reconnect_behavior
    mock_conn.simulate_reconnect = simulate_reconnect

    # For convenience in tests
    mock_conn.connection_state = connection_state


def _setup_special_data_types(mock_conn, db_type):
    """
    Configure support for special data types.

    This function adds support for testing with special data types like JSON, arrays,
    UUIDs, and other complex types typically used in database applications.

    Args:
        mock_conn: The mock connection
        db_type: Database type ('postgresql', 'sqlite')

    Returns
        None
    """
    import uuid

    # Define type conversions
    type_converters = {}

    # Add JSON support
    if db_type == 'postgresql':
        # PostgreSQL JSON type (114 for json, 3802 for jsonb)
        json_types = {114, 3802}

        def json_result_processor(value):
            if value is None:
                return None
            if isinstance(value, str):
                return json.loads(value)
            return value

        type_converters['json'] = (json_types, json_result_processor)

    # Add UUID support
    if db_type == 'postgresql':
        # UUID type (2950 for PostgreSQL)
        uuid_types = {2950}

        def uuid_result_processor(value):
            if value is None:
                return None
            if isinstance(value, str):
                return uuid.UUID(value)
            return value

        type_converters['uuid'] = (uuid_types, uuid_result_processor)

    # Add Array support for PostgreSQL
    if db_type == 'postgresql':
        # Array types - map base type to array type
        array_types = {
            23: 1007,  # int[]
            25: 1009,  # text[]
            16: 1000,  # bool[]
            700: 1021,  # float[]
            701: 1022,  # double[]
            1114: 1115,  # timestamp[]
        }

        def array_result_processor(value):
            if value is None:
                return None
            if isinstance(value, str) and value.startswith('{') and value.endswith('}'):
                # Simple parser for PostgreSQL array format {a,b,c}
                content = value[1:-1]
                return [x.strip('"\'') for x in content.split(',')]
            return value

        type_converters['array'] = (set(array_types.values()), array_result_processor)

    # Add timestamp with time zone support
    if db_type == 'postgresql':
        # Timestamptz type (1184 for PostgreSQL)
        timestamp_types = {1184}

        def timestamptz_result_processor(value):
            if value is None:
                return None
            if isinstance(value, str):
                try:
                    from dateutil import parser
                    return parser.parse(value)
                except:
                    return value
            return value

        type_converters['timestamptz'] = (timestamp_types, timestamptz_result_processor)

    # Configure result conversion based on column type
    def convert_result_by_type(column, value):
        """Convert value based on column type"""
        if value is None:
            return None

        column_type = getattr(column, 'type_code', None)
        if column_type is None:
            return value

        # Check each type converter
        for (type_codes, processor) in type_converters.values():
            if column_type in type_codes:
                return processor(value)

        return value

    # Add type conversion to mock_conn
    mock_conn.convert_result_by_type = convert_result_by_type
    mock_conn.special_types = list(type_converters.keys())


def _setup_schema_operations_tracking(mock_conn):
    """
    Setup tracking of schema operations.

    This function adds tracking for schema-related functions like vacuum,
    reindex, etc., which is useful for testing schema maintenance operations.

    Args:
        mock_conn: The mock connection

    Returns
        List of patchers created
    """
    schema_ops = {
        'vacuum': [],
        'reindex': [],
        'cluster': [],
        'reset_sequence': [],
        'primary_keys': {},  # Dict of table -> [key columns]
        'sequences': {},     # Dict of table -> [sequence columns]
        'table_columns': {},  # Dict of table -> [columns]
    }

    # Patch schema operations
    patchers = []

    # Add strategy getter patch
    strategy_mock = MagicMock()

    # Configure vacuum operations
    def mock_vacuum(cn, table):
        schema_ops['vacuum'].append(table)
    strategy_mock.vacuum_table.side_effect = mock_vacuum

    # Configure reindex operations
    def mock_reindex(cn, table):
        schema_ops['reindex'].append(table)
    strategy_mock.reindex_table.side_effect = mock_reindex

    # Configure cluster operations
    def mock_cluster(cn, table, index=None):
        schema_ops['cluster'].append((table, index))
    strategy_mock.cluster_table.side_effect = mock_cluster

    # Configure reset_sequence operations
    def mock_reset_sequence(cn, table, identity=None):
        schema_ops['reset_sequence'].append((table, identity))
    strategy_mock.reset_sequence.side_effect = mock_reset_sequence

    # Configure get_primary_keys
    def mock_get_primary_keys(cn, table):
        return schema_ops['primary_keys'].get(table, [])
    strategy_mock.get_primary_keys.side_effect = mock_get_primary_keys

    # Configure get_sequence_columns
    def mock_get_sequence_columns(cn, table):
        return schema_ops['sequences'].get(table, [])
    strategy_mock.get_sequence_columns.side_effect = mock_get_sequence_columns

    # Configure get_columns
    def mock_get_columns(cn, table):
        return schema_ops['table_columns'].get(table, [])
    strategy_mock.get_columns.side_effect = mock_get_columns

    # Configure quote_identifier
    def mock_quote_identifier(identifier, dialect):
        return f'"{identifier}"'
    strategy_mock.quote_identifier.side_effect = mock_quote_identifier

    # Patch the strategy module
    get_strategy_patcher = patch('database.strategy.get_db_strategy', return_value=strategy_mock)
    patchers.append(get_strategy_patcher)

    # Helper functions to configure schema info
    def set_primary_keys(table, keys):
        schema_ops['primary_keys'][table] = keys

    def set_sequence_columns(table, columns):
        schema_ops['sequences'][table] = columns

    def set_table_columns(table, columns):
        schema_ops['table_columns'][table] = columns

    # Add helpers to the mock connection
    mock_conn.set_primary_keys = set_primary_keys
    mock_conn.set_sequence_columns = set_sequence_columns
    mock_conn.set_table_columns = set_table_columns

    # Expose operations tracking
    mock_conn.schema_ops = schema_ops

    # Start patchers
    for p in patchers:
        p.start()

    # Return patchers to be stopped
    return patchers


def _setup_connection_pool_testing(mock_conn_factory, pool_size=5):
    """
    Create a connection pool mock for testing.

    This function sets up mocking for the connection pool, allowing tests
    to verify pool behavior without requiring actual database connections.

    Args:
        mock_conn_factory: Function that creates mock connections
        pool_size: Size of the mock connection pool

    Returns
        Tuple of (pool_mock, pool_patcher)
    """
    # Create a pool of mock connections
    pool_connections = [mock_conn_factory() for _ in range(pool_size)]

    # Keep track of connections in use
    conn_status = {id(conn): {'in_use': False, 'conn': conn} for conn in pool_connections}

    # Create pool mock
    pool_mock = MagicMock()

    # Add adapter_registry mock to each connection
    with patch('database.adapter_registry'):
        def get_connection():
            """Get an available connection from the pool"""
            for status in conn_status.values():
                if not status['in_use']:
                    status['in_use'] = True
                    return status['conn']
            raise RuntimeError('Pool exhausted')

        def release_connection(conn):
            """Return a connection to the pool"""
            conn_id = id(conn)
            if conn_id in conn_status:
                conn_status[conn_id]['in_use'] = False

        def close_all():
            """Close all connections"""
            for status in conn_status.values():
                status['in_use'] = False

        # Assign methods to mock
        pool_mock.get_connection.side_effect = get_connection
        pool_mock.release_connection.side_effect = release_connection
        pool_mock.close_all.side_effect = close_all

        # Expose connection status for inspection
        pool_mock.conn_status = conn_status

        # Patch the connection pool creation
        pool_patcher = patch('database.core.connection.ConnectionPool', return_value=pool_mock)
        pool_patcher.start()

        return pool_mock, pool_patcher


def _setup_transaction(mock_conn, cursor):
    """
    Configure transaction support for a mock connection.

    Sets up the necessary methods and behavior to simulate transactions using
    the database module's Transaction class. This includes configuring the
    connection to create mock transaction objects that track their state and
    simulate commit/rollback behavior.

    Args:
        mock_conn: The mock connection to add transaction support to
        cursor: The mock cursor to use within transactions

    Returns
        A function that creates transaction mocks for this connection
    """
    # Create a transaction factory function
    def create_transaction():
        # Create a mock transaction object
        tx = MagicMock()
        tx.connection = mock_conn.connection
        tx.cursor = cursor

        # Add common transaction methods
        tx.execute = MagicMock()
        tx.select = MagicMock()
        tx.select_column = MagicMock()
        tx.select_row = MagicMock()
        tx.select_row_or_none = MagicMock()
        tx.select_scalar = MagicMock()

        # Setup query execution within transaction
        def tx_execute(sql, *args, **kwargs):
            cursor.execute(sql, *args)
            return cursor.rowcount

        tx.execute.side_effect = tx_execute

        # Setup select within transaction
        def tx_select(sql, *args, **kwargs):
            cursor.execute(sql, *args)
            return mock_conn.options.data_loader(
                cursor.fetchall(),
                [c[0] for c in cursor.description],
                **kwargs
            )

        tx.select.side_effect = tx_select

        # Setup select_column within transaction
        def tx_select_column(sql, *args):
            result = tx_select(sql, *args)
            return [row.get(list(row.keys())[0]) if row else None for row in result]

        tx.select_column.side_effect = tx_select_column

        # Setup select_row and related methods
        def tx_select_row(sql, *args):
            result = tx_select(sql, *args)
            assert len(result) == 1, f'Expected one row, got {len(result)}'
            return result[0]

        tx.select_row.side_effect = tx_select_row

        def tx_select_row_or_none(sql, *args):
            result = tx_select(sql, *args)
            if not result or len(result) == 0:
                return None
            return result[0]

        tx.select_row_or_none.side_effect = tx_select_row_or_none

        def tx_select_scalar(sql, *args):
            result = tx_select(sql, *args)
            assert len(result) == 1, f'Expected one row, got {len(result)}'
            return list(result[0].values())[0] if result[0] else None

        tx.select_scalar.side_effect = tx_select_scalar

        # Add context manager behavior
        tx.__enter__ = MagicMock(return_value=tx)
        tx.__exit__ = MagicMock()

        # Setup __exit__ behavior to handle exceptions
        def exit_handler(exc_type, exc_val, exc_tb):
            if exc_type is not None:
                mock_conn.connection.rollback.assert_called_once()
            else:
                mock_conn.connection.commit.assert_called_once()
            return False  # Don't suppress exceptions

        tx.__exit__.side_effect = exit_handler

        return tx

    # Add transaction creation method to mock connection
    mock_conn.create_transaction = create_transaction

    # Add patch for the transaction module
    return patch('database.core.transaction.Transaction', side_effect=create_transaction)


def _setup_data_loader(mock_conn, driver_name):
    """
    Setup a data loader for the mock connection.

    Configures a simple data loader that converts database results into the format
    expected by the database module. This allows the mocks to return data in a
    consistent format regardless of the database driver.

    Args:
        mock_conn: Mock connection to configure
        driver_name: Database driver name for options ('postgresql', 'sqlite')

    Notes
        - The data loader converts tuples to dictionaries using column names
        - It handles various result formats including dicts, tuples, and scalar values
        - The loader mimics the behavior of the real data loaders in database.options
    """
    mock_options = MagicMock()
    mock_options.drivername = driver_name

    # Generic data loader that works across all drivers
    def mock_data_loader(data, cols, **kwargs):
        result = []
        if not data:
            return result

        for row in data:
            if isinstance(row, dict):
                result.append(row)
            elif isinstance(row, tuple) and cols:
                result.append({cols[i]: val for i, val in enumerate(row) if i < len(cols)})
            else:
                result.append({cols[0] if cols else 'value': row})
        return result

    mock_options.data_loader = mock_data_loader
    mock_conn.options = mock_options


@pytest.fixture
def mock_postgres_conn():
    """
    Create a mock PostgreSQL connection for testing.

    This fixture provides a fully configured mock PostgreSQL connection that can be
    used for testing database code without requiring an actual PostgreSQL server.
    The mock includes pre-configured responses for common operations and patches
    the necessary functions to ensure type detection works correctly.

    Returns
        Mock PostgreSQL connection object

    Example usage:
        def test_postgres_query(mock_postgres_conn):
            # Test a SELECT query
            result = db.select(mock_postgres_conn, "SELECT * FROM users WHERE id = %s", 1)
            assert len(result) > 0

            # Verify the executed SQL
            assert "SELECT * FROM users" in mock_postgres_conn.cursor().last_sql
            assert mock_postgres_conn.cursor().last_params == 1

            # Test an INSERT statement
            db.execute(mock_postgres_conn, "INSERT INTO users (name) VALUES (%s)", "test_user")
            assert mock_postgres_conn.cursor().statusmessage == 'INSERT 0 1'

        def test_postgres_transaction(mock_postgres_conn):
            # Test transaction rollback
            with db.transaction(mock_postgres_conn) as tx:
                tx.execute("DELETE FROM users")
                # Transaction will be rolled back (no changes applied)
                raise Exception("Force rollback")

            # Verify rollback was called
            mock_postgres_conn.connection.rollback.assert_called_once()
    """
    mock_conn = _setup_mock_conn('postgresql', 'psycopg.Connection')

    # Add PostgreSQL-specific error types
    class PostgresErrors:
        class UniqueViolation(Exception):
            pass

    mock_conn.connection.errors = PostgresErrors

    # Setup cursor with PostgreSQL-specific fields
    # Type codes match actual PostgreSQL OIDs:
    # 23 = int4 (integer), 25 = text
    cursor = _setup_cursor(
        mock_conn,
        description=[
            ('id', 23, None, None, None, None, None),  # 23=int4
            ('name', 25, None, None, None, None, None)  # 25=text
        ]
    )

    # Add PostgreSQL specific cursor methods
    cursor.mogrify = MagicMock(side_effect=lambda sql, params=None: f'MOGRIFIED: {sql}')
    cursor.statusmessage = 'INSERT 0 1'  # Status returned by PostgreSQL after INSERT

    # Setup data loader
    _setup_data_loader(mock_conn, 'postgresql')

    # Setup error simulation
    error_classes = _setup_error_simulation(mock_conn, 'postgresql')

    # Setup reconnection testing
    _setup_reconnection_testing(mock_conn, error_classes)

    # Setup special data type support
    _setup_special_data_types(mock_conn, 'postgresql')

    # Setup schema operations tracking
    schema_patchers = _setup_schema_operations_tracking(mock_conn)

    # Setup transaction support
    transaction_patcher = _setup_transaction(mock_conn, cursor)

    # Apply common patches to make test isolation clean
    patchers = [transaction_patcher] + schema_patchers

    # Connection type detection patches
    # These ensure that get_dialect_name() returns 'postgresql' for our mock
    patchers.append(patch(
        'database.utils.connection_utils.get_dialect_name',
        side_effect=lambda obj: 'postgresql' if (
            obj is mock_conn or
            obj is mock_conn.connection or
            (hasattr(obj, 'connection') and obj.connection is mock_conn.connection) or
            (hasattr(obj, 'driver_connection') and (
                obj.driver_connection is mock_conn or
                obj.driver_connection is mock_conn.connection
            ))
        ) else None
    ))
    patchers.append(patch(
        'database.utils.connection_utils.isconnection',
        side_effect=lambda x: x is mock_conn or patchers[0].get_original()[0](x)
    ))

    # SQL handling patches
    patchers.append(patch(
        'database.sql.quote_identifier',
        side_effect=lambda ident, dialect: f'"{ident}"'
    ))
    patchers.append(patch(
        'database.sql.standardize_placeholders',
        side_effect=lambda sql, dialect: sql.replace('?', '%s') if dialect == 'postgresql' else sql
    ))
    patchers.append(patch(
        'database.operations.query.load_data',
        side_effect=lambda cursor, **kwargs: mock_conn.options.data_loader(
            cursor.fetchall(),
            [c[0] for c in cursor.description],
            **kwargs
        )
    ))
    patchers.append(patch(
        'database.operations.query.extract_column_info',
        return_value=[
            MagicMock(name='id', python_type=int, to_dict=lambda: {'name': 'id', 'python_type': 'int'}),
            MagicMock(name='name', python_type=str, to_dict=lambda: {'name': 'name', 'python_type': 'str'})
        ]
    ))

    # Start all patchers
    for p in patchers:
        p.start()

    yield mock_conn

    # Stop all patchers
    for p in patchers:
        p.stop()


@pytest.fixture
def mock_postgres_conn_with_types():
    """
    Create a mock PostgreSQL connection with enhanced type support.

    This fixture extends the basic postgres mock with enhanced column type information
    and special data type handling, making it suitable for testing complex data types.

    Returns
        Enhanced mock PostgreSQL connection object with type support

    Example usage:
        def test_json_data(mock_postgres_conn_with_types):
            # Setup JSON test data
            mock_postgres_conn_with_types.cursor().fetchall.return_value = [
                {'id': 1, 'data': '{"name": "John", "age": 30}'}
            ]

            # Query with JSON data
            result = db.select(mock_postgres_conn_with_types,
                              "SELECT id, data FROM users")

            # JSON is automatically parsed
            assert isinstance(result[0]['data'], dict)
            assert result[0]['data']['name'] == 'John'
    """
    mock_conn = _setup_mock_conn('postgresql', 'psycopg.Connection')

    # Define column specs with rich type information
    column_specs = [
        {'name': 'id', 'python_type': int, 'type_code': 23, 'display_size': None,
         'internal_size': 4, 'precision': None, 'scale': None, 'nullable': False},
        {'name': 'name', 'python_type': str, 'type_code': 25, 'display_size': None,
         'internal_size': -1, 'precision': None, 'scale': None, 'nullable': True},
        {'name': 'data', 'python_type': dict, 'type_code': 114, 'display_size': None,
         'internal_size': -1, 'precision': None, 'scale': None, 'nullable': True},
        {'name': 'tags', 'python_type': list, 'type_code': 1009, 'display_size': None,
         'internal_size': -1, 'precision': None, 'scale': None, 'nullable': True},
        {'name': 'created_at', 'python_type': datetime.datetime, 'type_code': 1114,
         'display_size': None, 'internal_size': 8, 'precision': None, 'scale': None, 'nullable': True},
        {'name': 'uuid', 'python_type': str, 'type_code': 2950, 'display_size': None,
         'internal_size': 16, 'precision': None, 'scale': None, 'nullable': True},
    ]

    # Create enhanced column info
    columns = _setup_enhanced_column_info(column_specs)

    # Create description matching the type codes
    description = [(spec['name'], spec['type_code'], spec['display_size'],
                   spec['internal_size'], spec['precision'], spec['scale'],
                   spec['nullable']) for spec in column_specs]

    # Setup cursor with type information
    cursor = _setup_cursor(mock_conn, description)

    # Sample JSON data results
    json_data = [
        {'id': 1, 'name': 'Test 1', 'data': '{"key": "value", "num": 42}',
         'tags': '{tag1,tag2,tag3}', 'created_at': '2023-01-01 12:00:00',
         'uuid': 'f47ac10b-58cc-4372-a567-0e02b2c3d479'},
        {'id': 2, 'name': 'Test 2', 'data': '{"key": "value2", "num": 43}',
         'tags': '{tag2,tag3}', 'created_at': '2023-01-02 14:30:00',
         'uuid': '550e8400-e29b-41d4-a716-446655440000'}
    ]

    # Setup special data type support
    _setup_special_data_types(mock_conn, 'postgresql')

    # Setup data loader
    _setup_data_loader(mock_conn, 'postgresql')

    # Configure enhanced fetchall with type conversion
    def enhanced_fetchall():
        results = []
        for row in json_data:
            converted_row = {}
            for col_name, col_value in row.items():
                # Find the column info
                col_info = next((c for c in columns if c.name == col_name), None)
                if col_info:
                    # Convert the value
                    converted_row[col_name] = mock_conn.convert_result_by_type(col_info, col_value)
                else:
                    converted_row[col_name] = col_value
            results.append(converted_row)
        return results

    cursor.fetchall.side_effect = enhanced_fetchall

    # Override patch for extract_column_info
    column_info_patcher = patch(
        'database.operations.query.extract_column_info',
        return_value=columns
    )

    # Setup transaction support
    transaction_patcher = _setup_transaction(mock_conn, cursor)

    # Setup error simulation
    error_classes = _setup_error_simulation(mock_conn, 'postgresql')

    # Apply common patches
    patchers = [transaction_patcher, column_info_patcher]

    # Connection type detection patches
    patchers.append(patch(
        'database.utils.connection_utils.is_psycopg_connection',
        side_effect=lambda obj, _seen=None: (
            obj is mock_conn or
            obj is mock_conn.connection or
            (hasattr(obj, 'connection') and obj.connection is mock_conn.connection) or
            (hasattr(obj, 'driver_connection') and (
                obj.driver_connection is mock_conn or
                obj.driver_connection is mock_conn.connection
            ))
        )
    ))

    # And isconnection
    patchers.append(patch(
        'database.utils.connection_utils.isconnection',
        side_effect=lambda x: x is mock_conn or patchers[0].get_original()[0](x)
    ))

    # Start all patchers
    for p in patchers:
        p.start()

    yield mock_conn

    # Stop all patchers
    for p in patchers:
        p.stop()


@pytest.fixture
def mock_sqlite_conn():
    """
    Create a mock SQLite connection for testing.

    This fixture provides a fully configured mock SQLite connection that can be
    used for testing database code without requiring an actual SQLite database file.
    The mock includes pre-configured responses for common operations and patches
    the necessary functions to ensure type detection works correctly.

    Returns
        Mock SQLite connection object

    Example usage:
        def test_sqlite_query(mock_sqlite_conn):
            # Test a SELECT query
            result = db.select(mock_sqlite_conn, "SELECT * FROM users WHERE id = ?", 1)
            assert len(result) > 0

            # Verify the executed SQL
            assert "SELECT * FROM users" in mock_sqlite_conn.cursor().last_sql
            assert mock_sqlite_conn.cursor().last_params == 1

            # Test an INSERT statement
            db.execute(mock_sqlite_conn, "INSERT INTO users (name) VALUES (?)", "test_user")

            # Access the lastrowid attribute like a real SQLite cursor
            assert mock_sqlite_conn.cursor().lastrowid == 42

            # Test direct connection execute method
            mock_sqlite_conn.connection.execute.assert_called()

        def test_sqlite_pragma(mock_sqlite_conn):
            # Test SQLite-specific PRAGMA statements
            result = db.select(mock_sqlite_conn, "PRAGMA table_info(users)")
            # Database module will use the mock result values configured for the connection
            assert len(result) > 0
    """
    mock_conn = _setup_mock_conn('sqlite', 'sqlite3.Connection')

    # Add SQLite-specific methods
    conn = mock_conn.connection
    conn.execute = MagicMock()  # SQLite allows direct execute() on connection

    # Create mock for sqlite3.Row factory
    # This simulates the behavior of the SQLite row_factory that allows
    # accessing columns by both index and name
    class MockSqlite3Row(UserDict):
        def __getitem__(self, key):
            if isinstance(key, int):
                return list(self.values())[key]
            return super().__getitem__(key)

    conn.row_factory = MockSqlite3Row

    # Setup cursor with SQLite-specific fields
    # Type codes match actual SQLite type codes:
    # 1 = INTEGER, 3 = TEXT
    cursor = _setup_cursor(
        mock_conn,
        description=[
            ('col1', 1, None, None, None, None, None),  # 1=INTEGER
            ('col2', 3, None, None, None, None, None)   # 3=TEXT
        ]
    )

    # Add SQLite-specific cursor attributes
    cursor.lastrowid = 42  # Simulate auto-increment primary key value
    cursor.executescript = MagicMock()  # Used for running multiple SQL statements

    # Setup data loader
    _setup_data_loader(mock_conn, 'sqlite')

    # Setup error simulation
    error_classes = _setup_error_simulation(mock_conn, 'sqlite')

    # Setup reconnection testing
    _setup_reconnection_testing(mock_conn, error_classes)

    # Setup special data type support
    _setup_special_data_types(mock_conn, 'sqlite')

    # Setup schema operations tracking
    schema_patchers = _setup_schema_operations_tracking(mock_conn)

    # Setup transaction support
    transaction_patcher = _setup_transaction(mock_conn, cursor)

    # Apply common patches to make test isolation clean
    patchers = [transaction_patcher] + schema_patchers

    # Connection type detection patches
    # These ensure that get_dialect_name() returns 'sqlite' for our mock
    patchers.append(patch(
        'database.utils.connection_utils.get_dialect_name',
        side_effect=lambda obj: 'sqlite' if (
            obj is mock_conn or
            obj is mock_conn.connection or
            # Simplify the connection checks to avoid potential recursion
            (hasattr(obj, 'connection') and id(obj.connection) == id(mock_conn.connection)) or
            (hasattr(obj, 'driver_connection') and (
                id(obj.driver_connection) == id(mock_conn) or
                id(obj.driver_connection) == id(mock_conn.connection)
            ))
        ) else None
    ))
    patchers.append(patch(
        'database.utils.connection_utils.isconnection',
        side_effect=lambda x: x is mock_conn or patchers[0].get_original()[0](x)
    ))

    # SQL handling patches
    patchers.append(patch(
        'database.sql.quote_identifier',
        side_effect=lambda ident, dialect: f'"{ident}"'
    ))
    patchers.append(patch(
        'database.sql.standardize_placeholders',
        side_effect=lambda sql, dialect: sql.replace('%s', '?') if dialect == 'sqlite' else sql
    ))
    patchers.append(patch(
        'database.operations.query.load_data',
        side_effect=lambda cursor, **kwargs: mock_conn.options.data_loader(
            cursor.fetchall(),
            [c[0] for c in cursor.description],
            **kwargs
        )
    ))
    patchers.append(patch(
        'database.operations.query.extract_column_info',
        return_value=[
            MagicMock(name='col1', python_type=int, to_dict=lambda: {'name': 'col1', 'python_type': 'int'}),
            MagicMock(name='col2', python_type=str, to_dict=lambda: {'name': 'col2', 'python_type': 'str'})
        ]
    ))

    # Start all patchers
    for p in patchers:
        p.start()

    yield mock_conn

    # Stop all patchers
    for p in patchers:
        p.stop()


@pytest.fixture
def mock_connection_pool():
    """
    Create a mock connection pool for testing.

    This fixture provides a fully configured mock connection pool that can be used
    for testing connection pooling functionality.

    Returns
        Tuple of (pool_mock, pool_patcher)

    Example usage:
        def test_connection_pool(mock_connection_pool):
            pool_mock, _ = mock_connection_pool

            # Test getting connections from the pool
            conn1 = db.connect("postgres:///dbname", use_pool=True)
            conn2 = db.connect("postgres:///dbname", use_pool=True)

            # Check that connections are marked as in-use
            in_use_count = sum(1 for status in pool_mock.conn_status.values()
                               if status['in_use'])
            assert in_use_count == 2

            # Test returning a connection to the pool
            conn1.cleanup()

            # Check that conn1A was returned to the pool
            in_use_count = sum(1 for status in pool_mock.conn_status.values()
                               if status['in_use'])
            assert in_use_count == 1
    """
    # Setup a factory to create consistent mock connections
    def create_mock_postgres():
        return _setup_mock_conn('postgresql', 'psycopg.Connection')

    # Create a pool of mock connections
    pool_mock, pool_patcher = _setup_connection_pool_testing(create_mock_postgres, pool_size=3)

    yield pool_mock, pool_patcher

    # Cleanup
    pool_patcher.stop()
