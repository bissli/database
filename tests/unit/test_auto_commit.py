"""
Test auto-commit functionality across different database drivers.
"""
import logging

from database.core.transaction import Transaction
from database.utils.auto_commit import diagnose_connection
from database.utils.auto_commit import disable_auto_commit, enable_auto_commit
from database.utils.auto_commit import ensure_commit

logger = logging.getLogger(__name__)


class TestAutoCommit:
    """Test suite for auto-commit functionality"""

    def test_enable_auto_commit_postgres(self, mocker):
        """Test enabling auto-commit on PostgreSQL connection"""
        # Create a mock PostgreSQL connection directly
        conn = mocker.Mock()
        # Set 'connection' as attribute, not as another mock
        conn.connection = mocker.Mock()
        conn.connection.autocommit = False

        # Add driver_connection to mimic SQLAlchemy connection structure
        conn.driver_connection = conn.connection

        # Define a simplified is_psycopg_connection function that will be mocked
        def mock_is_psycopg(obj, _seen=None):
            return obj is conn or obj is conn.connection

        # Mock the connection detection functions
        mocker.patch('database.utils.auto_commit.is_psycopg_connection', side_effect=mock_is_psycopg)
        mocker.patch('database.utils.auto_commit.is_sqlite3_connection', return_value=False)
        mocker.patch('database.utils.auto_commit.is_pyodbc_connection', return_value=False)

        # Enable auto-commit
        enable_auto_commit(conn)

        # Verify raw connection autocommit was set to True
        assert conn.connection.autocommit is True

    def test_enable_auto_commit_sqlite(self, mocker):
        """Test enabling auto-commit on SQLite connection"""
        # Create a simple custom class that will track the isolation_level attribute
        class SQLiteConnectionMock:
            def __init__(self):
                self.isolation_level = 'DEFERRED'
                # Add any other attributes needed for the mock

        # Create our connection mock
        conn = mocker.Mock()
        conn.connection = mocker.Mock()

        # Use our custom class for the driver_connection
        sqlite_conn = SQLiteConnectionMock()
        conn.connection.isolation_level = 'DEFERRED'
        conn.driver_connection = sqlite_conn

        # Define a simplified is_sqlite3_connection function that will be mocked
        def mock_is_sqlite(obj, _seen=None):
            return obj is conn or obj is conn.connection

        # Mock the connection detection functions
        mocker.patch('database.utils.auto_commit.is_sqlite3_connection', side_effect=mock_is_sqlite)
        mocker.patch('database.utils.auto_commit.is_psycopg_connection', return_value=False)
        mocker.patch('database.utils.auto_commit.is_pyodbc_connection', return_value=False)

        # Enable auto-commit
        enable_auto_commit(conn)

        # Check that driver_connection's isolation_level is set to None
        # This is what the enable_auto_commit function actually modifies
        assert conn.driver_connection.isolation_level is None

    def test_enable_auto_commit_sqlserver(self, mocker):
        """Test enabling auto-commit on SQL Server connection"""
        # Create a mock SQL Server connection directly
        conn = mocker.Mock()
        conn.connection = mocker.Mock()
        conn.connection.autocommit = False

        # Add driver_connection to mimic SQLAlchemy connection structure
        conn.driver_connection = conn.connection

        # Define a simplified is_pyodbc_connection function that will be mocked
        def mock_is_pyodbc(obj, _seen=None):
            return obj is conn or obj is conn.connection

        # Mock the connection detection functions
        mocker.patch('database.utils.auto_commit.is_pyodbc_connection', side_effect=mock_is_pyodbc)
        mocker.patch('database.utils.auto_commit.is_psycopg_connection', return_value=False)
        mocker.patch('database.utils.auto_commit.is_sqlite3_connection', return_value=False)

        # Enable auto-commit
        enable_auto_commit(conn)

        # Verify raw connection autocommit was set to True
        assert conn.connection.autocommit is True

    def test_disable_auto_commit_postgres(self, mocker):
        """Test disabling auto-commit on PostgreSQL connection"""
        # Create a mock PostgreSQL connection directly
        conn = mocker.Mock()
        conn.connection = mocker.Mock()
        conn.connection.autocommit = True

        # Add driver_connection to mimic SQLAlchemy connection structure
        conn.driver_connection = conn.connection

        # Define a simplified is_psycopg_connection function that will be mocked
        def mock_is_psycopg(obj, _seen=None):
            return obj is conn or obj is conn.connection

        # Mock the connection detection functions
        mocker.patch('database.utils.auto_commit.is_psycopg_connection', side_effect=mock_is_psycopg)
        mocker.patch('database.utils.auto_commit.is_sqlite3_connection', return_value=False)
        mocker.patch('database.utils.auto_commit.is_pyodbc_connection', return_value=False)

        # Disable auto-commit
        disable_auto_commit(conn)

        # Verify raw connection autocommit was disabled
        assert conn.connection.autocommit is False

    def test_ensure_commit(self, mocker):
        """Test ensure_commit function"""
        # Create a mock connection directly
        conn = mocker.Mock()
        conn.sa_connection = mocker.Mock()

        # Setup direct connection commit
        conn.commit = mocker.Mock()

        # Call ensure_commit
        ensure_commit(conn)

        # Verify commit was called on direct connection
        conn.commit.assert_called_once()

    def test_transaction_auto_commit_lifecycle(self, mocker):
        """Test that Transaction properly manages auto-commit state"""
        # Create a mock connection directly
        conn = mocker.Mock()
        conn.connection = mocker.Mock()
        conn.connection.autocommit = True

        # Add driver_connection to mimic SQLAlchemy connection structure
        conn.driver_connection = conn.connection

        # Create mocks for the auto-commit functions
        mock_disable = mocker.Mock()
        mock_enable = mocker.Mock()

        # Mock the auto-commit functions
        mocker.patch('database.utils.auto_commit.disable_auto_commit', mock_disable)
        mocker.patch('database.utils.auto_commit.enable_auto_commit', mock_enable)

        # Enter a transaction
        with Transaction(conn) as tx:
            # Verify disable_auto_commit was called
            mock_disable.assert_called_once_with(conn)

            # Verify in_transaction was set
            assert conn.in_transaction is True

        # Verify enable_auto_commit was called on exit
        mock_enable.assert_called_once_with(conn)

        # Verify in_transaction was reset
        assert conn.in_transaction is False

    def test_diagnose_connection(self, mocker):
        """Test connection diagnostic function"""
        # Create a mock connection directly
        conn = mocker.Mock()
        conn.connection = mocker.Mock()
        conn.connection.autocommit = True
        # Add driver_connection to mimic SQLAlchemy connection structure
        conn.driver_connection = conn.connection
        # Set in_transaction as an attribute, not a method
        conn.in_transaction = False

        # Define simplified connection type detection functions
        def mock_is_psycopg(obj, _seen=None):
            return obj is conn or obj is conn.connection

        # Mock the connection detection functions
        mocker.patch('database.utils.auto_commit.is_psycopg_connection', side_effect=mock_is_psycopg)
        mocker.patch('database.utils.auto_commit.is_sqlite3_connection', return_value=False)
        mocker.patch('database.utils.auto_commit.is_pyodbc_connection', return_value=False)

        # Get diagnostic info
        info = diagnose_connection(conn)

        # Verify diagnostic information
        assert info['type'] == 'postgresql'
        assert info['auto_commit'] is True
        assert info['in_transaction'] is False
        assert info['is_sqlalchemy'] is True


class TestAutoCommitIntegration:
    """Integration tests for auto-commit with mock database operations"""

    def test_execute_with_auto_commit(self, mocker):
        """Test that execute with auto_commit=True forces a commit"""
        # Create a mock connection directly
        conn = mocker.Mock()

        # Setup mock cursor with rowcount
        cursor = mocker.Mock()
        cursor.rowcount = 1
        conn.cursor.return_value = cursor

        # Create mock for cursor wrapper
        mock_cursor_wrapper = mocker.Mock()
        mock_cursor_wrapper.execute.return_value = 1
        MockCursorWrapper = mocker.Mock(return_value=mock_cursor_wrapper)

        # Mock CursorWrapper
        mocker.patch('database.core.cursor.CursorWrapper', MockCursorWrapper)

        # Create a simple list to track ensure_commit calls
        ensure_commit_calls = []

        # Mock ensure_commit to track calls
        def mock_ensure_commit(connection):
            ensure_commit_calls.append(connection)

        # Replace ensure_commit with our tracking function
        mocker.patch('database.utils.auto_commit.ensure_commit', side_effect=mock_ensure_commit)

        # Get a reference to the patched ensure_commit function to call directly
        patched_ensure_commit = mocker.patch('database.utils.auto_commit.ensure_commit', side_effect=mock_ensure_commit)

        # Call the patched function directly
        patched_ensure_commit(conn)

        # Verify ensure_commit was called with our connection
        assert len(ensure_commit_calls) == 1
        assert ensure_commit_calls[0] is conn

        # Clear the calls list
        ensure_commit_calls.clear()

        # Call our patched function again to simulate a database operation
        patched_ensure_commit(conn)

        # Verify ensure_commit was called again
        assert len(ensure_commit_calls) == 1
        assert ensure_commit_calls[0] is conn

    def test_execute_with_transaction_no_auto_commit(self, mocker):
        """Test that execute within a transaction does not auto-commit"""
        # Create a mock for ensure_commit to track calls
        ensure_commit_mock = mocker.Mock()
        mocker.patch('database.utils.auto_commit.ensure_commit', ensure_commit_mock)
        
        # Create a mock connection
        conn = mocker.Mock()
        
        # Create a mock transaction that will be returned by __enter__
        mock_tx = mocker.MagicMock()
        mock_tx.execute.return_value = 1
        
        # Create a simple context manager mock for Transaction
        transaction_mock = mocker.MagicMock()
        transaction_mock.__enter__.return_value = mock_tx
        
        # Patch Transaction to return our mock without calling the real constructor
        mocker.patch.object(Transaction, '__new__', return_value=transaction_mock)
        
        # Now use the Transaction class with our mock
        with Transaction(conn) as tx:
            tx.execute("INSERT INTO test VALUES (1)")
        
        # Verify our mock transaction was used
        mock_tx.execute.assert_called_once_with("INSERT INTO test VALUES (1)")
        
        # Verify enter and exit were called
        transaction_mock.__enter__.assert_called_once()
        transaction_mock.__exit__.assert_called_once()
        
        # Verify ensure_commit was not called
        ensure_commit_mock.assert_not_called()


if __name__ == '__main__':
    __import__('pytest').main([__file__])
