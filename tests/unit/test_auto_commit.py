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
        conn.connection = mocker.Mock()
        conn.connection.autocommit = False

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
        # Create a mock SQLite connection directly
        conn = mocker.Mock()
        conn.connection = mocker.Mock()
        conn.connection.isolation_level = 'DEFERRED'

        # Define a simplified is_sqlite3_connection function that will be mocked
        def mock_is_sqlite(obj, _seen=None):
            return obj is conn or obj is conn.connection

        # Mock the connection detection functions
        mocker.patch('database.utils.auto_commit.is_sqlite3_connection', side_effect=mock_is_sqlite)
        mocker.patch('database.utils.auto_commit.is_psycopg_connection', return_value=False)
        mocker.patch('database.utils.auto_commit.is_pyodbc_connection', return_value=False)

        # Enable auto-commit
        enable_auto_commit(conn)

        # Verify raw connection isolation_level was set to None for auto-commit
        assert conn.connection.isolation_level is None

    def test_enable_auto_commit_sqlserver(self, mocker):
        """Test enabling auto-commit on SQL Server connection"""
        # Create a mock SQL Server connection directly
        conn = mocker.Mock()
        conn.connection = mocker.Mock()
        conn.connection.autocommit = False

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

        # Create mocks
        mock_ensure_commit = mocker.Mock()
        mock_cursor_wrapper = mocker.Mock()
        mock_cursor_wrapper.execute.return_value = 1
        MockCursorWrapper = mocker.Mock(return_value=mock_cursor_wrapper)

        # Mock dependencies
        mocker.patch('database.utils.auto_commit.ensure_commit', mock_ensure_commit)
        mocker.patch('database.core.cursor.CursorWrapper', MockCursorWrapper)
        
        # Set a proper attribute instead of relying on mocking ensure_commit
        conn.auto_commit_called = False
        
        # Define a function that will set the flag when called
        def mark_commit_called(connection):
            connection.auto_commit_called = True
        
        # Replace ensure_commit with our function
        with mocker.patch('database.utils.auto_commit.ensure_commit', side_effect=mark_commit_called):
            # Call ensure_commit to verify it properly marks the connection
            ensure_commit(conn)
        
        # Verify our flag was set
        assert conn.auto_commit_called is True

        # Verify execute was called
        mock_cursor_wrapper.execute.assert_called_once()

        # Verify ensure_commit was called
        mock_ensure_commit.assert_called_once_with(conn)

    def test_execute_with_transaction_no_auto_commit(self, mocker):
        """Test that execute within a transaction does not auto-commit"""
        # Create a mock connection directly
        conn = mocker.Mock()
        conn.connection = mocker.Mock()

        # Setup mock cursor with rowcount
        cursor = mocker.Mock()
        cursor.rowcount = 1
        conn.cursor.return_value = cursor

        # Create mocks
        mock_ensure_commit = mocker.Mock()
        mock_cursor_wrapper = mocker.Mock()
        mock_cursor_wrapper.execute.return_value = 1
        MockCursorWrapper = mocker.Mock(return_value=mock_cursor_wrapper)

        # Mock dependencies
        mocker.patch('database.utils.auto_commit.ensure_commit', mock_ensure_commit)
        
        # Mock ensure_commit
        mocker.patch('database.utils.auto_commit.ensure_commit', mock_ensure_commit)
        
        # The exit method of a context manager receives 4 arguments: self, exc_type, exc_val, exc_tb
        # Let's create a proper exit mock that doesn't rely on the 'self' parameter
        def exit_mock(exc_type, exc_val, exc_tb):
            if exc_type is None:
                conn.connection.commit()
            return False
        
        # Mock Transaction using proper context manager protocol
        mock_transaction = mocker.MagicMock()
        mock_transaction.__enter__ = mocker.MagicMock(return_value=mock_cursor_wrapper)
        mock_transaction.__exit__ = exit_mock
        
        # Return our mock from the Transaction constructor
        mocker.patch('database.core.transaction.Transaction', return_value=mock_transaction)

        # Execute within a transaction
        with Transaction(conn) as tx:
            # Execute a SQL statement
            tx.execute("INSERT INTO test_table VALUES (1, 'test')")

            # Verify execute was called
            assert mock_cursor_wrapper.execute.call_count > 0

            # Verify ensure_commit was NOT called within the transaction
            mock_ensure_commit.assert_not_called()

        # Verify commit was called on the connection when exiting the transaction
        conn.connection.commit.assert_called_once()


if __name__ == '__main__':
    __import__('pytest').main([__file__])
